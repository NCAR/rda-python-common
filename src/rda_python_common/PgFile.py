#
###############################################################################
#
#     Title : PgFile.py
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 08/05/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module to copy, move and delete data files locally
#             and remotely
#
#    Github : https://github.com/NCAR/rda-python_common.git
#
###############################################################################
#
import sys
import os
from os import path as op
import pwd
import grp
import stat
import re
import time
import glob
import json
from . import PgLOG
from . import PgUtil
from . import PgSIG
from . import PgDBI

CMDBTH = (0x0033)   # return both stdout and stderr, 16 + 32 + 2 + 1
RETBTH = (0x0030)   # return both stdout and stderr, 16 + 32
CMDRET = (0x0110)   # return stdout and save error, 16 + 256
CMDERR = (0x0101)   # display command and save error, 1 + 256
CMDGLB = (0x0313)   # return stdout and save error for globus, 1+2+16+256+512

PGCMPS = {
#  extension Compress       Uncompress       ArchiveFormat
   'Z'   :  ['compress -f', 'uncompress -f', 'Z'],
   'zip' :  ['zip',         'unzip',         'ZIP'],
   'gz'  :  ['gzip',        'gunzip',        'GZ'],
   'xz'  :  ['xz',          'unxz',          'XZ'],
   'bz2' :  ['bzip2',       'bunzip2',       'BZ2']
}
CMPSTR = '|'.join(PGCMPS)

PGTARS = {
#  extension   Packing      Unpacking   ArchiveFormat
   'tar'     : ['tar -cvf',  'tar -xvf', 'TAR'],
   'tar.Z'   : ['tar -Zcvf', 'tar -xvf', 'TAR.Z'],
   'zip'     : ['zip -v',    'unzip -v', 'ZIP'],
   'tgz'     : ['tar -zcvf', 'tar -xvf', 'TGZ'],
   'tar.gz'  : ['tar -zcvf', 'tar -xvf', 'TAR.GZ'],
   'txz'     : ['tar -cvJf', 'tar -xvf', 'TXZ'],
   'tar.xz'  : ['tar -cvJf', 'tar -xvf', 'TAR.XZ'],
   'tbz2'    : ['tar -cvjf', 'tar -xvf', 'TBZ2'],
   'tar.bz2' : ['tar -cvjf', 'tar -xvf', 'TAR.BZ2']
}

TARSTR = '|'.join(PGTARS)
DELDIRS = {}
TASKIDS = {}   # cache unfinished 
MD5CMD = 'md5sum'
SHA512CMD = 'sha512sum'
LHOST = "localhost"
OHOST = PgLOG.PGLOG['OBJCTSTR']
BHOST = PgLOG.PGLOG['BACKUPNM']
DHOST = PgLOG.PGLOG['DRDATANM']
OBJCTCMD = "isd_s3_cli"
BACKCMD = "dsglobus" 

HLIMIT = 0    # HTAR file count limit
BLIMIT = 2    # minimum back tar file size in DB 
DIRLVLS = 0

# record how many errors happen for working with HPSS, local or remote machines
ECNTS = {'D' : 0, 'H' : 0, 'L' : 0, 'R' : 0, 'O' : 0, 'B' : 0}
# up limits for how many continuing errors allowed
ELMTS = {'D' : 20, 'H' : 20, 'L' : 20, 'R' : 20, 'O' : 10, 'B' : 10}

# down storage hostnames & paths
DHOSTS = {
   'G' : PgLOG.PGLOG['GPFSNAME'],
   'O' : OHOST,
   'B' : BHOST,
   'D' : DHOST
}

DPATHS = {
   'G' : PgLOG.PGLOG['DSSDATA'],
   'O' : PgLOG.PGLOG['OBJCTBKT'],
   'B' : '/' + PgLOG.PGLOG['DEFDSID'],   # backup globus endpoint
   'D' : '/' + PgLOG.PGLOG['DEFDSID']    # disaster recovery globus endpoint
}

QSTATS = {
   'A' : 'ACTIVE',
   'I' : 'INACTIVE',
   'S' : 'SUCCEEDED',
   'F' : 'FAILED',
}

QPOINTS = {
   'L' : 'rda-glade',
   'B' : 'rda-quasar',
   'D' : 'rda-quasar-drdata'
}

QHOSTS = {
   'rda-glade' : LHOST,
   'rda-quasar' : BHOST,
   'rda-quasar-drdata' : DHOST
}

ENDPOINTS = {
   'rda-glade' : "NCAR RDA GLADE",
   'rda-quasar' : "NCAR RDA Quasar",
   'rda-quasar-drdata' : "NCAR RDA Quasar DRDATA"
}

#
# reset the up limit for a specified error type
#
def reset_error_limit(etype, lmt):

   ELMTS[etype] = lmt

#
# wrapping PgLOG.pglog() to show error and no fatal exit at the first call for retry 
#
def errlog(msg, etype, retry = 0, logact = 0):

   bckgrnd = PgLOG.PGLOG['BCKGRND']
   logact |= PgLOG.ERRLOG
   if not retry:
      if msg and not re.search(r'\n$', msg): msg += "\n"
      msg += "[The same execution will be retried in {} Seconds]".format(PgSIG.PGSIG['ETIME'])
      PgLOG.PGLOG['BCKGRND'] = 1
      logact &= ~(PgLOG.EMEROL|PgLOG.EXITLG)
   elif ELMTS[etype]:
       ECNTS[etype] += 1
       if ECNTS[etype] >= ELMTS[etype]:
          logact |= PgLOG.EXITLG
          ECNTS[etype] = 0

   if PgLOG.PGLOG['DSCHECK'] and logact&PgLOG.EXITLG: PgDBI.record_dscheck_error(msg)
   PgLOG.pglog(msg, logact)
   PgLOG.PGLOG['BCKGRND'] = bckgrnd
   if not retry: time.sleep(PgSIG.PGSIG['ETIME'])

   return PgLOG.FAILURE

#
# Copy a file from one host (including local host) to an another host (including local host)
# excluding copy file from remote host to remote host copying in background is permitted
#
#   tofile - target file name
# fromfile - source file name
#   tohost - target host name, default to LHOST
# fromhost - original host name, default to LHOST
#
# Return 1 if successful 0 if failed with error message generated in PgLOG.pgsystem() cached in PgLOG.PGLOG['SYSERR'] 
#
def copy_rda_file(tofile, fromfile, tohost = LHOST, fromhost = LHOST, logact = 0):

   thost = strip_host_name(tohost)
   fhost = strip_host_name(fromhost)

   if PgUtil.pgcmp(thost, fhost, 1) == 0:
      if PgUtil.pgcmp(thost, LHOST, 1) == 0:
         return local_copy_local(tofile, fromfile, logact)
   elif PgUtil.pgcmp(fhost, LHOST, 1) == 0:
      if PgUtil.pgcmp(thost, OHOST, 1) == 0:
         return local_copy_object(tofile, fromfile, None, None, logact)
      elif PgUtil.pgcmp(thost, BHOST, 1) == 0:
         return local_copy_backup(tofile, fromfile, QPOINTS['B'], logact)
      elif PgUtil.pgcmp(thost, DHOST, 1) == 0:
         return local_copy_backup(tofile, fromfile, QPOINTS['D'], logact)
      else:
         return local_copy_remote(tofile, fromfile, tohost, logact)
   elif PgUtil.pgcmp(thost, LHOST, 1) == 0:
      if PgUtil.pgcmp(fhost, OHOST, 1) == 0:
         return object_copy_local(tofile, fromfile, None, logact)
      elif PgUtil.pgcmp(fhost, BHOST, 1) == 0:
         return backup_copy_local(tofile, fromfile, QPOINTS['B'], logact)
      elif PgUtil.pgcmp(fhost, DHOST, 1) == 0:
         return backup_copy_local(tofile, fromfile, QPOINTS['D'], logact)
      else:
         return remote_copy_local(tofile, fromfile, fromhost)

   return errlog("{}-{}->{}-{}: Cannot copy file".format(fhost, fromfile, thost, tofile), 'O', 1, PgLOG.LGEREX)

#
# Copy a file locally
#
#   tofile - target file name
# fromfile - source file name
#
def local_copy_local(tofile, fromfile, logact = 0):

   finfo = check_local_file(fromfile, 0, logact)
   if not finfo:
      if finfo != None: return PgLOG.FAILURE
      return lmsg(fromfile, "{} to copy to {}".format(PgLOG.PGLOG['MISSFILE'], tofile), logact)

   target = tofile
   ms = re.match(r'^(.+)/$', tofile)
   if ms:
      dir = ms.group(1)
      tofile += op.basename(fromfile)
   else:
      dir = get_local_dirname(tofile)

   if not make_local_directory(dir, logact): return PgLOG.FAILURE

   cmd = "cp -{} {} {}".format(('f' if finfo['isfile'] else "rf"), fromfile, target)
   reset = loop = 0
   while((loop-reset) < 2):
      info = None
      PgLOG.PGLOG['ERR2STD'] = ['are the same file']
      ret = PgLOG.pgsystem(cmd, logact, CMDERR)
      PgLOG.PGLOG['ERR2STD'] = []
      if ret:
         info = check_local_file(tofile, 143, logact)   # 1+2+4+8+128
         if info:
            if not info['isfile']:
               set_local_mode(tofile, 0, 0, info['mode'], info['logname'], logact)
               return PgLOG.SUCCESS
            elif info['data_size'] == finfo['data_size']:
               set_local_mode(tofile, 1, 0, info['mode'], info['logname'], logact)
               return PgLOG.SUCCESS
         elif info != None:
            break

      if PgLOG.PGLOG['SYSERR']:
         errmsg = PgLOG.PGLOG['SYSERR']
      else:
         errmsg = "Error of '{}': Miss target file {}".format(cmd, tofile)
      errlog(errmsg, 'L', (loop - reset), logact)
      if loop == 0: reset = reset_local_info(tofile, info, logact)
      loop += 1

   return PgLOG.FAILURE

#
# Copy a local file to a remote host
#
#   tofile - target file name
# fromfile - source file name
#     host - remote host name
#
def local_copy_remote(tofile, fromfile, host, logact = 0):

   finfo = check_local_file(fromfile, 0, logact)
   if not finfo:
      if finfo != None: return PgLOG.FAILURE
      return lmsg(fromfile, "{} to copy to {}-{}".format(PgLOG.PGLOG['MISSFILE'], host, tofile), logact)

   target = tofile
   ms = re.match(r'^(.+)/$', tofile)
   if ms:
      dir = ms.group(1)
      tofile += op.basename(fromfile)
   else:
      dir = op.dirname(tofile)

   if not make_remote_directory(dir, host, logact): return PgLOG.FAILURE

   cmd = PgLOG.get_sync_command(host)
   cmd += " {} {}".format(fromfile, target)
   for loop in range(2):
      if PgLOG.pgsystem(cmd, logact, CMDERR):
         info = check_remote_file(tofile, host, 0, logact)
         if info:
            if not finfo['isfile']:
               set_remote_mode(tofile, 0, host, PgLOG.PGLOG['EXECMODE'])
               return PgLOG.SUCCESS
            elif info['data_size'] == finfo['data_size']:
               set_remote_mode(tofile, 1, host, PgLOG.PGLOG['FILEMODE'])
               return PgLOG.SUCCESS         
         elif info != None:
            break
   
      errlog(PgLOG.PGLOG['SYSERR'], 'R', loop, logact)

   return PgLOG.FAILURE

#
# Copy a local file to object store
#
#   tofile - target file name
# fromfile - source file name
#   bucket - bucket name on Object store
#     meta - reference to metadata hash
#
def local_copy_object(tofile, fromfile, bucket = None, meta = None, logact = 0):

   if not bucket: bucket = PgLOG.PGLOG['OBJCTBKT']
   if meta is None: meta = {}
   if 'user' not in meta: meta['user'] = PgLOG.PGLOG['CURUID']
   if 'group' not in meta: meta['group'] = PgLOG.PGLOG['RDAGRP']
   uinfo = json.dumps(meta)
 
   finfo = check_local_file(fromfile, 0, logact)
   if not finfo:
      if finfo != None: return PgLOG.FAILURE
      return lmsg(fromfile, "{} to copy to {}-{}".format(PgLOG.PGLOG['MISSFILE'], OHOST, tofile), logact)

   if not logact&PgLOG.OVRIDE:
      tinfo = check_object_file(tofile, bucket, 0, logact)
      if tinfo and tinfo['data_size'] > 0:
         return PgLOG.pglog("{}-{}-{}: file exists already".format(OHOST, bucket, tofile), logact)

   cmd = "{} ul -lf {} -b {} -k {} -md '{}'".format(OBJCTCMD, fromfile, bucket, tofile, uinfo)
   for loop in range(2):
      buf = PgLOG.pgsystem(cmd, logact, CMDBTH)
      tinfo = check_object_file(tofile, bucket, 0, logact)
      if tinfo:
         if tinfo['data_size'] == finfo['data_size']:
            return PgLOG.SUCCESS      
      elif tinfo != None:
         break
   
      errlog("Error Execute: {}\n{}".format(cmd, buf), 'O', loop, logact)

   return PgLOG.FAILURE

#
# Copy multiple files from a Globus endpoint to another
#   tofiles - target file name list, echo name leading with /dsnnn.n/ on Quasar and 
#             leading with /data/ or /decsdata/ on local glade disk
# fromfiles - source file name list, the same format as the tofiles
#   topoint - target endpoint name, 'rda-glade', 'rda-quasar' or 'rda-quasar-drdata' 
# frompoint - source endpoint name, the same choices as the topoint
#
def quasar_multiple_trasnfer(tofiles, fromfiles, topoint, frompoint, logact = 0):

   ret = PgLOG.FAILURE
   qstr = '{"action":"transfer","label":"%s","verify_checksum":true,' % ENDPOINTS[topoint]
#   qstr = '{"action":"transfer","label":"%s",' % ENDPOINTS[topoint]
   qstr += '"source_endpoint":"%s","destination_endpoint":"%s","files":[\n' % (frompoint, topoint)
   fcnt = len(fromfiles)
   bstr = ''
   for i in range(fcnt):
      qstr += '%s{"source_file":"%s","destination_file":"%s"}' % (bstr, fromfiles[i], tofiles[i])
      if i == 0: bstr = ',\n'
   qstr += ']}'

   task = submit_globus_task(BACKCMD, topoint, logact, qstr)
   if task['stat'] == 'S':
      ret = PgLOG.SUCCESS
   elif task['stat'] == 'A':
      TASKIDS["{}-{}".format(topoint, tofiles[0])] = task['id']
      ret = PgLOG.FINISH

   return ret

#
# Copy a file from a Globus endpoint to another

#    tofile - target file name, leading with /dsnnn.n/ on Quasar and 
#             leading with /data/ or /decsdata/ on local glade disk
#  fromfile - source file, the same format as the tofile
#   topoint - target endpoint name, 'rda-glade', 'rda-quasar' or 'rda-quasar-drdata' 
# frompoint - source endpoint name, the same choices as the topoint
#
def endpoint_copy_endpoint(tofile, fromfile, topoint, frompoint, logact = 0):

   ret = PgLOG.FAILURE
   finfo = check_globus_file(fromfile, frompoint, 0, logact)
   if not finfo:
      if finfo != None: return ret
      return lmsg(fromfile, "{} to copy {} file to {}-{}".format(PgLOG.PGLOG['MISSFILE'], frompoint, topoint, tofile), logact)

   if not logact&PgLOG.OVRIDE:
      tinfo = check_globus_file(tofile, topoint, 0, logact)
      if tinfo and tinfo['data_size'] > 0:
         return PgLOG.pglog("{}-{}: file exists already".format(topoint, tofile), logact)

   cmd = "{} -t -vc -se {} -de {} -sf {} -df {}".format(BACKCMD, frompoint, topoint, fromfile, tofile)
   task = submit_globus_task(cmd, topoint, logact)
   if task['stat'] == 'S':
      ret = PgLOG.SUCCESS
   elif task['stat'] == 'A':
      TASKIDS["{}-{}".format(topoint, tofile)] = task['id']
      ret = PgLOG.FINISH

   return ret

#
# submit a globus task and return a task id
#
def submit_globus_task(cmd, endpoint, logact = 0, qstr = None):

   task = {'id' : None, 'stat' : 'U'}
   loop = reset = 0
   while (loop-reset) < 2:
      buf = PgLOG.pgsystem(cmd, logact, CMDGLB, qstr)
      syserr = PgLOG.PGLOG['SYSERR']
      if buf and buf.find('a task has been created') > -1:
         ms = re.search(r'Task ID:\s+(\S+)', buf)
         if ms:
            task['id'] = ms.group(1)
            lp = 0
            while lp < 2:
               task['stat'] = check_globus_status(task['id'], endpoint, logact)
               if task['stat'] == 'S': break
               time.sleep(PgSIG.PGSIG['ETIME'])
               lp += 1
            if task['stat'] == 'S' or task['stat'] == 'A': break
            if task['stat'] == 'F' and not syserr: break

      errmsg = "Error Execute: " + cmd
      if qstr: errmsg += " with stdin:\n" + qstr
      if syserr:
         errmsg += "\n" + syserr
         (hstat, msg) = host_down_status('', QHOSTS[endpoint], 1, logact)
         if hstat: errmsg += "\n" + msg
      errlog(errmsg, 'B', (loop - reset), logact)
      if loop == 0 and syserr and syserr.find('This user has too many pending jobs') > -1: reset = 1
      loop += 1

   if task['stat'] == 'S' or task['stat'] == 'A': ECNTS['B'] = 0   # reset error count
   return task

#
# check Globus transfer status for given taskid. Cancel the task
# if PgLOG.NOWAIT presents and Details is neither OK nor Queued
#
def check_globus_status(taskid, endpoint = None, logact = 0):

   ret = 'U'
   if not taskid: return ret
   if not endpoint: endpoint = PgLOG.PGLOG['BACKUPEP']
   mp = r'Status:\s+({})'.format('|'.join(QSTATS.values()))
   cmd = "{} -gt --task-id {}".format(BACKCMD, taskid)
   astats = ['OK', 'Queued']

   for loop in range(2):
      buf = PgLOG.pgsystem(cmd, logact, CMDRET)
      if buf:
         ms = re.search(mp, buf)
         if ms:
            ret = ms.group(1)[0]
            if ret == 'A':
               ms = re.search(r'Details:\s+(\S+)', buf)
               if ms:
                  detail = ms.group(1)
                  if detail not in astats:
                     if logact&PgLOG.NOWAIT:
                        errmsg = "{}: Cancel Task due to {}:\n{}".format(taskid, detail, buf)
                        errlog(errmsg, 'B', 1, logact)
                        ccmd = "{} -ct --task-id {}".format(BACKCMD, taskid)
                        PgLOG.pgsystem(ccmd, logact, 7)
                     else:
                        time.sleep(PgSIG.PGSIG['ETIME'])
                     continue
            break
            
      errmsg = "Error Execute: " + cmd
      if PgLOG.PGLOG['SYSERR']:
         errmsg = "\n" + PgLOG.PGLOG['SYSERR']
         (hstat, msg) = host_down_status('', QHOSTS[endpoint], 1, logact)
         if hstat: errmsg += "\n" + msg
      errlog(errmsg, 'B', loop, logact)

   if ret == 'S' or ret == 'A': ECNTS['B'] = 0   # reset error count
   return ret

#
# return SUCCESS if Globus transfer is done; FAILURE otherwise
#
def check_globus_finished(tofile, topoint, logact = 0):

   ret = PgLOG.SUCCESS
   ckey = "{}-{}".format(topoint, tofile)
   if ckey in TASKIDS:
      taskid = TASKIDS[ckey]
   else:
      errlog(ckey + ": Miss Task ID to check Status", 'B', 1, logact)
      return PgLOG.FAILURE

   lp = 0
   if logact&PgLOG.NOWAIT:
      act = logact&(~PgLOG.NOWAIT)
      lps = 2
   else:
      act = logact
      lps = 0

   while True:
      stat = check_globus_status(taskid, topoint, act)
      if stat == 'A':
         if lps:
            lp += 1
            if lp > lps: act = logact
         time.sleep(PgSIG.PGSIG['ETIME'])
      else:
         if stat == 'S':
            del TASKIDS[ckey]
         else:
            status = QSTATS[stat] if stat in QSTATS else 'UNKNOWN'
            errlog("{}: Status '{}' for Task {}".format(ckey, status, taskid), 'B', 1, logact)
            ret = PgLOG.FAILURE
         break

   return ret

#
# Copy a local file to Quasar backup tape system
#
#   tofile - target file name, leading with /dsnnn.n/
# fromfile - source file name, leading with /data/ or /decsdata/
# endpoint - endpoint name on Quasar Backup Server
#
def local_copy_backup(tofile, fromfile, endpoint = None, logact = 0):

   if not endpoint: endpoint = PgLOG.PGLOG['BACKUPEP']
   return endpoint_copy_endpoint(tofile, fromfile, endpoint, 'rda-glade', logact)

#
# Copy a  Quasar backup file to local Globus endpoint
#
#   tofile - target file name, leading with /data/ or /decsdata/
# fromfile - source file name, leading with /dsnnn.n/
# endpoint - endpoint name on Quasar Backup Server
#
def backup_copy_local(tofile, fromfile, endpoint = None, logact = 0):

   if not endpoint: endpoint = PgLOG.PGLOG['BACKUPEP']
   return endpoint_copy_endpoint(tofile, fromfile, 'rda-glade', endpoint, logact)

#
# Copy a remote file to local
#
#   tofile - target file name
# fromfile - source file name
#     host - remote host name
#
def remote_copy_local(tofile, fromfile, host, logact = 0):

   cmd = PgLOG.get_sync_command(host)
   finfo = check_remote_file(fromfile, host, 0, logact)
   if not finfo:
      if finfo != None: return PgLOG.FAILURE
      return errlog("{}-{}: {} to copy to {}".format(host, fromfile, PgLOG.PGLOG['MISSFILE'], tofile), 'R', 1, logact)

   target = tofile
   ms = re.match(r'^(.+)/$', tofile)
   if ms:
      dir = ms.group(1)
      tofile += op.basename(fromfile)
   else:
      dir = get_local_dirname(tofile)

   if not make_local_directory(dir, logact): return PgLOG.FAILURE

   cmd += " -g {} {}".format(fromfile, target)
   loop = reset = 0
   while (loop-reset) < 2:
      if PgLOG.pgsystem(cmd, logact, CMDERR):
         info = check_local_file(tofile, 143, logact)  # 1+2+4+8+128
         if info:
            if not info['isfile']:
                set_local_mode(tofile, 0, PgLOG.PGLOG['EXECMODE'])
                return PgLOG.SUCCESS
            elif info['data_size'] == finfo['data_size']:
                set_local_mode(tofile, 1, PgLOG.PGLOG['FILEMODE'])
                return PgLOG.SUCCESS
         elif info != None:
            break

      errlog(PgLOG.PGLOG['SYSERR'], 'L', (loop - reset), logact)
      if loop == 0: reset = reset_local_info(tofile, info, logact)
      loop += 1

   return PgLOG.FAILURE

#
# Copy a object file to local
#
#   tofile - target file name
# fromfile - source file name
#   bucket - bucket name on Object store
#
def object_copy_local(tofile, fromfile, bucket = None, logact = 0):
   
   ret = PgLOG.FAILURE
   if not bucket: bucket = PgLOG.PGLOG['OBJCTBKT']
   finfo = check_object_file(fromfile, bucket, 0, logact)
   if not finfo:
      if finfo != None: return ret
      return lmsg(fromfile, "{}-{} to copy to {}".format(OHOST, PgLOG.PGLOG['MISSFILE'], tofile), logact)

   cmd = "{} go -k {} -b {}".format(OBJCTCMD, fromfile, bucket)
   fromname = op.basename(fromfile)
   toname = op.basename(tofile)
   if toname == tofile:
      dir = odir = None
   else:
      dir = op.dirname(tofile)
      odir = change_local_directory(dir, logact)
   loop = reset = 0
   while (loop-reset) < 2:
      buf = PgLOG.pgsystem(cmd, logact, CMDBTH)
      info = check_local_file(fromname, 143, logact)   # 1+2+4+8+128
      if info:
         if info['data_size'] == finfo['data_size']:
            set_local_mode(fromfile, info['isfile'], 0, info['mode'], info['logname'], logact)
            if toname == fromname or move_local_file(toname, fromname, logact):
               ret = PgLOG.SUCCESS
               break
         
      
      elif info != None:
         break
   
      errlog("Error Execute: {}\n{}".format(cmd, buf), 'L', (loop - reset), logact)
      if loop == 0: reset = reset_local_info(tofile, info, logact)
      loop += 1
   if odir and odir != dir:
      change_local_directory(odir, logact)

   return ret

#
# Copy a remote file to object
#
#   tofile - target object file name
# fromfile - source remote file name
#     host - remote host name
#   bucket - bucket name on Object store
#     meta - reference to metadata hash
#
def remote_copy_object(tofile, fromfile, host, bucket = None, meta = None, logact = 0):

   if is_local_host(host): return local_copy_object(tofile, fromfile, bucket, meta, logact)

   locfile = "{}/{}".format(PgLOG.PGLOG['TMPPATH'], op.basename(tofile))
   ret = remote_copy_local(locfile, fromfile, host, logact)
   if ret:
      ret = local_copy_object(tofile, locfile, bucket, meta, logact)
      delete_local_file(locfile, logact)

   return ret

#
# Copy an object file to remote
#
#   tofile - target remote file name
# fromfile - source object file name
#     host - remote host name
#   bucket - bucket name on Object store
#     meta - reference to metadata hash
#
def object_copy_remote(tofile, fromfile, host, bucket = None, logact = 0):

   if is_local_host(host): return object_copy_local(tofile, fromfile, bucket, logact)

   locfile = "{}/{}".format(PgLOG.PGLOG['TMPPATH'], op.basename(tofile))
   ret = object_copy_local(locfile, fromfile, bucket, logact)
   if ret:
      ret = local_copy_remote(fromfile, locfile, host, logact)
      delete_local_file(locfile, logact)

   return ret

#
# Delete a file/directory on a given host name (including local host) no background process for deleting
#
# file - file name to be deleted
# host - host name the file on, default to LHOST
#
# Return 1 if successful 0 if failed with error message generated in PgLOG.pgsystem() cached in PgLOG.PGLOG['SYSERR'] 
#
def delete_rda_file(file, host, logact = 0):
       
   shost = strip_host_name(host)
   if PgUtil.pgcmp(shost, LHOST, 1) == 0:
      return delete_local_file(file, logact)
   elif PgUtil.pgcmp(shost, OHOST, 1) == 0:
      return delete_object_file(file, None, logact)      
   else:
      return delete_remote_file(file, host, logact)

#
# Delete a local file/irectory
#
def delete_local_file(file, logact = 0):

   info = check_local_file(file, 0, logact)
   if not info: return PgLOG.FAILURE
   cmd = "rm -rf "
   cmd += file
   loop = reset = 0
   while (loop-reset) < 2:
      if PgLOG.pgsystem(cmd, logact, CMDERR):
         info = check_local_file(file, 14, logact)
         if info is None:
            if DIRLVLS: record_delete_directory(op.dirname(file), LHOST)
            return PgLOG.SUCCESS
         elif not info:
            break   # error checking file

      errlog(PgLOG.PGLOG['SYSERR'], 'L', (loop - reset), logact)
      if loop == 0: reset = reset_local_info(file, info, logact)
      loop += 1

   return PgLOG.FAILURE

#
# Delete file/directory on a remote host
#
def delete_remote_file(file, host, logact = 0):

   if not check_remote_file(file, host, logact): return PgLOG.FAILURE
   
   cmd = PgLOG.get_sync_command(host)

   for loop in range(2):
      if PgLOG.pgsystem("{} -d {}".format(cmd, file), logact, CMDERR):
         if DIRLVLS: record_delete_directory(op.dirname(file), host)
         return PgLOG.SUCCESS
   
      errlog(PgLOG.PGLOG['SYSERR'], 'R', loop, logact)

   return PgLOG.FAILURE

#
# Delete a file on object store  
#
def delete_object_file(file, bucket = None, logact = 0):

   if not bucket: bucket = PgLOG.PGLOG['OBJCTBKT']
   for loop in range(2):
      list = object_glob(file, bucket, 0, logact)
      if not list: return PgLOG.FAILURE
      errmsg = None
      for key in list:
         cmd = "{} dl {} -b {}".format(OBJCTCMD, key, bucket)
         if not PgLOG.pgsystem(cmd, logact, CMDERR):
            errmsg = PgLOG.PGLOG['SYSERR']
            break
   
      list = object_glob(file, bucket, 0, logact)
      if not list: return PgLOG.SUCCESS
      if errmsg: errlog(errmsg, 'O', loop, logact)

   return PgLOG.FAILURE

#
# Delete a backup file on Quasar Server  
#
def delete_backup_file(file, endpoint = None, logact = 0):

   if not endpoint: endpoint = PgLOG.PGLOG['BACKUPEP']
   info = check_backup_file(file, endpoint, 0, logact)
   if not info: return PgLOG.FAILURE

   cmd = "{} -d -ep {} -tf {}".format(BACKCMD, endpoint, file)
   task = submit_globus_task(cmd, endpoint, logact)
   if task['stat'] == 'S':
      return PgLOG.SUCCESS
   elif task['stat'] == 'A':
      TASKIDS["{}-{}".format(endpoint, file)] = task['id']
      return PgLOG.FINISH

   return PgLOG.FAILURE

#
# reset local file/directory information to make them writable for PgLOG.PGLOG['RDAUSER']
# file - file name (mandatory)
# info - gathered file info with option 14, None means file not exists
#
def reset_local_info(file, info = None, logact = 0):

   ret = 0
   if info:
      if info['isfile']:
         ret += reset_local_file(file, info, logact)
         dir = get_local_dirname(file)
         info = check_local_file(dir, 14, logact)
      else:
         dir = file
   else:
      dir = get_local_dirname(file)
      info = check_local_file(dir, 14, logact)

   if info: ret += reset_local_directory(dir, info, logact)
   
   return 1 if ret else 0

#
# reset local directory group/mode
#
def reset_local_directory(dir, info = None, logact = 0):

   ret = 0
   if not (info and 'mode' in info and 'group' in info and 'logname' in info):
      info = check_local_file(dir, 14, logact)
   if info:
      if info['mode'] and info['mode'] != 0o775:
         ret += set_local_mode(dir, 0, 0o775, info['mode'], info['logname'], logact)
      if info['group'] and PgLOG.PGLOG['RDAGRP'] != info['group']:
         ret += change_local_group(dir, PgLOG.PGLOG['RDAGRP'], info['group'], info['logname'], logact)

   return 1 if ret else 0

#
# reset local file group/mode
#
def reset_local_file(file, info = None, logact = 0):

   ret = 0
   if not (info and 'mode' in info and 'group' in info and 'logname' in info):
      info = check_local_file(file, 14, logact)
   if info:
      if info['mode'] != 0o664:
         ret += set_local_mode(file, 1, 0o664, info['mode'], info['logname'], logact)
      if PgLOG.PGLOG['RDAGRP'] != info['group']:
         ret += change_local_group(file, PgLOG.PGLOG['RDAGRP'], info['group'], info['logname'], logact)

   return ret

#
# Move file locally or remotely on the same host no background process for moving
#
#   tofile - target file name
# fromfile - original file name
#     host - host name the file is moved on, default to LHOST
#
# Return PgLOG.SUCCESS if successful PgLOG.FAILURE otherwise
#
def move_rda_file(tofile, fromfile, host, logact = 0):

   shost = strip_host_name(host)
   if PgUtil.pgcmp(shost, LHOST, 1) == 0:
      return move_local_file(tofile, fromfile, logact)
   elif PgUtil.pgcmp(shost, OHOST, 1) == 0:
      return move_object_file(tofile, fromfile, None, None, logact)
   else:
      return move_remote_file(tofile, fromfile, host, logact)

#
# Move a file locally
#
#   tofile - target file name
# fromfile - source file name
#
def move_local_file(tofile, fromfile, logact = 0):

   dir = get_local_dirname(tofile)
   info = check_local_file(fromfile, 0, logact)
   tinfo = check_local_file(tofile, 0, logact)
   if not info:
      if info != None: return PgLOG.FAILURE
      if tinfo:
         PgLOG.pglog("{}: Moved to {} already".format(fromfile, tofile), PgLOG.LOGWRN)
         return PgLOG.SUCCESS
      else:
         return errlog("{}: {} to move".format(fromfile, PgLOG.PGLOG['MISSFILE']), 'L', 1, logact)
   if tinfo:
      if tinfo['data_size'] > 0 and not logact&PgLOG.OVRIDE:
         return errlog("{}: File exists, cannot move {} to it".format(tofile, fromfile), 'L', 1, logact)
   elif tinfo != None:
      return PgLOG.FAILURE

   if not make_local_directory(dir, logact): return PgLOG.FAILURE
 
   cmd = "mv {} {}".format(fromfile, tofile)
   loop = reset = 0
   while (loop-reset) < 2:
      if PgLOG.pgsystem(cmd, logact, CMDERR):
         if DIRLVLS: record_delete_directory(op.dirname(fromfile), LHOST)
         return PgLOG.SUCCESS
   
      errlog(PgLOG.PGLOG['SYSERR'], 'L', (loop - reset), logact)
      if loop == 0: reset = reset_local_info(tofile, info, logact)
      loop += 1

   return PgLOG.FAILURE

#
# Move a remote file on the same host
#
#   tofile - target file name
# fromfile - original file name
#     host - remote host name
#  locfile - local copy of tofile
#
def move_remote_file(tofile, fromfile, host, logact = 0):

   if is_local_host(host): return move_local_file(tofile, fromfile, logact)

   ret = PgLOG.FAILURE
   dir = op.dirname(tofile)
   info = check_remote_file(fromfile, host, 0, logact)
   tinfo = check_remote_file(tofile, host, 0, logact)
   if not info:
      if info != None: return PgLOG.FAILURE
      if tinfo:
         PgLOG.pglog("{}-{}: Moved to {} already".format(host, fromfile, tofile), PgLOG.LOGWRN)
         return PgLOG.SUCCESS
      else:
         return errlog("{}-{}: {} to move".format(host, fromfile, PgLOG.PGLOG['MISSFILE']), 'R', 1, logact)   
   if tinfo:
      if tinfo['data_size'] > 0 and not logact&PgLOG.OVRIDE:
         return errlog("{}-{}: File exists, cannot move {} to it".format(host, tofile, fromfile), 'R', 1, logact)
   elif tinfo != None:
      return PgLOG.FAILURE

   if make_remote_directory(dir, host, logact):
      locfile = "{}/{}".format(PgLOG.PGLOG['TMPPATH'], op.basename(tofile))
      if remote_copy_local(locfile, fromfile, host, logact):
         ret = local_copy_remote(tofile, locfile, host, logact)
         delete_local_file(locfile, logact)
         if ret:
            ret = delete_remote_file(fromfile, host, logact)
            if DIRLVLS: record_delete_directory(op.dirname(fromfile), host)

   return ret

#
# Move an object file on Object Store
#
#     tofile - target file name
#   fromfile - original file name
#   tobucket - target bucket name
# frombucket - original bucket name
#
def move_object_file(tofile, fromfile, tobucket, frombucket, logact = 0):

   ret = PgLOG.FAILURE
   if not tobucket: tobucket = PgLOG.PGLOG['OBJCTBKT']
   if not frombucket: frombucket = tobucket
   finfo = check_object_file(fromfile, frombucket, 0, logact)
   tinfo = check_object_file(tofile, tobucket, 0, logact)
   if not finfo:
      if finfo != None: return PgLOG.FAILURE
      if tinfo:
         PgLOG.pglog("{}-{}: Moved to {}-{} already".format(frombucket, fromfile, tobucket, tofile), PgLOG.LOGWRN)
         return PgLOG.SUCCESS
      else:
         return errlog("{}-{}: {} to move".format(frombucket, fromfile, PgLOG.PGLOG['MISSFILE']), 'R', 1, logact)   
   if tinfo:
      if tinfo['data_size'] > 0 and not logact&PgLOG.OVRIDE:
         return errlog("{}-{}: Object File exists, cannot move {}-{} to it".format(tobucket, tofile, frombucket, fromfile), 'R', 1, logact)
   elif tinfo != None:
      return PgLOG.FAILURE

   cmd = "{} mv -b {} -db {} -k {} -dk {}".format(OBJCTCMD, frombucket, tobucket, fromfile, tofile)
   ucmd = "{} gm -k {} -b {}".format(OBJCTCMD, fromfile, frombucket)
   ubuf = PgLOG.pgsystem(ucmd, PgLOG.LOGWRN, CMDRET)
   if ubuf and re.match(r'^\{', ubuf): cmd += " -md '{}'".format(ubuf)

   for loop in range(2):
      buf = PgLOG.pgsystem(cmd, logact, CMDBTH)
      tinfo = check_object_file(tofile, tobucket, 0, logact)
      if tinfo:
         if tinfo['data_size'] == finfo['data_size']:
            return PgLOG.SUCCESS
      elif tinfo != None:
         break

      errlog("Error Execute: {}\n{}".format(cmd, buf), 'O', loop, logact)

   return PgLOG.FAILURE

#
# Move an object path on Object Store and all the file keys under it
#
#     topath - target path name
#   frompath - original path name
#   tobucket - target bucket name
# frombucket - original bucket name
#
def move_object_path(topath, frompath, tobucket, frombucket, logact = 0):

   ret = PgLOG.FAILURE
   if not tobucket: tobucket = PgLOG.PGLOG['OBJCTBKT']
   if not frombucket: frombucket = tobucket
   fcnt = check_object_path(frompath, frombucket, logact)
   tcnt = check_object_path(topath, tobucket, logact)
   if not fcnt:
      if fcnt == None: return PgLOG.FAILURE
      if tcnt:
         PgLOG.pglog("{}-{}: Moved to {}-{} already".format(frombucket, frompath, tobucket, topath), PgLOG.LOGWRN)
         return PgLOG.SUCCESS
      else:
         return errlog("{}-{}: {} to move".format(frombucket, frompath, PgLOG.PGLOG['MISSFILE']), 'R', 1, logact)   

   cmd = "{} mv -b {} -db {} -k {} -dk {}".format(OBJCTCMD, frombucket, tobucket, frompath, topath)

   for loop in range(2):
      buf = PgLOG.pgsystem(cmd, logact, CMDBTH)
      fcnt = check_object_path(frompath, frombucket, logact)
      if not fcnt: return PgLOG.SUCCESS
      errlog("Error Execute: {}\n{}".format(cmd, buf), 'O', loop, logact)

   return PgLOG.FAILURE

#
# Move a backup file on Quasar Server
#
#   tofile - target file name
# fromfile - source file name
# endpoint - Globus endpoint
#
def move_backup_file(tofile, fromfile, endpoint = None, logact = 0):

   ret = PgLOG.FAILURE
   if not endpoint: endpoint = PgLOG.PGLOG['BACKUPEP']
   finfo = check_backup_file(fromfile, endpoint, 0, logact)
   tinfo = check_backup_file(tofile, endpoint, 0, logact)
   if not finfo:
      if finfo != None: return ret
      if tinfo:
         PgLOG.pglog("{}: Moved to {} already".format(fromfile, tofile), PgLOG.LOGWRN)
         return PgLOG.SUCCESS
      else:
         return errlog("{}: {} to move".format(fromfile, PgLOG.PGLOG['MISSFILE']), 'B', 1, logact)
   
   if tinfo:
      if tinfo['data_size'] > 0 and not logact&PgLOG.OVRIDE:
         return errlog("{}: File exists, cannot move {} to it".format(tofile, fromfile), 'B', 1, logact)
   elif tinfo != None:
      return ret

   cmd = "{} --rename -ep {} --oldpath {} --newpath {}".format(BACKCMD, endpoint, fromfile, tofile)
   loop = 0
   while loop < 2:
      buf = PgLOG.pgsystem(cmd, logact, CMDRET)
      syserr = PgLOG.PGLOG['SYSERR']
      if buf:
         if buf.find('File or directory renamed successfully') > -1:
            ret = PgLOG.SUCCESS
            break
      if syserr:
         if syserr.find("No such file or directory") > -1:
            if make_backup_directory(op.dirname(tofile), endpoint, logact): continue
         errmsg = "Error Execute: {}\n{}".format(cmd, syserr)
         (hstat, msg) = host_down_status('', QHOSTS[endpoint], 1, logact)
         if hstat: errmsg += "\n" + msg
         errlog(errmsg, 'B', loop, logact)
      loop += 1

   if ret == PgLOG.SUCCESS: ECNTS['B'] = 0   # reset error count
   return ret

#
# Make a directory on a given host name (including local host)
#
#  dir - directory path to be made
# host - host name the directory on, default to LHOST
#
# Return PgLOG.SUCCESS(1) if successful or PgLOG.FAILURE(0) if failed
#
def make_rda_directory(dir, host, logact = 0):

   if not dir: return PgLOG.SUCCESS
   shost = strip_host_name(host)
   if PgUtil.pgcmp(shost, LHOST, 1) == 0:
      return make_local_directory(dir, logact)
   else:
      return make_remote_directory(dir, host, logact)

#
# Make a local directory
#
# dir - directory path to be made
#
def make_local_directory(dir, logact = 0):

   return make_one_local_directory(dir, None, logact)

#
# Make a local directory recursively
#
def make_one_local_directory(dir, odir = None, logact = 0):

   if not dir or op.isdir(dir): return PgLOG.SUCCESS
   if op.isfile(dir): return errlog(dir + ": is file, cannot make directory", 'L', 1, logact)

   if not odir: odir = dir
   if is_root_directory(dir, 'L', LHOST, "make directory " + odir, logact): return PgLOG.FAILURE
   if not make_one_local_directory(op.dirname(dir), odir, logact): return PgLOG.FAILURE

   loop = reset = 0
   while (loop-reset) < 2:
      try:
         os.mkdir(dir, PgLOG.PGLOG['EXECMODE'])
      except Exception as e:
         errmsg = str(e)
         if errmsg.find('File exists') > -1: return PgLOG.SUCCESS
         errlog(errmsg, 'L', (loop - reset), logact)
         if loop == 0: reset = reset_local_info(dir, None, logact)
         loop += 1
      else:
         return PgLOG.SUCCESS

   return PgLOG.FAILURE

#
# Make a directory on a remote host name
#
#  dir - directory path to be made
# host - host name the directory on
#
def make_remote_directory(dir, host, logact = 0):
   
   return make_one_remote_directory(dir, None, host, logact)

def make_one_remote_directory(dir, odir, host, logact = 0):

   info = check_remote_file(dir, host, 0, logact)
   if info:
      if info['isfile']: return errlog("{}-{}: is file, cannot make directory".format(host, dir), 'R', 1, logact)
      return PgLOG.SUCCESS
   elif info != None:
      return PgLOG.FAILURE

   if not odir: odir = dir
   if is_root_directory(dir, 'R', host, "make directory {} on {}".format(odir, host), logact): return PgLOG.FAILURE
 
   if make_one_remote_directory(op.dirname(dir), odir, host, logact):
      if PgLOG.pgsystem("{} {} {}".format(PgLOG.get_sync_command(host), PgLOG.PGLOG['TMPSYNC'], dir), logact, 5):
         set_remote_mode(dir, 0, host, PgLOG.PGLOG['EXECMODE'])
         return PgLOG.SUCCESS
   
   return PgLOG.FAILURE

#
# Make a quasar directory
#
# dir - directory path to be made
#
def make_backup_directory(dir, endpoint, logact = 0):

   return make_one_backup_directory(dir, None, endpoint, logact)

#
# Make a quasar directory recursively
#
def make_one_backup_directory(dir, odir, endpoint = None, logact = 0):

   if not dir or dir == '/': return PgLOG.SUCCESS
   if not endpoint: endpoint = PgLOG.PGLOG['BACKUPEP']
   info = check_backup_file(dir, endpoint, 0, logact)
   if info:
      if info['isfile']: return errlog("{}-{}: is file, cannot make backup directory".format(endpoint, dir), 'B', 1, logact)
      return PgLOG.SUCCESS
   elif info != None:
      return PgLOG.FAILURE

   if not odir: odir = dir
   if not make_one_backup_directory(op.dirname(dir), odir, endpoint, logact): return PgLOG.FAILURE

   cmd = "{} --mkdir -ep {} -p {}".format(BACKCMD, endpoint, dir)
   for loop in range(2):
      buf = PgLOG.pgsystem(cmd, logact, CMDRET)
      syserr = PgLOG.PGLOG['SYSERR']
      if buf:
         if(buf.find('The directory was created successfully') > -1 or
            buf.find("Path '{}' already exists".format(dir)) > -1):
            ret = PgLOG.SUCCESS
            break
      if syserr:
         if syserr.find("No such file or directory") > -1:
            ret = make_one_backup_directory(op.dirname(dir), odir, endpoint, logact)
            if ret == PgLOG.SUCCESS or loop: break
            time.sleep(PgSIG.PGSIG['ETIME'])
         else:
            errmsg = "Error Execute: {}\n{}".format(cmd, syserr)
            (hstat, msg) = host_down_status('', QHOSTS[endpoint], 1, logact)
            if hstat: errmsg += "\n" + msg
            errlog(errmsg, 'B', loop, logact)

   if ret == PgLOG.SUCCESS: ECNTS['B'] = 0   # reset error count
   return ret

#
# check and return 1 if a root directory
#
def is_root_directory(dir, etype, host = None, action = None, logact = 0):

   ret = cnt = 0

   if etype == 'H':
      ms = re.match(r'^({})(.*)$'.format(PgLOG.PGLOG['ALLROOTS']), dir)
      if ms:
         m2 = ms.group(2) 
         if not m2 or m2 == '/': ret = 1 
      else:
         cnt = 2  
   elif re.match(r'^{}'.format(PgLOG.PGLOG['DSSDATA']), dir):
      ms = re.match(r'^({})(.*)$'.format(PgLOG.PGLOG['GPFSROOTS']), dir)
      if ms:
         m2 = ms.group(2) 
         if not m2 or m2 == '/': ret = 1 
      else:
         cnt = 4
   else:
      ms = re.match(r'^({})(.*)$'.format(PgLOG.PGLOG['HOMEROOTS']), dir)
      if ms:
         m2 = ms.group(2) 
         if not m2 or m2 == '/': ret = 1 
      else:
         cnt = 2

   if cnt and re.match(r'^(/[^/]+){0,%d}(/*)$' % cnt, dir):
      ret = 1

   if ret and action:
      cnt = 0
      errmsg = "{}: Cannot {} from {}".format(dir, action, PgLOG.PGLOG['HOSTNAME'])
      (hstat, msg) = host_down_status(dir, host, 0, logact)
      if hstat: errmsg += "\n" + msg
      errlog(errmsg, etype, 1, logact|PgLOG.ERRLOG)

   return ret

#
# set mode for a given direcory/file on a given host (include local host)
#
def set_rda_mode(file, isfile, host, nmode = None, omode = None, logname = None, logact = 0):
   
   shost = strip_host_name(host)
   if PgUtil.pgcmp(shost, LHOST, 1) == 0:
      return set_local_mode(file, isfile, nmode, omode, logname, logact)
   else:
      return set_remote_mode(file, isfile, host, nmode, omode, logact)      

#
# set mode for given local directory or file
#
def set_local_mode(file, isfile = 1, nmode = 0, omode = 0, logname = None, logact = 0):

   if not nmode: nmode = (PgLOG.PGLOG['FILEMODE'] if isfile else PgLOG.PGLOG['EXECMODE'])
   if not (omode and logname):
      info = check_local_file(file, 6)
      if not info:
         if info != None: return PgLOG.FAILURE 
         return lmsg(file, "{} to set mode({})".format(PgLOG.PGLOG['MISSFILE'], PgLOG.int2base(nmode, 8)), logact)   
      omode = info['mode']
      logname = info['logname']

   if nmode == omode: return PgLOG.SUCCESS

   try:
      os.chmod(file, nmode)
   except Exception as e:
      return errlog(str(e), 'L', 1, logact)

   return PgLOG.SUCCESS

#
# set mode for given directory or file on remote host
#
def set_remote_mode(file, isfile, host, nmode = 0, omode = 0, logact = 0):

   if not nmode: nmode = (PgLOG.PGLOG['FILEMODE'] if isfile else PgLOG.PGLOG['EXECMODE'])
   if not omode:
      info = check_remote_file(file, host, 6)
      if not info:
         if info != None: return PgLOG.FAILURE
         return errlog("{}-{}: {} to set mode({})".format(host, file, PgLOG.PGLOG['MISSFILE'], PgLOG.int2base(nmode, 8)), 'R', 1, logact)
      omode = info['mode']

   if nmode == omode: return PgLOG.SUCCESS
   return PgLOG.pgsystem("{} -m {} {}".format(PgLOG.get_sync_command(host), PgLOG.int2base(nmode, 8), file), logact, 5)

#
# change group for given local directory or file
#
def change_local_group(file, ngrp = None, ogrp = None, logname = None, logact = 0):

   if not ngrp:
      ngid = PgLOG.PGLOG['RDAGID']
   else:
      ngid = grp.getgrnam[ngrp].gr_gid
   if logact and logact&PgLOG.EXITLG: logact &=~PgLOG.EXITLG
   if not (ogrp and logname):
      info = check_local_file(file, 10, logact)
      if not info:
         if info != None: return PgLOG.FAILURE
         return errlog("{}: {} to change group({})".format(file, PgLOG.PGLOG['MISSFILE'], ngrp), 'L', 1, logact)   
      ogid = info['gid']
      ouid = info['uid']
   else:
      ouid = pwd.getpwnam(logname).pw_uid
      ogid = grp.getgrnam(logname).gr_gid

   if ngid == ogid: return PgLOG.SUCCESS

   try:
      os.chown(file, ouid, ngid)
   except Exception as e:
      return errlog(str(e), 'L', 1, logact)

#
# Check if given path on a specified host or the host itself are down
#
#   path: path name to be checked
#   host: host name the file on, default to LHOST
# chkopt: 1 - do a file/path check, 0 - do not 
#
# Return array of 2 (hstat, msg)
#         hstat: 0 if system is up and accessible,
#                1 - host is down,
#                2 - if path not accessible
#                negative values if planned system down
#           msg: None - stat == 0
#                an unempty string for system down message - stat != 0 
#
def host_down_status(path, host, chkopt = 0, logact = 0):

   shost = strip_host_name(host)
   hstat = 0
   rets = [0, None]
   
   msg = hostname = None

   if PgUtil.pgcmp(shost, LHOST, 1) == 0:
      if not path or (chkopt and check_local_file(path)): return rets
      msg = path + ": is not accessible"
      flag = "L"
      if re.match(r'^(/{}/|{})'.format(PgLOG.PGLOG['GPFSNAME'], PgLOG.PGLOG['DSSDATA']), path):
         hstat = 1
         hostname = PgLOG.PGLOG['GPFSNAME']
      else:
         hstat = 2
   
   elif PgUtil.pgcmp(shost, PgLOG.PGLOG['GPFSNAME'], 1) == 0:
      if not path or (chkopt and check_local_file(path)): return rets
      msg = path + ": is not accessible"
      flag = "L"
      hstat = 1
      hostname = PgLOG.PGLOG['GPFSNAME']
   elif PgUtil.pgcmp(shost, BHOST, 1) == 0:
      if path:
         hstat = 2
      else:
         hstat = 1
         path = DPATHS['B']

      if chkopt and check_backup_file(path, QPOINTS['B']): return rets
      hostname = BHOST
      msg = "{}-{}: is not accessible".format(hostname, path)
      flag = "B"
   elif PgUtil.pgcmp(shost, DHOST, 1) == 0:
      if path:
         hstat = 2
      else:
         hstat = 1
         path = DPATHS['B']

      if chkopt and check_backup_file(path, QPOINTS['D']): return rets
      hostname = DHOST
      msg = "{}-{}: is not accessible".format(hostname, path)
      flag = "D"
   elif PgUtil.pgcmp(shost, OHOST, 1) == 0:
      if path:
         hstat = 2
      else:
         hstat = 1
         path = PgLOG.PGLOG['OBJCTBKT']
   
      if chkopt and check_object_file(path): return rets
 
      hostname = OHOST
      msg = "{}-{}: is not accessible".format(hostname, path)
      flag = "O"
   elif PgUtil.pgcmp(shost, PgLOG.PGLOG['PGBATCH'], 1):
      if path and chkopt and check_remote_file(path, host): return rets
      estat = ping_remote_host(host)
      if estat:
         hstat = 1
         hostname = host
      else:
         if not path: return rets
         if re.match(r'^/{}/'.format(PgLOG.PGLOG['GPFSNAME']), path):
            hstat = 1
            hostname = PgLOG.PGLOG['GPFSNAME']
         else:
            hstat = 2
            hostname = host      
   
      flag = "R"
      msg = "{}-{}: is not accessible".format(host, path)
   elif PgLOG.get_host(1) == PgLOG.PGLOG['PGBATCH']:   # local host is a batch node
      if not path or (chkopt and check_local_file(path)): return rets
      msg = path + ": is not accessible"
      flag = "L"
      if re.match(r'^(/{}/|{})'.format(PgLOG.PGLOG['GPFSNAME'], PgLOG.PGLOG['DSSDATA']), path):
         hstat = 1
         hostname = PgLOG.PGLOG['GPFSNAME']
      else:
         hstat = 2
   
   msg += " at the moment Checked on " + PgLOG.PGLOG['HOSTNAME']

   if hostname:
     estat = PgDBI.system_down_message(hostname, path, 0, logact) 
     if estat:
        hstat = -hstat
        msg += "\n" + estat

   if logact and (chkopt or hstat < 0): errlog(msg, flag, 1, logact)
   
   return (hstat, msg)

#
# Check if given path on a specified host is down or not
#
# path: path name to be checked
# host: host name the file on, default to LHOST
#
# Return errmsg if not accessible and None otherwise
#
def check_host_down(path, host, logact = 0):

   (hstat, msg) = host_down_status(path, host, 1, logact)

   return msg if hstat else None

#
# Check if given service name is accessible from a specified host
#
#  sname: service name to be checked
#  fhost: from host name to connect to service, default to LHOST
#
#  reset the service flag to A or I accordingly
#
# Return 0 if accessible, dsservice.sindex if not, and -1 if can not be checked
#
def check_service_accessibilty(sname, fhost = None, logact = 0):

   if not fhost: fhost = PgLOG.PGLOG['HOSTNAME']
   pgrec = PgDBI.pgget("dsservice", "*", "service = '{}' AND hostname = '{}'".format(sname, fhost), logact)
   if not pgrec:
      PgLOG.pglog("dsservice: Access {} from {} is not defined in RDA Configuration".format(sname, fhost), logact)
      return -1

   path = sname if (pgrec['flag'] == "H" or pgrec['flag'] == "G") else None
   (hstat, msg) = host_down_status(path, fhost, 1, logact)

   return msg if hstat else None

#
# check if this host is a local host for given host name
#
def is_local_host(host):

   host = strip_host_name(host)
   if host == LHOST or PgLOG.valid_batch_host(host): return 1

   return 0

#
# check and return action string on a node other than local one
#
def local_host_action(host, action, info, logact = 0):

   if is_local_host(host): return 1
   if not logact: return 0

   if host == "partition":
      msg = "for individual partition"
   elif host == "rda_config":
      msg = "via https://rda.ucar.edu/internal/rda_pg_config"
   elif host in PgLOG.BCHCMDS:
      msg = "on a {} Node".format(host)
   else:
      msg = "on " + host

   return PgLOG.pglog("{}: Cannot {}, try {}".format(info, action, msg), logact)

#
# ping a given remote host name
#
# return None if system is up error messge if not
#
def ping_remote_host(host):

   while True:
      buf = PgLOG.pgsystem("ping -c 3 " + host, PgLOG.LOGWRN, CMDRET)
      if buf:
         ms = re.search(r'3 packets transmitted, (\d)', buf)
         if ms:
            if int(ms.group(1)) > 0:
               return None
            else:
               return host + " seems down not accessible"
      if PgLOG.PGLOG['SYSERR']:
         if PgLOG.PGLOG['SYSERR'].find("ping: unknown host") > -1 and host.find('.') > -1:
            host += ".ucar.edu"
            continue
         return PgLOG.PGLOG['SYSERR']
      else:
         return "Cannot ping " + host

#
# compare given two host names, return 1 if same and 0 otherwise
#
def same_hosts(host1, host2):

   host1 = strip_host_name(host1)
   host2 = strip_host_name(host2)
   
   return (1 if PgUtil.pgcmp(host1, host2, 1) == 0 else 0)

#
#  strip and identify the proper host name
#
def strip_host_name(host):

   if not host: return LHOST

   ms = re.match(r'^([^\.]+)\.', host)
   if ms: host = ms.group(1)
   if PgUtil.pgcmp(host, PgLOG.PGLOG['HOSTNAME'], 1) == 0:
      return LHOST
   else:
      return host

#
# Check a file stuatus info on a given host name (including local host) no background process for checking
#
# file: file name to be checked
# host: host name the file on, default to LHOST
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#       1 - get date/time modified (date_modified, time_modfied)
#       2 - get file owner's login name (logname)
#       4 - get permission mode in 3 octal digits (mode)
#       8 - get group name (group)
#      16 - get week day 0-Sunday, 1-Monday (week_day)
#      32 - get checksum (checksum), work for local file only
#
# Return a dict of file info, or None if file not exists
#
def check_rda_file(file, host = LHOST, opt = 0, logact = 0):

   shost = strip_host_name(host)

   if PgUtil.pgcmp(shost, LHOST, 1) == 0:
      return check_local_file(file, opt, logact)
   elif PgUtil.pgcmp(shost, OHOST, 1) == 0:
      return check_object_file(file, None, opt, logact)      
   elif PgUtil.pgcmp(shost, BHOST, 1) == 0:
      return check_backup_file(file, QPOINTS['B'], opt, logact)      
   elif PgUtil.pgcmp(shost, DHOST, 1) == 0:
      return check_backup_file(file, QPOINTS['D'], opt, logact)      
   else:
      return check_remote_file(file, host, opt, logact)

#
# wrapper to check_local_file() and check_globus_file() to check info for a file
# on local or remote Globus endpoints
#
def check_globus_file(file, endpoint = None, opt = 0, logact = 0):

   if not endpoint: endpoint = PgLOG.PGLOG['BACKUPEP']
   if endpoint == 'rda-glade':
      if re.match(r'^/(data|decsdata)/', file): file = PgLOG.PGLOG['DSSDATA'] + file
      return check_local_file(file, opt, logact)
   else:
      return check_backup_file(file, endpoint, opt, logact)

#
# check and get local file status information
#
# file: local File name
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#       1 - get date/time modified (date_modified, time_modfied)
#       2 - get file owner's login name (logname)
#       4 - get permission mode in 3 octal digits (mode)
#       8 - get group name (group)
#      16 - get week day 0-Sunday, 1-Monday (week_day)
#      32 - get checksum (checksum)
#      64 - remove file too small
#     128 - check twice for missing file
#
# Return: a dict of file info, or None if not exists 
#
def check_local_file(file, opt = 0, logact = 0):

   ret = None
   if not file: return ret
   loop = 0
   while loop < 2:
      if op.exists(file):
         try:
            fstat = os.stat(file)
            ret = local_file_stat(file, fstat, opt, logact)
            break
         except Exception as e:
            errmsg = "{}: {}".format(file, str(e))
            (hstat, msg) = host_down_status(file, LHOST, 0, logact)
            if hstat: errmsg += "\n" + msg
            errlog(errmsg, 'L', loop, logact)
      else:
         if loop > 0 or opt&128 == 0: break
         PgLOG.pglog(file + ": check it again in a moment", PgLOG.LOGWRN)
         time.sleep(6)
      loop += 1

   if loop > 1: return PgLOG.FAILURE
   ECNTS['L'] = 0   # reset error count
   return ret

#
# local function to get local file stat
#
def local_file_stat(file, fstat, opt, logact):

   if not fstat:
      errlog(file + ": Error check file stat", 'L', 1, logact)
      return None

   info = {}
   info['isfile'] = (1 if stat.S_ISREG(fstat.st_mode) else 0)
   info['data_size'] = fstat.st_size
   info['fname'] = op.basename(file)
   if not opt: return info
   if opt&64 and info['isfile'] and info['data_size'] < PgLOG.PGLOG['MINSIZE']:
      PgLOG.pglog("{}: Remove {} file".format(file, ("Small({}B)".format(info['data_size']) if info['data_size'] else "Empty")), logact&~PgLOG.EXITLG)
      delete_local_file(file, logact)
      return None

   if opt&17:
      mdate, mtime = PgUtil.get_date_time(fstat.st_mtime)
      if opt&1:
         info['date_modified'] = mdate
         info['time_modified'] = mtime
         cdate, ctime = PgUtil.get_date_time(fstat.st_ctime)
         info['date_created'] = cdate
         info['time_created'] = ctime
      if opt&16: info['week_day'] = PgUtil.get_weekday(mdate)

   if opt&2:
      info['uid'] = fstat.st_uid
      info['logname'] = pwd.getpwuid(info['uid']).pw_name
   if opt&4: info['mode'] = stat.S_IMODE(fstat.st_mode)
   if opt&8:
      info['gid'] = fstat.st_gid
      info['group'] = grp.getgrgid(info['gid']).gr_name
   if opt&32: info['checksum'] = get_md5sum(file, 0, logact)

   return info

#
# check and get file status information of a file on remote host
#
# file: remote File name
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#       1 - get date/time modified (date_modified, time_modfied)
#       2 - file owner's login name (logname), assumed 'rdadata'
#       4 - get permission mode in 3 octal digits (mode)
#       8 - get group name (group), assumed 'dss'
#      16 - get week day 0-Sunday, 1-Monday (week_day)
#
# Return: a dict of file info, or None if not exists 
#
def check_remote_file(file, host, opt = 0, logact = 0):

   if not file: return None
   ms = re.match(r'^(.+)/$', file)
   if ms: file = ms.group(1)    # remove ending '/' in case
   cmd = "{} {}".format(PgLOG.get_sync_command(host), file)
   loop = 0
   while loop < 2:
      buf = PgLOG.pgsystem(cmd, PgLOG.LOGWRN, CMDRET)
      if buf or not PgLOG.PGLOG['SYSERR'] or PgLOG.PGLOG['SYSERR'].find(PgLOG.PGLOG['MISSFILE']) > -1: break
      errmsg = PgLOG.PGLOG['SYSERR']
      (hstat, msg) = host_down_status(file, host, 0, logact)
      if hstat: errmsg += "\n" + msg
      errlog(errmsg, 'R', loop, logact)
      loop += 1

   if loop > 1: return PgLOG.FAILURE
   ECNTS['R'] = 0   # reset error count
   if buf:
      for line in re.split(r'\n', buf):
         info = remote_file_stat(line, opt)
         if info: return info

   return None

#
# local function to get remote file stat
#
def remote_file_stat(line, opt):

   info = {}
   items = re.split(r'\s+', line)
   if len(items) < 5 or items[4] == '.': return None
   ms = re.match(r'^([d\-])([\w\-]{9})$',  items[0])
   info['isfile'] = (1 if ms and ms.group(1) == "-" else 0)
   if opt&4: info['mode'] = get_file_mode(ms.group(2))
   fsize = items[1]
   if fsize.find(',') > -1: fsize = re.sub(r',', '', fsize)
   info['data_size'] = int(fsize)
   info['fname'] = op.basename(items[4])
   if not opt: return info
   if opt&17:
      mdate = PgUtil.format_date(items[2], "YYYY-MM-DD", "YYYY/MM/DD")
      mtime = items[3]
      if PgLOG.PGLOG['GMTZ']: (mdate, mtime) = PgUtil.addhour(mdate, mtime, PgLOG.PGLOG['GMTZ'])
      if opt&1:
         info['date_modified'] = mdate
         info['time_modified'] = mtime
      if opt&16: info['week_day'] = PgUtil.get_weekday(mdate)

   if opt&2: info['logname'] = "rdadata"
   if opt&8: info['group'] = PgLOG.PGLOG['RDAGRP']

   return info

#
# check and get object file status information
#
# file: object store File key name
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#       1 - get date/time modified (date_modified, time_modfied)
#       2 - get file owner's login name (logname)
#       4 - get metadata hash
#       8 - get group name (group)
#      16 - get week day 0-Sunday, 1-Monday (week_day)
#      32 - get checksum (checksum)
#      64 - check once, no rechecking
#
# Return a dict of file info, or None if file not exists
#
def check_object_file(file, bucket = None, opt = 0, logact = 0):

   if not bucket: bucket = PgLOG.PGLOG['OBJCTBKT']
   ret = None
   if not file: return ret
   cmd = "{} lo {} -b {}".format(OBJCTCMD, file, bucket)
   ucmd = "{} gm -k {} -b {}".format(OBJCTCMD, file, bucket) if opt&14 else None
   loop = 0
   while loop < 2:
      buf = PgLOG.pgsystem(cmd, PgLOG.LOGWRN, CMDRET)
      if buf:
         if re.match(r'^\[\]', buf): break
         if re.match(r'^\[\{', buf):
            ary = json.loads(buf)
            cnt = len(ary)
            if cnt > 1: return PgLOG.pglog("{}-{}: {} records returned\n{}".format(bucket, file, cnt, buf), logact|PgLOG.ERRLOG)
            hash = ary[0]
            uhash = None
            if ucmd:
               ubuf = PgLOG.pgsystem(ucmd, PgLOG.LOGWRN, CMDRET)
               if ubuf and re.match(r'^\{', ubuf): uhash = json.loads(ubuf)
            ret = object_file_stat(hash, uhash, opt)
            break
      if opt&64: return PgLOG.FAILURE
      errmsg = "Error Execute: {}\n{}".format(cmd, PgLOG.PGLOG['SYSERR'])
      (hstat, msg) = host_down_status(bucket, OHOST, 0, logact)
      if hstat: errmsg += "\n" + msg
      errlog(errmsg, 'O', loop, logact)
      loop += 1

   if loop > 1: return PgLOG.FAILURE
   ECNTS['O'] = 0   # reset error count
   return ret

#
# check an object path status information
#
# path: object store path name
#
# Return count of object key names, 0 if not file exists; None if error checking
#
def check_object_path(path, bucket = None, logact = 0):

   if not bucket: bucket = PgLOG.PGLOG['OBJCTBKT']
   ret = None
   if not path: return ret
   cmd = "{} lo {} -ls -b {}".format(OBJCTCMD, path, bucket)
   loop = 0
   while loop < 2:
      buf = PgLOG.pgsystem(cmd, PgLOG.LOGWRN, CMDRET)
      if buf:
         ary = json.loads(buf)
         return len(ary)
      errmsg = "Error Execute: {}\n{}".format(cmd, PgLOG.PGLOG['SYSERR'])
      (hstat, msg) = host_down_status(bucket, OHOST, 0, logact)
      if hstat: errmsg += "\n" + msg
      errlog(errmsg, 'O', loop, logact)
      loop += 1

   ECNTS['O'] = 0   # reset error count
   return ret

#
# object store function to get file stat
#
def object_file_stat(hash, uhash, opt):

   info = {'isfile' : 1, 'data_size' : int(hash['Size']), 'fname' : op.basename(hash['Key'])}
   if not opt: return info   
   if opt&17:
      ms = re.match(r'^(\d+-\d+-\d+)\s+(\d+:\d+:\d+)', hash['LastModified'])
      if ms:
         (mdate, mtime) = ms.groups()
         if PgLOG.PGLOG['GMTZ']: (mdate, mtime) = PgUtil.addhour(mdate, mtime, PgLOG.PGLOG['GMTZ'])
         if opt&1:
            info['date_modified'] = mdate
            info['time_modified'] = mtime
         if opt&16: info['week_day'] = PgUtil.get_weekday(mdate)
   if opt&32:
      ms = re.match(r'"(.+)"',  hash['ETag'])
      if ms: info['checksum'] = ms.group(1)
   if uhash:
      if opt&2: info['logname'] = uhash['user']
      if opt&4: info['meta'] = uhash
      if opt&8: info['group'] = uhash['group']

   return info

#
# check and get backup file status information
#
# file: backup File key name
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#       1 - get date/time modified (date_modified, time_modfied)
#       2 - get file owner's login name (logname)
#       4 - get metadata hash
#       8 - get group name (group)
#      16 - get week day 0-Sunday, 1-Monday (week_day)
#      64 - rechecking
#
# Return a dict of file info, or None if file not exists
#
def check_backup_file(file, endpoint = None, opt = 0, logact = 0):

   ret = None
   if not file: return ret
   if not endpoint: endpoint = PgLOG.PGLOG['BACKUPEP']
   bdir = op.dirname(file)
   bfile = op.basename(file)
   cmd = "{} -ls -ep {} -p {} --filter {}".format(BACKCMD, endpoint, bdir, bfile)
   ccnt = loop = 0
   while loop < 2:
      buf = PgLOG.pgsystem(cmd, logact, CMDRET)
      syserr = PgLOG.PGLOG['SYSERR']
      if buf:
         getstat = 0
         for line in re.split(r'\n', buf):
            if re.match(r'^(User|-+)\s*\|', line):
               getstat += 1
            elif getstat > 1:
               ret = backup_file_stat(line, opt)
               if ret: break
         if ret: break
         if loop or opt&64 == 0: return ret
         time.sleep(PgSIG.PGSIG['ETIME'])
      elif syserr:
         if syserr.find("Directory '{}' not found on endpoint".format(bdir)) > -1:
            if loop or opt&64 == 0: return ret
            time.sleep(PgSIG.PGSIG['ETIME'])
         elif ccnt < 2 and syserr.find("The connection to the server was broken") > -1:
            time.sleep(PgSIG.PGSIG['ETIME'])
            ccnt += 1
            continue
         else:
            if opt&64 == 0: return PgLOG.FAILURE
            errmsg = "Error Execute: {}\n{}".format(cmd, syserr)
            (hstat, msg) = host_down_status('', QHOSTS[endpoint], 0, logact)
            if hstat: errmsg += "\n" + msg
            errlog(errmsg, 'B', loop, logact)
      loop += 1

   if ret: ECNTS['B'] = 0   # reset error count
   return ret

#
# backup store function to get file stat
#
def backup_file_stat(line, opt):

   info = {}
   items = re.split(r'[\s\|]+', line)
   if len(items) < 8: return None
   info['isfile'] = (1 if items[6] == 'file' else 0)
   info['data_size'] = int(items[3])
   info['fname'] = items[7]
   if not opt: return info
   if opt&17:
      mdate = items[4]
      mtime = items[5]
      ms = re.match(r'^(\d+:\d+:\d+)', mtime)
      if ms: mtime = ms.group(1)
      if PgLOG.PGLOG['GMTZ']: (mdate, mtime) = PgUtil.addhour(mdate, mtime, PgLOG.PGLOG['GMTZ'])
      if opt&1:
         info['date_modified'] = mdate
         info['time_modified'] = mtime
      if opt&16: info['week_day'] = PgUtil.get_weekday(mdate)
   if opt&2: info['logname'] = items[0]
   if opt&4: info['mode'] = get_file_mode(items[2])
   if opt&8: info['group'] = items[1]

   return info

#
# check and get a file status information inside a tar file
#
# file: File name to be checked
# tfile: the tar file name
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#       1 - get date/time modified (date_modified, time_modfied)
#       2 - get file owner's login name (logname)
#       4 - get permission mode in 3 octal digits (mode)
#       8 - get group name (group)
#      16 - get week day 0-Sunday, 1-Monday (week_day)
#
# Return a dict of file info, or None if file not exists
#
def check_tar_file(file, tfile, opt = 0, logact = 0):

   ret = None 
   if not (file and tfile): return ret

   for loop in range(2):
      buf = PgLOG.pgsystem("tar -tvf {} {}".format(tfile, file), PgLOG.LOGWRN, CMDRET)
      if buf or not PgLOG.PGLOG['SYSERR'] or PgLOG.PGLOG['SYSERR'].find('Not found in archive') > -1: break

      errmsg = PgLOG.PGLOG['SYSERR']
      (hstat, msg) = host_down_status(tfile, LHOST, 0, logact)
      errlog(errmsg, 'L', loop, logact)

   if loop > 0: return PgLOG.FAILURE
   if buf:
      for line in re.split(r'\n', buf):
         ret = tar_file_stat(line, opt)
         if ret: break
   ECNTS['L'] = 0   # reset error count

   return ret

#
# local function to get file stat in a tar file
#
def tar_file_stat(line, opt):

   items = re.split(r'\s+', line)
   if len(items) < 6: return None
   ms = re.match(r'^([d\-])([\w\-]{9})$', items[0])
   if not ms: return None
   info = {}
   info['isfile'] = (1 if ms and ms.group(1) == "-" else 0)
   info['data_size'] = int(items[2])
   info['fname'] = op.basename(items[5])
   if not opt: return info
   if opt&4: info['mode'] = get_file_mode(ms.group(2))
   if opt&17:
      mdate = items[3]
      mtime = items[4]
      if PgLOG.PGLOG['GMTZ']: (mdate, mtime) = PgUtil.addhour(mdate, mtime, PgLOG.PGLOG['GMTZ'])
      if opt&1:
         info['date_modified'] = mdate
         info['time_modified'] = mtime
      if opt&16: info['week_day'] = PgUtil.get_weekday(mdate)

   if opt&10:
      ms = re.match(r'^(\w+)/(\w+)', items[1])
      if ms:
         if opt&2: info['logname'] = ms.group(1)
         if opt&8: info['group'] = ms.group(2)

   return info

#
# check and get a file status information on ftp server
#
# file: File name to be checked
# name: login user name
# pswd: login password
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#       1 - get date/time modified (date_modified, time_modfied)
#       2 - get file owner's login name (logname)
#       4 - get permission mode in 3 octal digits (mode)
#       8 - get group name (group)
#      16 - get week day 0-Sunday, 1-Monday (week_day)
#
# Return a dict of file info, or None if file not exists
#
def check_ftp_file(file, opt = 0, name = None, pswd = None, logact = 0):

   if not file: return None

   ms = re.match(r'^(.+)/$', file)
   if ms: file = ms.group(1)     # remove ending '/' in case
   cmd = "ncftpls -l "
   if name: cmd += "-u {} ".format(name)
   if pswd: cmd += "-p {} ".format(pswd)
   fname = op.basename(file)
   

   for loop in range(2):
      buf = PgLOG.pgsystem(cmd + file, PgLOG.LOGWRN, CMDRET)
      if buf: break
      if PgLOG.PGLOG['SYSERR']:
         errlog(PgLOG.PGLOG['SYSERR'], 'O', loop, logact|PgLOG.LOGERR)         
      if loop == 0: file = op.dirname(file) + '/'

   if loop > 1: return PgLOG.FAILURE
   for line in re.split(r'\n', buf):
      if not line or line.find(fname) < 0: continue
      info = ftp_file_stat(line, opt)
      if info: return info

   return None

#
# local function to get stat of a file on ftp server
#
def ftp_file_stat(line, opt):

   items = re.split(r'\s+', line)
   if len(items) < 9: return None
   ms = re.match(r'^([d\-])([\w\-]{9})$', items[0])
   info = {}
   info['isfile'] = (1 if ms and ms.group(1) == "-" else 0)
   info['data_size'] = int(items[4])
   info['fname'] = op.basename(items[8])
   if not opt: return info
   if opt&4: info['mode'] = get_file_mode(ms.group(2))
   if opt&17:
      dy = int(items[6])
      mn = PgUtil.get_month(items[5])
      if re.match(r'^\d+$', items[7]):
         yr = int(items[7])
         mtime = "00:00:00"
      else:
         mtime = items[7] + ":00"
         cdate = PgUtil.curdate()
         ms = re.match(r'^(\d+)-(\d\d)', cdate)
         if ms:
            yr = int(ms.group(1))
            cm = int(ms.group(2))   # current month
            if cm < mn: yr -= 1     # previous year
   
      mdate = "{}-{:02}-{:02}".format(yr, mn, dy)
      if opt&1:
         info['date_modified'] = mdate
         info['time_modified'] = mtime
      if opt&16: info['week_day'] = PgUtil.get_weekday(mdate)

   if opt&2: info['logname'] = items[2]
   if opt&8: info['group'] = items[3]

   return info

#
# get an array of directories/files under given dir on a given host name (including local host)
#
#  dir: directory name to be listed
# host: host name the directory on, default to LHOST
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#        1 - get date/time modified (date_modified, time_modfied)
#        2 - get file owner's login name (logname)
#        4 - get permission mode in 3 octal digits (mode)
#        8 - get group name (group)
#       16 - get week day 0-Sunday, 1-Monday (week_day)
#       32 - get checksum (checksum), work for local file only
#
# Return: a dict with filenames as keys None if empty directory
#
def rda_glob(dir, host, opt = 0, logact = 0):

   shost = strip_host_name(host)
   if PgUtil.pgcmp(shost, LHOST, 1) == 0:
      return local_glob(dir, opt, logact)
   elif PgUtil.pgcmp(shost, OHOST, 1) == 0:
      return object_glob(dir, None, opt, logact)      
   elif PgUtil.pgcmp(shost, BHOST, 1) == 0:
      return backup_glob(dir, None, opt, logact)
   else:
      return remote_glob(dir, host, opt, logact)

#
# get an array of directories/files under given dir on local host
#
#  dir: directory name to be listed
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#       1 - get date/time modified (date_modified, time_modfied)
#       2 - get file owner's login name (logname)
#       4 - get permission mode in 3 octal digits (mode)
#       8 - get group name (group)
#      16 - get week day 0-Sunday, 1-Monday (week_day)
#      32 - get checksum (checksum), work for local file only
#
#     256 - get files only and ignore directories
#
# Return: dict with filenames as keys or None if empty directory
#

def local_glob(dir, opt = 0, logact = 0):

   flist = {}
   if not re.search(r'[*?]', dir):
      if op.exists(dir):
         dir = PgLOG.join_paths(dir, "*")
      else:
         dir += "*"

   for file in glob.glob(dir):
      info = check_local_file(file, opt, logact)
      if info and (info['isfile'] or not 256&opt): flist[file] = info

   return flist

#
# check and get file status information of a file on remote host
#
#  dir: remote directory name
# host: host name the directory on, default to LHOST
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#       1 - get date/time modified (date_modified, time_modfied)
#       2 - file owner's login name (logname), assumed 'rdadata'
#       4 - get permission mode in 3 octal digits (mode)
#       8 - get group name (group), assumed 'dss'
#      16 - get week day 0-Sunday, 1-Monday (week_day)
#
# Return: dict with filenames as keys or None if empty directory
#
def remote_glob(dir, host, opt = 0, logact = 0):

   flist = {}
   if not re.search(r'/$', dir): dir += '/'
   buf = PgLOG.pgsystem(PgLOG.get_sync_command(host) + " dir", PgLOG.LOGWRN, CMDRET)
   if not buf:
      if PgLOG.PGLOG['SYSERR'] and PgLOG.PGLOG['SYSERR'].find(PgLOG.PGLOG['MISSFILE']) < 0:
         errlog("{}-{}: Error list directory\n{}".format(host, dir, PgLOG.PGLOG['SYSERR']), 'R', 1, logact)   
      return flist

   for line in re.split(r'\n', buf):
      info = remote_file_stat(line, opt)
      if info: flist[dir + info['fname']] = info

   return flist

#
# check and get muiltiple object store file status information
#
#  dir: object directory name
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#       1 - get date/time modified (date_modified, time_modfied)
#       2 - get file owner's login name (logname)
#       8 - get group name (group)
#      16 - get week day 0-Sunday, 1-Monday (week_day)
#
# Return: a dict with filenames as keys, or None if not exists
#
def object_glob(dir, bucket = None, opt = 0, logact = 0):

   flist = {}
   if not bucket: bucket = PgLOG.PGLOG['OBJCTBKT']
   ms = re.match(r'^(.+)/$', dir)
   if ms: dir = ms.group(1)
   cmd = "{} lo {} -b {}".format(OBJCTCMD, dir, bucket)
   ary = err = None
   buf = PgLOG.pgsystem(cmd, PgLOG.LOGWRN, CMDRET)
   if buf:
      if re.match(r'^\[\{', buf):
         ary = json.loads(buf)
      elif not re.match(r'^\[\]', buf):
         err = "{}\n{}".format(PgLOG.PGLOG['SYSERR'], buf)
   else:
      err = PgLOG.PGLOG['SYSERR']
   if not ary:
      if err:
         errlog("{}-{}-{}: Error list files\n{}".format(OHOST, bucket, dir, err), 'O', 1, logact)
         return PgLOG.FAILURE
      else:
         return flist

   for hash in ary:
      uhash = None
      if opt&10:
         ucmd = "{} gm -l {} -b {}".format(OBJCTCMD, hash['Key'], bucket)
         ubuf = PgLOG.pgsystem(ucmd, PgLOG.LOGWRN, CMDRET)
         if ubuf and re.match(r'^\{.+', ubuf): uhash = json.loads(ubuf)
      info = object_file_stat(hash, uhash, opt)
      if info: flist[hash['Key']] = info

   return flist

#
# check and get muiltiple Quasar backup file status information
#
#  dir: backup path
#  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
#       1 - get date/time modified (date_modified, time_modfied)
#       2 - get file owner's login name (logname)
#       8 - get group name (group)
#      16 - get week day 0-Sunday, 1-Monday (week_day)
#      64 - rechecking
#
# Return: a dict with filenames as keys, or None if not exists
#
def backup_glob(dir, endpoint = None, opt = 0, logact = 0):

   if not dir: return None
   if not endpoint: endpoint = PgLOG.PGLOG['BACKUPEP']
   cmd = "{} -ls -ep {} -p {}".format(BACKCMD, endpoint, dir)
   flist = {}
   for loop in range(2):
      buf = PgLOG.pgsystem(cmd, logact, CMDRET)
      syserr = PgLOG.PGLOG['SYSERR']
      if buf:
         getstat = 0
         for line in re.split(r'\n', buf):
            if re.match(r'^(User|-+)\s*\|', line):
               getstat += 1
            elif getstat > 1:
               info = backup_file_stat(line, opt)
               if info: flist[info['fname']] = info
         if flist: break
         if loop or opt&64 == 0: return None
         time.sleep(PgSIG.PGSIG['ETIME'])
      elif syserr:
         if syserr.find("Directory '{}' not found on endpoint".format(dir)) > -1:
            if loop or opt&64 == 0: return None
            time.sleep(PgSIG.PGSIG['ETIME'])
         else:
            if opt&64 == 0: return PgLOG.FAILURE
            errmsg = "Error Execute: {}\n{}".format(cmd, syserr)
            (hstat, msg) = host_down_status('', QHOSTS[endpoint], 0, logact)
            if hstat: errmsg += "\n" + msg
            errlog(errmsg, 'B', loop, logact)

   if flist:
      ECNTS['B'] = 0   # reset error count
      return flist
   else:
      return PgLOG.FAILURE

#
# local function to get file/directory mode for given permission string, for example, rw-rw-r--
#
def get_file_mode(perm):
   
   mbits = [4, 2, 1]
   mults = [64, 8, 1]
   plen = len(perm)
   if plen == 4:
      perm = perm[1:]
      plen = 3
   mode = 0
   for i in range(3):
      for j in range(3):
         pidx = 3*i+j
         if pidx < plen and perm[pidx] != "-": mode += mults[i]*mbits[j]

   return mode

#
# Evaluate md5 checksum
#
#  file: file name for MD5 checksum
# count: defined if filename is a array 
#
# Return: one or a array of 128-bits md5 'fingerprint' None if failed
#
def get_md5sum(file, count = 0, logact = 0):

   cmd = MD5CMD + ' '

   if count > 0:
      checksum = [None]*count
      for i in range(count):
         chksm = PgLOG.pgsystem(cmd + file[i], logact, 20)
         if chksm:
            ms = re.search(r'(\w{32})', chksm)
            if ms: checksum[i] = ms.group(1)
   else:
      chksm = PgLOG.pgsystem(cmd + file, logact, 20)
      checksum = None
      if chksm:
         ms = re.search(r'(\w{32})', chksm)
         if ms: checksum = ms.group(1)

   return checksum

#
# Evaluate md5 checksums and compare them for two given files
#
#  file1, file2: file names
#
# Return: 0 if same and 1 if not
#
def compare_md5sum(file1, file2, logact = 0):

   if op.isdir(file1) or op.isdir(file2):
      files1 = get_directory_files(file1)
      fcnt1 = len(files1) if files1 else 0
      files2 = get_directory_files(file2)
      fcnt2 = len(files2) if files2 else 0
      if fcnt1 != fcnt2: return 1
      chksm1 = get_md5sum(files1, fcnt1, logact)
      chksm1 = ''.join(chksm1)
      chksm2 = get_md5sum(files1, fcnt2, logact)
      chksm2 = ''.join(chksm2)
   else:
      chksm1 = get_md5sum(file1, 0, logact)
      chksm2 = get_md5sum(file2, 0, logact)

   return (0 if (chksm1 and chksm2 and chksm1 == chksm2) else 1)

#
#  change local directory to todir, and return odir upon success
#
def change_local_directory(todir, logact = 0):

   if logact:
      lact = logact&~(PgLOG.EXITLG|PgLOG.ERRLOG)
   else:
      logact = lact = PgLOG.LOGWRN
   if not op.isdir(todir):
      if op.isfile(todir): return errlog(todir + ": is file, cannot change directory", 'L', 1, logact)
      if not make_local_directory(todir, logact): return PgLOG.FAILURE 

   odir = PgLOG.PGLOG['CURDIR']
   if todir == odir:
      PgLOG.pglog(todir + ": in Directory", lact)
      return odir
   try:
      os.chdir(todir)
   except Exception as e:
      return errlog(str(e), 'L', 1, logact)
   else:
      if not op.isabs(todir): todir = os.getcwd()
      PgLOG.PGLOG['CURDIR'] = todir
      PgLOG.pglog(todir + ": Change to Directory", lact)

   return odir

#
# record the directory for the deleted file
# pass in empty dir to turn the recording delete directory on 
#
def record_delete_directory(dir, val):

   global DIRLVLS

   if dir is None:
      if isinstance(val, int):
         DIRLVLS = val
      elif re.match(r'^\d+$'):
         DIRLVLS = int(val)
   elif dir and not re.match(r'^(\.|\./|/)$', dir) and dir not in DELDIRS:
      DELDIRS[dir] = val

#
# remove the recorded delete directory if it is empty
#
def clean_delete_directory(logact = 0):

   global DIRLVLS, DELDIRS

   if not DIRLVLS: return
   if logact:
      lact = logact&~(PgLOG.EXITLG)
   else:
      logact = lact = PgLOG.LOGWRN
   lvl = DIRLVLS
   DIRLVLS = 0     # set to 0 to stop recording directory
   while lvl > 0:
      lvl -= 1
      dirs = {}
      for dir in DELDIRS:
         host = DELDIRS[dir]
         dinfo = (dir if host == LHOST else  "{}-{}".format(host, dir))
         dstat = rda_empty_directory(dir, DELDIRS[dir])
         if dstat == 0:
            if delete_rda_file(dir, host, logact):
               PgLOG.pglog(dinfo + ": Empty directory removed", lact)
         elif dstat > 0:
            if dstat == 1 and lvl > 0: PgLOG.pglog(dinfo + ": Directory not empty yet", lact)
            continue
      
         if lvl: dirs[op.dirname(dir)] = host
   
      if not dirs: break
      DELDIRS = dirs

   DELDIRS = {}   # empty cache afterward

#
# remove the empty given directory and its all subdirectories
#
# return 1 if empty dirctory removed 0 otherwise
#
def clean_empty_directory(dir, host, logact = 0):

   if not dir: return 0

   dirs = rda_glob(dir, host)
   cnt = 0
   if logact:
      lact = logact&~PgLOG.EXITLG
   else:
      lact = logact = PgLOG.LOGWRN

   if dirs:
      for name in dirs:
         cnt += 1
         if dirs[name]['isfile']: continue
         cnt -= clean_empty_directory(name, host, logact)
   
   dinfo = (dir if same_hosts(host, LHOST) else "{}-{}".format(host, dir))
   if cnt == 0:
      if delete_rda_file(dir, host, logact):
         PgLOG.pglog(dinfo + ": Empty directory removed", lact)
         return 1
   else:
       PgLOG.pglog(dinfo + ": Directory not empty yet", lact)

   return 0


#
# check if given directory is empty
#
# Return: 0 if empty directory, 1 if not empty and -1 if invalid directory
#
def rda_empty_directory(dir, host):

   shost = strip_host_name(host)

   if PgUtil.pgcmp(shost, LHOST, 1) == 0:
      return local_empty_directory(dir)
   else:
      return remote_empty_directory(dir, host)

#
# return 0 if empty local directory, 1 if not; -1 if cannot remove
#
def local_empty_directory(dir):

   if not op.isdir(dir): return -1
   if is_root_directory(dir, 'L'): return 2
   if not re.search(r'/$', dir): dir += '/'
   dir += '*'
   return (1 if glob.glob(dir) else 0)

#
# return 0 if empty remote directory, 1 if not; -1 if cannot remove
#
def remote_empty_directory(dir, host):

   if is_root_directory(dir, 'R', host): return 2
   if not re.search(r'/$', dir): dir += '/'
   buf = PgLOG.pgsystem("{} {}".format(PgLOG.get_sync_command(host), dir), PgLOG.LOGWRN, CMDRET)
   if not buf: return -1

   for line in re.split(r'\n', buf):
      if remote_file_stat(line, 0): return 1

   return 0

#
# get sizes of files on a given host
#
# files: file names to get sizes
# host: host name the file on, default to LHOST
#
# return: array of file sizes size is -1 if file does not exist
#
def rda_file_sizes(files, host, logact = 0):

   sizes = []
   for file in files: sizes.append(rda_file_size(file, host, 2, logact))

   return sizes

#
# get sizes of local files
#
# files: file names to get sizes
#
# return: array of file sizes size is -1 if file does not exist
#
def local_file_sizes(files, logact = 0):

   sizes = []
   for file in files: sizes.append(local_file_size(file, 6, logact))

   return sizes

#
# check if a file on a given host is empty or too small to be considered valid
#
# file: file name to be checked
# host: host name the file on, default to LHOST
#  opt: 1 - to remove empty file
#       2 - show message for empty file
#       4 - show message for non-existing file
#
# return: file size in unit of byte
#         0 - empty file or small file, with size < PgLOG.PGLOG['MINSIZE']
#        -1 - file not exists
#        -2 - error check file
#
def rda_file_size(file, host, opt = 0, logact = 0):

   info = check_rda_file(file, host, 0, logact)
   if info:
      if info['isfile'] and info['data_size'] < PgLOG.PGLOG['MINSIZE']:
         if opt:
            if opt&2: errlog("{}-{}: {} file".format(host, file, ("Too small({}B)".format(info['data_size']) if info['data_size'] > 0 else "Empty")),
                             'O', 1, logact)
            if opt&1: delete_rda_file(file, host, logact)
         return 0
      else:
         return info['data_size']  # if not regular file or not empty
   
   elif info != None:
      return -2   # error access
   else:
      if opt&4: errlog("{}-{}: {}".format(host, file, PgLOG.PGLOG['MISSFILE']), 'O', 1, logact)
      return -1   # file not exist

#
# check if a local file is empty or too small to be considered valid
#
# file: file name to be checked
#  opt: 1 - to remove empty file
#       2 - show message for empty file
#       4 - show message for non-existing file
#
# return: file size in unit of byte
#         0 - empty file or small file, with size < PgLOG.PGLOG['MINSIZE']
#        -1 - file not exists
#        -2 - error check file
#
def local_file_size(file, opt = 0, logact = 0):

   if not op.isfile(file):
      if opt&4: lmsg(file, PgLOG.PGLOG['MISSFILE'], logact)
      return -1   # file not eixsts
      
   info = check_local_file(file, 0, logact)
   if info:
      if info['isfile'] and info['data_size'] < PgLOG.PGLOG['MINSIZE']:
         if opt:
            if opt&2: lmsg(file, ("Too small({}B)".format(info['data_size']) if info['data_size'] > 0 else "Empty file") , logact)
            if opt&1: delete_local_file(file, logact)
         return 0
      else:
         return info['data_size']  # if not regular file or not empty
   elif info != None:
      return -2   # error check file

#
# compress/uncompress a single local file
#
# ifile: file name to be compressed/uncompressed
#   fmt: archive format
#   act: 0 - uncompress
#        1 - compress
#        2 - get uncompress file name
#        3 - get compress file name
# return: array of new file name and archive format if changed otherwise original one
#
def compress_local_file(ifile, fmt = None, act = 0, logact = 0):

   ms = re.match(r'^(.+)\.({})'.format(CMPSTR), ifile)
   if ms:
      ofile = ms.group(1)
   else:
      ofile = ifile

   if fmt:
      if act&1:
         for ext in PGCMPS:
            if re.search(r'(^|\.)({})(\.|$)'.format(ext), fmt, re.I):
               ofile += '.' + ext
               break   
      else:
         ms = re.search(r'(^|\.)({})$'.format(CMPSTR), fmt, re.I)
         if ms: fmt = re.sub(r'{}{}$'.format(ms.group(1), ms.group(2)), '', fmt, 1)

   if act < 2 and ifile != ofile: convert_files(ofile, ifile, 0, logact)
   
   return (ofile, fmt)

#
# get file archive format from a givn file name; None if not found
#
def get_file_format(fname):

   ms = re.search(r'\.({})$'.format(TARSTR), fname, re.I)
   if ms: return PGTARS[ms.group(1)][2]

   ms = re.search(r'\.({})$'.format(CMPSTR), fname, re.I)
   if ms: return PGCMPS[ms.group(1)][2]

   return None
   
#
# tar/untar mutliple local file into/from a single tar/tar.gz/tgz/zip file
#
# tfile: tar file name to be tar/untarred
# files: member file names in the tar file
#   fmt: archive format (defaults to tar file name extension must be defined in PGTARS
#   act: 0 - untar
#        1 - tar
# return: PgLOG.SUCCESS upon successful PgLOG.FAILURE otherwise
#
def tar_local_file(tfile, files, fmt, act, logact = 0):

   if not fmt:
      ms = re.search(r'\.({})$'.format(TARSTR), tfile, re.I)
      if ms: fmt = ms.group(1)
   logact |= PgLOG.ERRLOG

   if not fmt: return PgLOG.pglog(tfile + ": Miss archive format", logact)
   if fmt not in PGTARS: return PgLOG.pglog(tfile + ": unknown format fmt provided", logact)
   tarray = PGTARS[fmt]
   
   if not act:  #untar member files
      cmd = "{} {}".format(tarray[1], tfile)
      if files: cmd += ' ' + ' '.join(files)
   else:
      if not files: return PgLOG.pglog(tfile + ": Miss member file to archive", logact)
      cmd = "{} {} {}".format(tarray[0], tfile, ' '.join(files))

   return PgLOG.pgsystem(cmd, logact, 7)

#
# get local file archive format by checking extension of given local file name
#
# file: local file name
#
def local_archive_format(file):

   ms = re.search(r'\.({})$'.format(CMPSTR), file)
   if ms:
      fmt = ms.group(1)
      if re.search(r'\.tar\.{}$'.format(fmt), file):
         return "TAR." + fmt.upper()
      else:
         return fmt.upper()
   elif re.search(r'\.tar$', file):
      return "TAR"

   return ''

#
# local function to show message with full local file path
#
def lmsg(file, msg, logact = 0):

   if not op.isabs(file): file = PgLOG.join_paths(os.getcwd(), file)

   return errlog("{}: {}".format(file, msg), 'L', 1, logact)

#
# check if given path is executable locally
#
# return PgLOG.SUCCESS if yes PgLOG.FAILURE if not
#
def check_local_executable(path, actstr = '', logact = 0):

   if os.access(path, os.W_OK): return PgLOG.SUCCESS
   if check_local_accessible(path, actstr, logact):
      if actstr: actstr += '-'
      errlog("{}{}: Accessible, but Unexecutable on'{}'".format(actstr, path, PgLOG.PGLOG['HOSTNAME']), 'L', 1, logact)

   return PgLOG.FAILURE


#
# check if given path is writable locally
#
# return PgLOG.SUCCESS if yes PgLOG.FAILURE if not
#
def check_local_writable(path, actstr = '', logact = 0):

   if os.access(path, os.W_OK): return PgLOG.SUCCESS
   if check_local_accessible(path, actstr, logact):
      if actstr: actstr += '-'
      errlog("{}{}: Accessible, but Unwritable on'{}'".format(actstr, path, PgLOG.PGLOG['HOSTNAME']), 'L', 1, logact)

   return PgLOG.FAILURE

#
# check if given path is accessible locally
#
# return PgLOG.SUCCESS if yes, PgLOG.FAILURE if not
#
def check_local_accessible(path, actstr = '', logact = 0):

   if os.access(path, os.F_OK): return PgLOG.SUCCESS
   if actstr: actstr += '-'
   errlog("{}{}: Unaccessible on '{}'".format(actstr, path, PgLOG.PGLOG['HOSTNAME']), 'L', 1, logact)
   return PgLOG.FAILURE

#
# check if given webfile under PgLOG.PGLOG['DSSDATA'] is writable
#
# return PgLOG.SUCCESS if yes PgLOG.FAILURE if not
#
def check_webfile_writable(action, wfile, logact = 0):
   
   ms = re.match(r'^({}/\w+)'.format(PgLOG.PGLOG['DSSDATA']), wfile)
   if ms:
      return check_local_writable(ms.group(1), "{} {}".format(action, wfile), logact)
   else:
      return PgLOG.SUCCESS    # do not need check

#
# convert the one file to another via uncompress, move/copy, and/or compress
#
def convert_files(ofile, ifile, keep = 0, logact = 0):

   if ofile == ifile: return PgLOG.SUCCESS
   oname = ofile
   iname = ifile

   if keep: kfile = ifile + ".keep"

   oext = iext = None
   for ext in PGCMPS:
      if oext is None:
          ms = re.match(r'^(.+)\.{}$'.format(ext), ofile)
          if ms:
            oname = ms.group(1)
            oext = ext
      if iext is None:
          ms = re.match(r'^(.+)\.{}$'.format(ext), ifile)
          if ms:
            iname = ms.group(1)
            iext = ext
   
   if iext and oext and oext == iext:
      oext = iext = None
      iname = ifile
      oname = ofile

   if iext:  # uncompress
      if keep:
         if iext == 'zip':
            kfile = ifile
         else:
            local_copy_local(kfile, ifile, logact)
 
      if PgLOG.pgsystem("{} {}".format(PGCMPS[iext][1], ifile), logact, 5):
         if iext == "zip":
            path = op.dirname(iname)
            if path and path != '.': move_local_file(iname, op.basename(iname), logact)
            if not keep: delete_local_file(ifile, logact)

   if oname != iname:   # move/copy
      path = op.dirname(oname)
      if path and not op.exists(path): make_local_directory(path, logact)
      if keep and not op.exists(kfile):
         local_copy_local(oname, iname, logact)
         kfile = iname            
      else:
         move_local_file(oname, iname, logact)

   if oext: # compress
      if keep and not op.exists(kfile):
         if oext == "zip":
            kfile = oname
         else:
            local_copy_local(kfile, oname, logact)
   
      if oext == "zip":
         path = op.dirname(oname)
         if path:
            if path != '.': path = change_local_directory(path, logact)
            bname = op.basename(oname)
            PgLOG.pgsystem("{} {}.zip {}".format(PGCMPS[oext][0], bname, bname), logact, 5)
            if path != '.': change_local_directory(path, logact)
         else:
            PgLOG.pgsystem("{} {} {}".format(PGCMPS[oext][0], ofile, oname), logact, 5)

         if not keep and op.exists(ofile): delete_local_file(oname, logact)
      else:
         PgLOG.pgsystem("{} {}".format(PGCMPS[oext][0], oname), logact, 5)

   if keep and op.exists(kfile) and kfile != ifile:
      if op.exist(ifile):
         delete_local_file(kfile, logact)
      else:
         move_local_file(ifile, kfile, logact)

   if op.exists(ofile):
      return PgLOG.SUCCESS
   else:
      return errlog("{}: ERROR convert from {}".format(ofile, ifile), 'L', 1, logact)

#
#  comapre two files from given two hash references to the file information
#  return 0 if same, 1  different, -1 if can not compare
#
def compare_file_info(ainfo, binfo):
   
   if not (ainfo and binfo): return -1   # at least one is missing

   return (0 if (ainfo['data_size'] == binfo['data_size'] and
                 ainfo['date_modified'] == binfo['date_modified'] and
                 ainfo['time_modified'] == binfo['time_modified']) else 1)

#
# get local_dirname
#
def get_local_dirname(file):
   
   dir = op.dirname(file)
   if dir == '.': dir = os.getcwd()

   return dir

#
# collect valid file names under a given directory, current directory if empty
#
def get_directory_files(dir = None, limit = 0, level = 0):

   files = []
   if dir:
      if level == 0 and op.isfile(dir):
         files.append(dir)
         return files
      dir += "/*"
   else:
      dir = "*"

   for file in glob.glob(dir):
      if op.isdir(file):
         if limit == 0 or (limit-level) > 0:
            fs = get_directory_files(file, limit, level+1)
            if fs: files.extend(fs)
      else:
         files.append(file)
   
   return files if files else None

#
# reads a local file into a string and returns it
#
def read_local_file(file, logact = 0):
 
   try:
      fd = open(file, 'r')
   except Exception as e:
      return errlog("{}: {}".format(file, str(e)), 'L', 1, logact)
   else:
      fstr = fd.read()
      fd.close()

   return fstr

#
# open a local file and return the file handler
#
def open_local_file(file, mode = 'r', logact = PgLOG.LOGERR):
 
   try:
      fd = open(file, mode)
   except Exception as e:
      return errlog("{}: {}".format(file, str(e)), 'L', 1, logact)

   return fd

#
# change absolute paths to relative paths
#
def get_relative_paths(files, cdir, logact = 0):

   cnt = len(files)
   if cnt == 0: return files
   if not cdir: cdir = os.getcwd()

   for i in range(cnt):
      afile = files[i]
      if op.isabs(afile):
         files[i] = PgLOG.join_paths(afile, cdir, 1)
      else:
         PgLOG.pglog("{}: is not under the working directory '{}'".format(afile, cdir), logact)

   return files

#
# check if the action to path is blocked
#
def check_block_path(path, act = '', logact = 0):

   blockpath = PgLOG.PGLOG['USRHOME']
   if not act: act = 'Copy'

   if re.match(r'^{}'.format(blockpath), path):
      return PgLOG.pglog("{}: {} to {} is blocked".format(path, act, blockpath), logact)
   else:
      return 1

#
# join two filenames by uing the common prefix/suffix and keeping the different main bodies,
# the bodies are seprated by sep replace fext with text if provided
#
def join_filenames(name1, name2, sep = '-', fext = None, text = None):

   if fext:
      name1 = remove_file_extention(name1, fext)   
      name2 = remove_file_extention(name2, fext)   

   if name1 == name2:
      fname = name1
   else:
      fname = suffix = ''
      cnt1 = len(name1)
      cnt2 = len(name2)
      cnt = (cnt1 if cnt1 < cnt2 else cnt2)

      # get common prefix
      for pcnt in range(cnt):
         if name1[pcnt] != name2[pcnt]: break
      
      # get common suffix
      cnt -= pcnt
      for scnt in range(0, cnt):
         if name1[cnt1-scnt-1] != name2[cnt2-scnt-1]: break
      
      body1 = name1[pcnt:(cnt1-scnt)]
      body2 = name2[pcnt:(cnt2-scnt)]
      if scnt > 0:
         suffix = name2[(cnt1-scnt):cnt1]
         if name1[cnt1-scnt-1].isnumeric():
           ms = re.match(r'^([\d\.-]*\d)', suffix)
           if ms: body1 += ms.group(1)   # include trailing digit chrs to body1
      if pcnt > 0:
         fname = name1[0:pcnt]
         if name2[pcnt].isnumeric():
           ms = re.search(r'(\d[\d\.-]*)$', fname)
           if ms: body2 = ms.group(1) + body2  # include leading digit chrs to body2

      fname += body1 + sep + body2      
      if suffix: fname += suffix

   if text: fname += "." + text

   return fname

# remove given file extention if provided 
# otherwise try to remove predfined compression extention in PGCMPS
def remove_file_extention(fname, fext):

   if not fname: return ''

   if fext:
      fname = re.sub(r'\.{}$'.format(fext), '', fname, 1, re.I)
   else:
      for fext in PGCMPS:
         mp = r'\.{}$'.format(fext)
         if re.search(mp, fname):
            fname = re.sub(mp, '', fname, 1, re.I)
            break

   return fname

# check if a previous down storage system is up now for given dflag
#
# return error message if failed checking, and None otherwise
#
def check_storage_down(dflag, dpath, dscheck, logact = 0):

   if dflag not in DHOSTS:
      if logact: PgLOG.pglog(dflag + ": Unknown Down Flag for Storage Systems", logact)
      return None
   dhost = DHOSTS[dflag]
   if not dpath and dflag in DPATHS: dpath = DPATHS[dflag]
   for loop in range(2):
      (stat, msg) = host_down_status(dpath, dhost, 1, logact)
      if stat < 0: break    # stop retry for planned down

   if not dscheck and PgLOG.PGLOG['DSCHECK']: dscheck = PgLOG.PGLOG['DSCHECK']
   if dscheck:
      didx = dscheck['dflags'].find(dflag)
      if msg:
         if didx < 0: dscheck['dflags'] += dflag
      else:
         if didx > -1: dscheck['dflags'].replace(dflag, '', 1)
   
   return msg

#
# check if previous down storage systems recorded in the dflags
#
# return an array of strings for storage systems that are still down,
#        and empty array if all up
#
def check_storage_dflags(dflags, dscheck = None, logact = 0):

   if not dflags: return 0

   isdict = isinstance(dflags, dict)
   msgary = []
   for dflag in dflags:
      msg = check_storage_down(dflag, dflags[dflag] if isdict else None, dscheck, logact)
      if msg: msgary.append(msg)

   if not msgary:
      if not dscheck and PgLOG.PGLOG['DSCHECK']: dscheck = PgLOG.PGLOG['DSCHECK']
      cidx = dscheck['cindex'] if dscheck else 0
      # clean dflags if the down storage systems are all up
      if cidx: PgDBI.pgexec("UPDATE dscheck SET dflags = '' WHERE cindex = {}".format(cidx), logact)

   return msgary
