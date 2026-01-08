###############################################################################
#     Title: pg_file.py
#    Author: Zaihua Ji,  zji@ucar.edu
#      Date: 08/05/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-12-01 convert to class PgFile
#   Purpose: python library module to copy, move and delete data files locally
#             and remotely
#    Github: https://github.com/NCAR/rda-python-common.git
###############################################################################
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
from .pg_util import PgUtil
from .pg_sig import PgSIG

class PgFile(PgUtil, PgSIG):

   CMDBTH = (0x0033)   # return both stdout and stderr, 16 + 32 + 2 + 1
   RETBTH = (0x0030)   # return both stdout and stderr, 16 + 32
   CMDRET = (0x0110)   # return stdout and save error, 16 + 256
   CMDERR = (0x0101)   # display command and save error, 1 + 256
   CMDGLB = (0x0313)   # return stdout and save error for globus, 1+2+16+256+512

   def __init__(self):
      super().__init__()  # initialize parent class
      self.PGCMPS = {
      #  extension Compress       Uncompress       ArchiveFormat
         'Z':  ['compress -f', 'uncompress -f', 'Z'],
         'zip':  ['zip',         'unzip',         'ZIP'],
         'gz':  ['gzip',        'gunzip',        'GZ'],
         'xz':  ['xz',          'unxz',          'XZ'],
         'bz2':  ['bzip2',       'bunzip2',       'BZ2']
      }
      self.CMPSTR = '|'.join(self.PGCMPS)
      self.PGTARS = {
      #  extension   Packing      Unpacking   ArchiveFormat
         'tar': ['tar -cvf',  'tar -xvf', 'TAR'],
         'tar.Z': ['tar -Zcvf', 'tar -xvf', 'TAR.Z'],
         'zip': ['zip -v',    'unzip -v', 'ZIP'],
         'tgz': ['tar -zcvf', 'tar -xvf', 'TGZ'],
         'tar.gz': ['tar -zcvf', 'tar -xvf', 'TAR.GZ'],
         'txz': ['tar -cvJf', 'tar -xvf', 'TXZ'],
         'tar.xz': ['tar -cvJf', 'tar -xvf', 'TAR.XZ'],
         'tbz2': ['tar -cvjf', 'tar -xvf', 'TBZ2'],
         'tar.bz2': ['tar -cvjf', 'tar -xvf', 'TAR.BZ2']
      }
      self.TARSTR = '|'.join(self.PGTARS)
      self.DELDIRS = {}
      self.TASKIDS = {}   # cache unfinished 
      self.LHOST = "localhost"
      self.OHOST = self.PGLOG['OBJCTSTR']
      self.BHOST = self.PGLOG['BACKUPNM']
      self.DHOST = self.PGLOG['DRDATANM']
      self.OBJCTCMD = "isd_s3_cli"
      self.BACKCMD = "dsglobus"
      self.DIRLVLS = 0
      self.BFILES = {}  # cache backup file names and dates for each bid
      # record how many errors happen for working with HPSS, local or remote machines
      self.ECNTS = {'D': 0, 'H': 0, 'L': 0, 'R': 0, 'O': 0, 'B': 0}
      # up limits for how many continuing errors allowed
      self.ELMTS = {'D': 20, 'H': 20, 'L': 20, 'R': 20, 'O': 10, 'B': 10}
      # down storage hostnames & paths
      self.DHOSTS = {
         'G': self.PGLOG['GPFSNAME'],
         'O': self.OHOST,
         'B': self.BHOST,
         'D': self.DHOST
      }
      self.DPATHS = {
         'G': self.PGLOG['DSSDATA'],
         'O': self.PGLOG['OBJCTBKT'],
         'B': '/' + self.PGLOG['DEFDSID'],   # backup globus endpoint
         'D': '/' + self.PGLOG['DEFDSID']    # disaster recovery globus endpoint
      }
      self.QSTATS = {
         'A': 'ACTIVE',
         'I': 'INACTIVE',
         'S': 'SUCCEEDED',
         'F': 'FAILED',
      }
      self.QPOINTS = {
         'L': 'gdex-glade',
         'B': 'gdex-quasar',
         'D': 'gdex-quasar-drdata'
      }
      self.QHOSTS = {
         'gdex-glade': self.LHOST,
         'gdex-quasar': self.BHOST,
         'gdex-quasar-drdata': self.DHOST
      }
      self.ENDPOINTS = {
         'gdex-glade': "NCAR GDEX GLADE",
         'gdex-quasar': "NCAR GDEX Quasar",
         'gdex-quasar-drdata': "NCAR GDEX Quasar DRDATA"
      }

   # reset the up limit for a specified error type
   def reset_error_limit(self, etype, lmt):
      self.ELMTS[etype] = lmt

   # wrapping self.pglog() to show error and no fatal exit at the first call for retry 
   def errlog(self, msg, etype, retry = 0, logact = 0):
      bckgrnd = self.PGLOG['BCKGRND']
      logact |= self.ERRLOG
      if not retry:
         if msg and not re.search(r'\n$', msg): msg += "\n"
         msg += "[The same execution will be retried in {} Seconds]".format(self.PGSIG['ETIME'])
         self.PGLOG['BCKGRND'] = 1
         logact &= ~(self.EMEROL|self.EXITLG)
      elif self.ELMTS[etype]:
          self.ECNTS[etype] += 1
          if self.ECNTS[etype] >= self.ELMTS[etype]:
             logact |= self.EXITLG
             self.ECNTS[etype] = 0
      if self.PGLOG['DSCHECK'] and logact&self.EXITLG: self.record_dscheck_error(msg, logact)
      self.pglog(msg, logact)
      self.PGLOG['BCKGRND'] = bckgrnd
      if not retry: time.sleep(self.PGSIG['ETIME'])
      return self.FAILURE

   # Copy a file from one host (including local host) to an another host (including local host)
   # excluding copy file from remote host to remote host copying in background is permitted
   #   tofile - target file name
   # fromfile - source file name
   #   tohost - target host name, default to self.LHOST
   # fromhost - original host name, default to self.LHOST
   # Return 1 if successful 0 if failed with error message generated in self.pgsystem() cached in self.PGLOG['SYSERR'] 
   def copy_gdex_file(self, tofile, fromfile, tohost = None, fromhost = None, logact = 0):
      if tohost is None: tohost = self.LHOST
      if fromhost is None: fromhost = self.LHOST
      thost = self.strip_host_name(tohost)
      fhost = self.strip_host_name(fromhost)
      if self.pgcmp(thost, fhost, 1) == 0:
         if self.pgcmp(thost, self.LHOST, 1) == 0:
            return self.local_copy_local(tofile, fromfile, logact)
      elif self.pgcmp(fhost, self.LHOST, 1) == 0:
         if self.pgcmp(thost, self.OHOST, 1) == 0:
            return self.local_copy_object(tofile, fromfile, None, None, logact)
         elif self.pgcmp(thost, self.BHOST, 1) == 0:
            return self.local_copy_backup(tofile, fromfile, self.QPOINTS['B'], logact)
         elif self.pgcmp(thost, self.DHOST, 1) == 0:
            return self.local_copy_backup(tofile, fromfile, self.QPOINTS['D'], logact)
         else:
            return self.local_copy_remote(tofile, fromfile, tohost, logact)
      elif self.pgcmp(thost, self.LHOST, 1) == 0:
         if self.pgcmp(fhost, self.OHOST, 1) == 0:
            return self.object_copy_local(tofile, fromfile, None, logact)
         elif self.pgcmp(fhost, self.BHOST, 1) == 0:
            return self.backup_copy_local(tofile, fromfile, self.QPOINTS['B'], logact)
         elif self.pgcmp(fhost, self.DHOST, 1) == 0:
            return self.backup_copy_local(tofile, fromfile, self.QPOINTS['D'], logact)
         else:
            return self.remote_copy_local(tofile, fromfile, fromhost)
      return self.errlog("{}-{}->{}-{}: Cannot copy file".format(fhost, fromfile, thost, tofile), 'O', 1, self.LGEREX)   
   copy_rda_file = copy_gdex_file

   # Copy a file locally
   #   tofile - target file name
   # fromfile - source file name
   def local_copy_local(self, tofile, fromfile, logact = 0):
      finfo = self.check_local_file(fromfile, 0, logact)
      if not finfo:
         if finfo != None: return self.FAILURE
         return self.lmsg(fromfile, "{} to copy to {}".format(self.PGLOG['MISSFILE'], tofile), logact)
      target = tofile
      ms = re.match(r'^(.+)/$', tofile)
      if ms:
         dir = ms.group(1)
         tofile += op.basename(fromfile)
      else:
         dir = self.get_local_dirname(tofile)
      if not self.make_local_directory(dir, logact): return self.FAILURE
   
      cmd = "cp -{} {} {}".format(('f' if finfo['isfile'] else "rf"), fromfile, target)
      reset = loop = 0
      while((loop-reset) < 2):
         info = None
         self.PGLOG['ERR2STD'] = ['are the same file']
         ret = self.pgsystem(cmd, logact, self.CMDERR)
         self.PGLOG['ERR2STD'] = []
         if ret:
            info = self.check_local_file(tofile, 143, logact)   # 1+2+4+8+128
            if info:
               if not info['isfile']:
                  self.set_local_mode(tofile, 0, 0, info['mode'], info['logname'], logact)
                  return self.SUCCESS
               elif info['data_size'] == finfo['data_size']:
                  self.set_local_mode(tofile, 1, 0, info['mode'], info['logname'], logact)
                  return self.SUCCESS
            elif info != None:
               break
         if self.PGLOG['SYSERR']:
            errmsg = self.PGLOG['SYSERR']
         else:
            errmsg = "Error of '{}': Miss target file {}".format(cmd, tofile)
         self.errlog(errmsg, 'L', (loop - reset), logact)
         if loop == 0: reset = self.reset_local_info(tofile, info, logact)
         loop += 1
      return self.FAILURE

   # Copy a local file to a remote host
   #   tofile - target file name
   # fromfile - source file name
   #     host - remote host name
   def local_copy_remote(self, tofile, fromfile, host, logact = 0):
      finfo = self.check_local_file(fromfile, 0, logact)
      if not finfo:
         if finfo != None: return self.FAILURE
         return self.lmsg(fromfile, "{} to copy to {}-{}".format(self.PGLOG['MISSFILE'], host, tofile), logact)
      target = tofile
      ms = re.match(r'^(.+)/$', tofile)
      if ms:
         dir = ms.group(1)
         tofile += op.basename(fromfile)
      else:
         dir = op.dirname(tofile)
      if not self.make_remote_directory(dir, host, logact): return self.FAILURE
      cmd = self.get_sync_command(host)
      cmd += " {} {}".format(fromfile, target)
      for loop in range(2):
         if self.pgsystem(cmd, logact, self.CMDERR):
            info = self.check_remote_file(tofile, host, 0, logact)
            if info:
               if not finfo['isfile']:
                  self.set_remote_mode(tofile, 0, host, self.PGLOG['EXECMODE'])
                  return self.SUCCESS
               elif info['data_size'] == finfo['data_size']:
                  self.set_remote_mode(tofile, 1, host, self.PGLOG['FILEMODE'])
                  return self.SUCCESS         
            elif info != None:
               break
         self.errlog(self.PGLOG['SYSERR'], 'R', loop, logact)
      return self.FAILURE

   # Copy a local file to object store
   #   tofile - target file name
   # fromfile - source file name
   #   bucket - bucket name on Object store
   #     meta - reference to metadata hash
   def local_copy_object(self, tofile, fromfile, bucket = None, meta = None, logact = 0):
      if not bucket: bucket = self.PGLOG['OBJCTBKT']
      if meta is None: meta = {}
      if 'user' not in meta: meta['user'] = self.PGLOG['CURUID']
      if 'group' not in meta: meta['group'] = self.PGLOG['GDEXGRP']
      uinfo = json.dumps(meta)
      finfo = self.check_local_file(fromfile, 0, logact)
      if not finfo:
         if finfo != None: return self.FAILURE
         return self.lmsg(fromfile, "{} to copy to {}-{}".format(self.PGLOG['MISSFILE'], self.OHOST, tofile), logact)
      if not logact&self.OVRIDE:
         tinfo = self.check_object_file(tofile, bucket, 0, logact)
         if tinfo and tinfo['data_size'] > 0:
            return self.pglog("{}-{}-{}: file exists already".format(self.OHOST, bucket, tofile), logact)
      cmd = "{} ul -lf {} -b {} -k {} -md '{}'".format(self.OBJCTCMD, fromfile, bucket, tofile, uinfo)
      for loop in range(2):
         buf = self.pgsystem(cmd, logact, self.CMDBTH)
         tinfo = self.check_object_file(tofile, bucket, 0, logact)
         if tinfo:
            if tinfo['data_size'] == finfo['data_size']:
               return self.SUCCESS      
         elif tinfo != None:
            break
         self.errlog("Error Execute: {}\n{}".format(cmd, buf), 'O', loop, logact)
      return self.FAILURE

   # Copy multiple files from a Globus endpoint to another
   #   tofiles - target file name list, echo name leading with /dsnnn.n/ on Quasar and 
   #             leading with /data/ or /decsdata/ on local glade disk
   # fromfiles - source file name list, the same format as the tofiles
   #   topoint - target endpoint name, 'gdex-glade', 'gdex-quasar' or 'gdex-quasar-dgdexta' 
   # frompoint - source endpoint name, the same choices as the topoint
   def quasar_multiple_trasnfer(self, tofiles, fromfiles, topoint, frompoint, logact = 0):
      ret = self.FAILURE
      fcnt = len(fromfiles)
      transfer_files = {"files": []}
      for i in range(fcnt):
         transfer_files["files"].append({
            "source_file": fromfiles[i],
            "destination_file": tofiles[i]
         })
      qstr = json.dumps(transfer_files)
      action = 'transfer'
      source_endpoint = frompoint
      destination_endpoint = topoint
      label = f"{self.ENDPOINTS[frompoint]} to {self.ENDPOINTS[topoint]} {action}"
      verify_checksum = True
      cmd = f'{self.BACKCMD} {action} -se {source_endpoint} -de {destination_endpoint} --label "{label}"'
      if verify_checksum:
         cmd += ' -vc'   
      cmd += ' --batch -'
      task = self.submit_globus_task(cmd, topoint, logact, qstr)
      if task['stat'] == 'S':
         ret = self.SUCCESS
      elif task['stat'] == 'A':
         self.TASKIDS["{}-{}".format(topoint, tofiles[0])] = task['id']
         ret = self.FINISH
      return ret

   # Copy a file from a Globus endpoint to another
   #    tofile - target file name, leading with /dsnnn.n/ on Quasar and 
   #             leading with /data/ or /decsdata/ on local glade disk
   #  fromfile - source file, the same format as the tofile
   #   topoint - target endpoint name, 'gdex-glade', 'gdex-quasar' or 'gdex-quasar-dgdexta' 
   # frompoint - source endpoint name, the same choices as the topoint
   def endpoint_copy_endpoint(self, tofile, fromfile, topoint, frompoint, logact = 0):
      ret = self.FAILURE
      finfo = self.check_globus_file(fromfile, frompoint, 0, logact)
      if not finfo:
         if finfo != None: return ret
         return self.lmsg(fromfile, "{} to copy {} file to {}-{}".format(self.PGLOG['MISSFILE'], frompoint, topoint, tofile), logact)
      if not logact&self.OVRIDE:
         tinfo = self.check_globus_file(tofile, topoint, 0, logact)
         if tinfo and tinfo['data_size'] > 0:
            return self.pglog("{}-{}: file exists already".format(topoint, tofile), logact)
      action = 'transfer'
      cmd = f'{self.BACKCMD} {action} -se {frompoint} -de {topoint} -sf {fromfile} -df {tofile} -vc'
      task = self.submit_globus_task(cmd, topoint, logact)
      if task['stat'] == 'S':
         ret = self.SUCCESS
      elif task['stat'] == 'A':
         self.TASKIDS["{}-{}".format(topoint, tofile)] = task['id']
         ret = self.FINISH
   
      return ret

   # submit a globus task and return a task id
   def submit_globus_task(self, cmd, endpoint, logact = 0, qstr = None):
      task = {'id': None, 'stat': 'U'}
      loop = reset = 0
      while (loop-reset) < 2:
         buf = self.pgsystem(cmd, logact, self.CMDGLB, qstr)
         syserr = self.PGLOG['SYSERR']
         if buf and buf.find('a task has been created') > -1:
            ms = re.search(r'Task ID:\s+(\S+)', buf)
            if ms:
               task['id'] = ms.group(1)
               lp = 0
               while lp < 2:
                  task['stat'] = self.check_globus_status(task['id'], endpoint, logact)
                  if task['stat'] == 'S': break
                  time.sleep(self.PGSIG['ETIME'])
                  lp += 1
               if task['stat'] == 'S' or task['stat'] == 'A': break
               if task['stat'] == 'F' and not syserr: break
         errmsg = "Error Execute: " + cmd
         if qstr: errmsg += " with stdin:\n" + qstr
         if syserr:
            errmsg += "\n" + syserr
            (hstat, msg) = self.host_down_status('', self.QHOSTS[endpoint], 1, logact)
            if hstat: errmsg += "\n" + msg
         self.errlog(errmsg, 'B', (loop - reset), logact)
         if loop == 0 and syserr and syserr.find('This user has too many pending jobs') > -1: reset = 1
         loop += 1
      if task['stat'] == 'S' or task['stat'] == 'A': self.ECNTS['B'] = 0   # reset error count
      return task

   # check Globus transfer status for given taskid. Cancel the task
   # if self.NOWAIT presents and Details is neither OK nor Queued
   def check_globus_status(self, taskid, endpoint = None, logact = 0):
      ret = 'U'
      if not taskid: return ret
      if not endpoint: endpoint = self.PGLOG['BACKUPEP']
      mp = r'Status:\s+({})'.format('|'.join(self.QSTATS.values()))
      cmd = f"{self.BACKCMD} get-task {taskid}"
      astats = ['OK', 'Queued']
      for loop in range(2):
         buf = self.pgsystem(cmd, logact, self.CMDRET)
         if buf:
            ms = re.search(mp, buf)
            if ms:
               ret = ms.group(1)[0]
               if ret == 'A':
                  ms = re.search(r'Details:\s+(\S+)', buf)
                  if ms:
                     detail = ms.group(1)
                     if detail not in astats:
                        if logact&self.NOWAIT:
                           errmsg = "{}: Cancel Task due to {}:\n{}".format(taskid, detail, buf)
                           self.errlog(errmsg, 'B', 1, logact)
                           ccmd = f"{self.BACKCMD} cancel-task {taskid}"
                           self.pgsystem(ccmd, logact, 7)
                        else:
                           time.sleep(self.PGSIG['ETIME'])
                        continue
               break
         errmsg = "Error Execute: " + cmd
         if self.PGLOG['SYSERR']:
            errmsg = "\n" + self.PGLOG['SYSERR']
            (hstat, msg) = self.host_down_status('', self.QHOSTS[endpoint], 1, logact)
            if hstat: errmsg += "\n" + msg
         self.errlog(errmsg, 'B', loop, logact)
      if ret == 'S' or ret == 'A': self.ECNTS['B'] = 0   # reset error count
      return ret

   # return SUCCESS if Globus transfer is done; FAILURE otherwise
   def check_globus_finished(self, tofile, topoint, logact = 0):
      ret = self.SUCCESS
      ckey = "{}-{}".format(topoint, tofile)
      if ckey in self.TASKIDS:
         taskid = self.TASKIDS[ckey]
      else:
         self.errlog(ckey + ": Miss Task ID to check Status", 'B', 1, logact)
         return self.FAILURE
      lp = 0
      if logact&self.NOWAIT:
         act = logact&(~self.NOWAIT)
         lps = 2
      else:
         act = logact
         lps = 0
      while True:
         stat = self.check_globus_status(taskid, topoint, act)
         if stat == 'A':
            if lps:
               lp += 1
               if lp > lps: act = logact
            time.sleep(self.PGSIG['ETIME'])
         else:
            if stat == 'S':
               del self.TASKIDS[ckey]
            else:
               status = self.QSTATS[stat] if stat in self.QSTATS else 'UNKNOWN'
               self.errlog("{}: Status '{}' for Task {}".format(ckey, status, taskid), 'B', 1, logact)
               ret = self.FAILURE
            break
      return ret

   # Copy a local file to Quasar backup tape system
   #   tofile - target file name, leading with /dsnnn.n/
   # fromfile - source file name, leading with /data/ or /decsdata/
   # endpoint - endpoint name on Quasar Backup Server
   def local_copy_backup(self, tofile, fromfile, endpoint = None, logact = 0):
      if not endpoint: endpoint = self.PGLOG['BACKUPEP']
      return self.endpoint_copy_endpoint(tofile, fromfile, endpoint, 'gdex-glade', logact)

   # Copy a  Quasar backup file to local Globus endpoint
   #   tofile - target file name, leading with /data/ or /decsdata/
   # fromfile - source file name, leading with /dsnnn.n/
   # endpoint - endpoint name on Quasar Backup Server
   def backup_copy_local(self, tofile, fromfile, endpoint = None, logact = 0):
      if not endpoint: endpoint = self.PGLOG['BACKUPEP']
      return self.endpoint_copy_endpoint(tofile, fromfile, 'gdex-glade', endpoint, logact)

   # Copy a remote file to local
   #   tofile - target file name
   # fromfile - source file name
   #     host - remote host name
   def remote_copy_local(self, tofile, fromfile, host, logact = 0):
      cmd = self.get_sync_command(host)
      finfo = self.check_remote_file(fromfile, host, 0, logact)
      if not finfo:
         if finfo != None: return self.FAILURE
         return self.errlog("{}-{}: {} to copy to {}".format(host, fromfile, self.PGLOG['MISSFILE'], tofile), 'R', 1, logact)
         target = tofile
      ms = re.match(r'^(.+)/$', tofile)
      if ms:
         dir = ms.group(1)
         tofile += op.basename(fromfile)
      else:
         dir = self.get_local_dirname(tofile)
      if not self.make_local_directory(dir, logact): return self.FAILURE
      cmd += " -g {} {}".format(fromfile, target)
      loop = reset = 0
      while (loop-reset) < 2:
         if self.pgsystem(cmd, logact, self.CMDERR):
            info = self.check_local_file(tofile, 143, logact)  # 1+2+4+8+128
            if info:
               if not info['isfile']:
                   self.set_local_mode(tofile, 0, self.PGLOG['EXECMODE'])
                   return self.SUCCESS
               elif info['data_size'] == finfo['data_size']:
                   self.set_local_mode(tofile, 1, self.PGLOG['FILEMODE'])
                   return self.SUCCESS
            elif info != None:
               break
         self.errlog(self.PGLOG['SYSERR'], 'L', (loop - reset), logact)
         if loop == 0: reset = self.reset_local_info(tofile, info, logact)
         loop += 1
      return self.FAILURE

   # Copy a object file to local
   #   tofile - target file name
   # fromfile - source file name
   #   bucket - bucket name on Object store
   def object_copy_local(self, tofile, fromfile, bucket = None, logact = 0):
      ret = self.FAILURE
      if not bucket: bucket = self.PGLOG['OBJCTBKT']
      finfo = self.check_object_file(fromfile, bucket, 0, logact)
      if not finfo:
         if finfo != None: return ret
         return self.lmsg(fromfile, "{}-{} to copy to {}".format(self.OHOST, self.PGLOG['MISSFILE'], tofile), logact)
      cmd = "{} go -k {} -b {}".format(self.OBJCTCMD, fromfile, bucket)
      fromname = op.basename(fromfile)
      toname = op.basename(tofile)
      if toname == tofile:
         dir = odir = None
      else:
         dir = op.dirname(tofile)
         odir = self.change_local_directory(dir, logact)
      loop = reset = 0
      while (loop-reset) < 2:
         buf = self.pgsystem(cmd, logact, self.CMDBTH)
         info = self.check_local_file(fromname, 143, logact)   # 1+2+4+8+128
         if info:
            if info['data_size'] == finfo['data_size']:
               self.set_local_mode(fromfile, info['isfile'], 0, info['mode'], info['logname'], logact)
               if toname == fromname or self.move_local_file(toname, fromname, logact):
                  ret = self.SUCCESS
                  break
         elif info != None:
            break
         self.errlog("Error Execute: {}\n{}".format(cmd, buf), 'L', (loop - reset), logact)
         if loop == 0: reset = self.reset_local_info(tofile, info, logact)
         loop += 1
      if odir and odir != dir:
         self.change_local_directory(odir, logact)
      return ret

   # Copy a remote file to object
   #   tofile - target object file name
   # fromfile - source remote file name
   #     host - remote host name
   #   bucket - bucket name on Object store
   #     meta - reference to metadata hash
   def remote_copy_object(self, tofile, fromfile, host, bucket = None, meta = None, logact = 0):
      if self.is_local_host(host): return self.local_copy_object(tofile, fromfile, bucket, meta, logact)
      locfile = "{}/{}".format(self.PGLOG['TMPPATH'], op.basename(tofile))
      ret = self.remote_copy_local(locfile, fromfile, host, logact)
      if ret:
         ret = self.local_copy_object(tofile, locfile, bucket, meta, logact)
         self.delete_local_file(locfile, logact)
      return ret

   # Copy an object file to remote
   #   tofile - target remote file name
   # fromfile - source object file name
   #     host - remote host name
   #   bucket - bucket name on Object store
   #     meta - reference to metadata hash
   def object_copy_remote(self, tofile, fromfile, host, bucket = None, logact = 0):
      if self.is_local_host(host): return self.object_copy_local(tofile, fromfile, bucket, logact)
      locfile = "{}/{}".format(self.PGLOG['TMPPATH'], op.basename(tofile))
      ret = self.object_copy_local(locfile, fromfile, bucket, logact)
      if ret:
         ret = self.local_copy_remote(fromfile, locfile, host, logact)
         self.delete_local_file(locfile, logact)
      return ret

   # Delete a file/directory on a given host name (including local host) no background process for deleting
   # file - file name to be deleted
   # host - host name the file on, default to self.LHOST
   # Return 1 if successful 0 if failed with error message generated in self.pgsystem() cached in self.PGLOG['SYSERR'] 
   def delete_gdex_file(self, file, host, logact = 0):
      shost = self.strip_host_name(host)
      if self.pgcmp(shost, self.LHOST, 1) == 0:
         return self.delete_local_file(file, logact)
      elif self.pgcmp(shost, self.OHOST, 1) == 0:
         return self.delete_object_file(file, None, logact)      
      else:
         return self.delete_remote_file(file, host, logact)
   delete_rda_file = delete_gdex_file

   # Delete a local file/irectory
   def delete_local_file(self, file, logact = 0):
      info = self.check_local_file(file, 0, logact)
      if not info: return self.FAILURE
      cmd = "rm -rf "
      cmd += file
      loop = reset = 0
      while (loop-reset) < 2:
         if self.pgsystem(cmd, logact, self.CMDERR):
            info = self.check_local_file(file, 14, logact)
            if info is None:
               if self.DIRLVLS: self.record_delete_directory(op.dirname(file), self.LHOST)
               return self.SUCCESS
            elif not info:
               break   # error checking file
         self.errlog(self.PGLOG['SYSERR'], 'L', (loop - reset), logact)
         if loop == 0: reset = self.reset_local_info(file, info, logact)
         loop += 1
      return self.FAILURE

   # Delete file/directory on a remote host
   def delete_remote_file(self, file, host, logact = 0):
      if not self.check_remote_file(file, host, logact): return self.FAILURE
      cmd = self.get_sync_command(host)
      for loop in range(2):
         if self.pgsystem("{} -d {}".format(cmd, file), logact, self.CMDERR):
            if self.DIRLVLS: self.record_delete_directory(op.dirname(file), host)
            return self.SUCCESS
         self.errlog(self.PGLOG['SYSERR'], 'R', loop, logact)
      return self.FAILURE

   # Delete a file on object store  
   def delete_object_file(self, file, bucket = None, logact = 0):
      if not bucket: bucket = self.PGLOG['OBJCTBKT']
      for loop in range(2):
         list = self.object_glob(file, bucket, 0, logact)
         if not list: return self.FAILURE
         errmsg = None
         for key in list:
            cmd = "{} dl {} -b {}".format(self.OBJCTCMD, key, bucket)
            if not self.pgsystem(cmd, logact, self.CMDERR):
               errmsg = self.PGLOG['SYSERR']
               break
         list = self.object_glob(file, bucket, 0, logact)
         if not list: return self.SUCCESS
         if errmsg: self.errlog(errmsg, 'O', loop, logact)
      return self.FAILURE

   # Delete a backup file on Quasar Server  
   def delete_backup_file(self, file, endpoint = None, logact = 0):
      if not endpoint: endpoint = self.PGLOG['BACKUPEP']
      info = self.check_backup_file(file, endpoint, 0, logact)
      if not info: return self.FAILURE
      cmd = f"{self.BACKCMD} delete -ep {endpoint} -tf {file}"
      task = self.submit_globus_task(cmd, endpoint, logact)
      if task['stat'] == 'S':
         return self.SUCCESS
      elif task['stat'] == 'A':
         self.TASKIDS["{}-{}".format(endpoint, file)] = task['id']
         return self.FINISH
      return self.FAILURE

   # reset local file/directory information to make them writable for self.PGLOG['GDEXUSER']
   # file - file name (mandatory)
   # info - gathered file info with option 14, None means file not exists
   def reset_local_info(self, file, info = None, logact = 0):
      ret = 0
      if info:
         if info['isfile']:
            ret += self.reset_local_file(file, info, logact)
            dir = self.get_local_dirname(file)
            info = self.check_local_file(dir, 14, logact)
         else:
            dir = file
      else:
         dir = self.get_local_dirname(file)
         info = self.check_local_file(dir, 14, logact)
      if info: ret += self.reset_local_directory(dir, info, logact)
      return 1 if ret else 0

   # reset local directory group/mode
   def reset_local_directory(self, dir, info = None, logact = 0):
      ret = 0
      if not (info and 'mode' in info and 'group' in info and 'logname' in info):
         info = self.check_local_file(dir, 14, logact)
      if info:
         if info['mode'] and info['mode'] != 0o775:
            ret += self.set_local_mode(dir, 0, 0o775, info['mode'], info['logname'], logact)
         if info['group'] and self.PGLOG['GDEXGRP'] != info['group']:
            ret += self.change_local_group(dir, self.PGLOG['GDEXGRP'], info['group'], info['logname'], logact)   
      return 1 if ret else 0

   # reset local file group/mode
   def reset_local_file(self, file, info = None, logact = 0):
      ret = 0
      if not (info and 'mode' in info and 'group' in info and 'logname' in info):
         info = self.check_local_file(file, 14, logact)
      if info:
         if info['mode'] != 0o664:
            ret += self.set_local_mode(file, 1, 0o664, info['mode'], info['logname'], logact)
         if self.PGLOG['GDEXGRP'] != info['group']:
            ret += self.change_local_group(file, self.PGLOG['GDEXGRP'], info['group'], info['logname'], logact)
      return ret

   # Move file locally or remotely on the same host no background process for moving
   #   tofile - target file name
   # fromfile - original file name
   #     host - host name the file is moved on, default to self.LHOST
   # Return self.SUCCESS if successful self.FAILURE otherwise
   def move_gdex_file(self, tofile, fromfile, host, logact = 0):
      shost = self.strip_host_name(host)
      if self.pgcmp(shost, self.LHOST, 1) == 0:
         return self.move_local_file(tofile, fromfile, logact)
      elif self.pgcmp(shost, self.OHOST, 1) == 0:
         return self.move_object_file(tofile, fromfile, None, None, logact)
      else:
         return self.move_remote_file(tofile, fromfile, host, logact)
   move_rda_file = move_gdex_file

   # Move a file locally
   #   tofile - target file name
   # fromfile - source file name
   def move_local_file(self, tofile, fromfile, logact = 0):
      dir = self.get_local_dirname(tofile)
      info = self.check_local_file(fromfile, 0, logact)
      tinfo = self.check_local_file(tofile, 0, logact)
      if not info:
         if info != None: return self.FAILURE
         if tinfo:
            self.pglog("{}: Moved to {} already".format(fromfile, tofile), self.LOGWRN)
            return self.SUCCESS
         else:
            return self.errlog("{}: {} to move".format(fromfile, self.PGLOG['MISSFILE']), 'L', 1, logact)
      if tinfo:
         if tinfo['data_size'] > 0 and not logact&self.OVRIDE:
            return self.errlog("{}: File exists, cannot move {} to it".format(tofile, fromfile), 'L', 1, logact)
      elif tinfo != None:
         return self.FAILURE
      if not self.make_local_directory(dir, logact): return self.FAILURE
      cmd = "mv {} {}".format(fromfile, tofile)
      loop = reset = 0
      while (loop-reset) < 2:
         if self.pgsystem(cmd, logact, self.CMDERR):
            if self.DIRLVLS: self.record_delete_directory(op.dirname(fromfile), self.LHOST)
            return self.SUCCESS
         self.errlog(self.PGLOG['SYSERR'], 'L', (loop - reset), logact)
         if loop == 0: reset = self.reset_local_info(tofile, info, logact)
         loop += 1
      return self.FAILURE

   # Move a remote file on the same host
   #   tofile - target file name
   # fromfile - original file name
   #     host - remote host name
   #  locfile - local copy of tofile
   def move_remote_file(self, tofile, fromfile, host, logact = 0):
      if self.is_local_host(host): return self.move_local_file(tofile, fromfile, logact)
      ret = self.FAILURE
      dir = op.dirname(tofile)
      info = self.check_remote_file(fromfile, host, 0, logact)
      tinfo = self.check_remote_file(tofile, host, 0, logact)
      if not info:
         if info != None: return self.FAILURE
         if tinfo:
            self.pglog("{}-{}: Moved to {} already".format(host, fromfile, tofile), self.LOGWRN)
            return self.SUCCESS
         else:
            return self.errlog("{}-{}: {} to move".format(host, fromfile, self.PGLOG['MISSFILE']), 'R', 1, logact)   
      if tinfo:
         if tinfo['data_size'] > 0 and not logact&self.OVRIDE:
            return self.errlog("{}-{}: File exists, cannot move {} to it".format(host, tofile, fromfile), 'R', 1, logact)
      elif tinfo != None:
         return self.FAILURE
      if self.make_remote_directory(dir, host, logact):
         locfile = "{}/{}".format(self.PGLOG['TMPPATH'], op.basename(tofile))
         if self.remote_copy_local(locfile, fromfile, host, logact):
            ret = self.local_copy_remote(tofile, locfile, host, logact)
            self.delete_local_file(locfile, logact)
            if ret:
               ret = self.delete_remote_file(fromfile, host, logact)
               if self.DIRLVLS: self.record_delete_directory(op.dirname(fromfile), host)
      return ret

   # Move an object file on Object Store
   #     tofile - target file name
   #   fromfile - original file name
   #   tobucket - target bucket name
   # frombucket - original bucket name
   def move_object_file(self, tofile, fromfile, tobucket, frombucket, logact = 0):
      ret = self.FAILURE
      if not tobucket: tobucket = self.PGLOG['OBJCTBKT']
      if not frombucket: frombucket = tobucket
      finfo = self.check_object_file(fromfile, frombucket, 0, logact)
      tinfo = self.check_object_file(tofile, tobucket, 0, logact)
      if not finfo:
         if finfo != None: return self.FAILURE
         if tinfo:
            self.pglog("{}-{}: Moved to {}-{} already".format(frombucket, fromfile, tobucket, tofile), self.LOGWRN)
            return self.SUCCESS
         else:
            return self.errlog("{}-{}: {} to move".format(frombucket, fromfile, self.PGLOG['MISSFILE']), 'R', 1, logact)   
      if tinfo:
         if tinfo['data_size'] > 0 and not logact&self.OVRIDE:
            return self.errlog("{}-{}: Object File exists, cannot move {}-{} to it".format(tobucket, tofile, frombucket, fromfile), 'R', 1, logact)
      elif tinfo != None:
         return self.FAILURE
      cmd = "{} mv -b {} -db {} -k {} -dk {}".format(self.OBJCTCMD, frombucket, tobucket, fromfile, tofile)
      ucmd = "{} gm -k {} -b {}".format(self.OBJCTCMD, fromfile, frombucket)
      ubuf = self.pgsystem(ucmd, self.LOGWRN, self.CMDRET)
      if ubuf and re.match(r'^\{', ubuf): cmd += " -md '{}'".format(ubuf)
      for loop in range(2):
         buf = self.pgsystem(cmd, logact, self.CMDBTH)
         tinfo = self.check_object_file(tofile, tobucket, 0, logact)
         if tinfo:
            if tinfo['data_size'] == finfo['data_size']:
               return self.SUCCESS
         elif tinfo != None:
            break
         self.errlog("Error Execute: {}\n{}".format(cmd, buf), 'O', loop, logact)
      return self.FAILURE

   # Move an object path on Object Store and all the file keys under it
   #     topath - target path name
   #   frompath - original path name
   #   tobucket - target bucket name
   # frombucket - original bucket name
   def move_object_path(self, topath, frompath, tobucket, frombucket, logact = 0):
      ret = self.FAILURE
      if not tobucket: tobucket = self.PGLOG['OBJCTBKT']
      if not frombucket: frombucket = tobucket
      fcnt = self.check_object_path(frompath, frombucket, logact)
      tcnt = self.check_object_path(topath, tobucket, logact)
      if not fcnt:
         if fcnt == None: return self.FAILURE
         if tcnt:
            self.pglog("{}-{}: Moved to {}-{} already".format(frombucket, frompath, tobucket, topath), self.LOGWRN)
            return self.SUCCESS
         else:
            return self.errlog("{}-{}: {} to move".format(frombucket, frompath, self.PGLOG['MISSFILE']), 'R', 1, logact)   
      cmd = "{} mv -b {} -db {} -k {} -dk {}".format(self.OBJCTCMD, frombucket, tobucket, frompath, topath)
      for loop in range(2):
         buf = self.pgsystem(cmd, logact, self.CMDBTH)
         fcnt = self.check_object_path(frompath, frombucket, logact)
         if not fcnt: return self.SUCCESS
         self.errlog("Error Execute: {}\n{}".format(cmd, buf), 'O', loop, logact)
      return self.FAILURE

   # Move a backup file on Quasar Server
   #   tofile - target file name
   # fromfile - source file name
   # endpoint - Globus endpoint
   def move_backup_file(self, tofile, fromfile, endpoint = None, logact = 0):
      ret = self.FAILURE
      if not endpoint: endpoint = self.PGLOG['BACKUPEP']
      finfo = self.check_backup_file(fromfile, endpoint, 0, logact)
      tinfo = self.check_backup_file(tofile, endpoint, 0, logact)
      if not finfo:
         if finfo != None: return ret
         if tinfo:
            self.pglog("{}: Moved to {} already".format(fromfile, tofile), self.LOGWRN)
            return self.SUCCESS
         else:
            return self.errlog("{}: {} to move".format(fromfile, self.PGLOG['MISSFILE']), 'B', 1, logact)
      if tinfo:
         if tinfo['data_size'] > 0 and not logact&self.OVRIDE:
            return self.errlog("{}: File exists, cannot move {} to it".format(tofile, fromfile), 'B', 1, logact)
      elif tinfo != None:
         return ret
      cmd = f"{self.BACKCMD} rename -ep {endpoint} --old-path {fromfile} --new-path {tofile}"
      loop = 0
      while loop < 2:
         buf = self.pgsystem(cmd, logact, self.CMDRET)
         syserr = self.PGLOG['SYSERR']
         if buf:
            if buf.find('File or directory renamed successfully') > -1:
               ret = self.SUCCESS
               break
         if syserr:
            if syserr.find("No such file or directory") > -1:
               if self.make_backup_directory(op.dirname(tofile), endpoint, logact): continue
            errmsg = "Error Execute: {}\n{}".format(cmd, syserr)
            (hstat, msg) = self.host_down_status('', self.QHOSTS[endpoint], 1, logact)
            if hstat: errmsg += "\n" + msg
            self.errlog(errmsg, 'B', loop, logact)
         loop += 1
      if ret == self.SUCCESS: self.ECNTS['B'] = 0   # reset error count
      return ret

   # Make a directory on a given host name (including local host)
   #  dir - directory path to be made
   # host - host name the directory on, default to self.LHOST
   # Return self.SUCCESS(1) if successful or self.FAILURE(0) if failed
   def make_gdex_directory(self, dir, host, logact = 0):
      if not dir: return self.SUCCESS
      shost = self.strip_host_name(host)
      if self.pgcmp(shost, self.LHOST, 1) == 0:
         return self.make_local_directory(dir, logact)
      else:
         return self.make_remote_directory(dir, host, logact)
   make_rda_directory = make_gdex_directory

   # Make a local directory
   # dir - directory path to be made
   def make_local_directory(self, dir, logact = 0):
      return self.make_one_local_directory(dir, None, logact)

   # Make a local directory recursively
   def make_one_local_directory(self, dir, odir = None, logact = 0):
      if not dir or op.isdir(dir): return self.SUCCESS
      if op.isfile(dir): return self.errlog(dir + ": is file, cannot make directory", 'L', 1, logact)
      if not odir: odir = dir
      if self.is_root_directory(dir, 'L', self.LHOST, "make directory " + odir, logact): return self.FAILURE
      if not self.make_one_local_directory(op.dirname(dir), odir, logact): return self.FAILURE
      loop = reset = 0
      while (loop-reset) < 2:
         try:
            os.mkdir(dir, self.PGLOG['EXECMODE'])
         except Exception as e:
            errmsg = str(e)
            if errmsg.find('File exists') > -1: return self.SUCCESS
            self.errlog(errmsg, 'L', (loop - reset), logact)
            if loop == 0: reset = self.reset_local_info(dir, None, logact)
            loop += 1
         else:
            return self.SUCCESS
      return self.FAILURE

   # Make a directory on a remote host name
   #  dir - directory path to be made
   # host - host name the directory on
   def make_remote_directory(self, dir, host, logact = 0):
      return self.make_one_remote_directory(dir, None, host, logact)
   
   def make_one_remote_directory(self, dir, odir, host, logact = 0):
      info = self.check_remote_file(dir, host, 0, logact)
      if info:
         if info['isfile']: return self.errlog("{}-{}: is file, cannot make directory".format(host, dir), 'R', 1, logact)
         return self.SUCCESS
      elif info != None:
         return self.FAILURE
      if not odir: odir = dir
      if self.is_root_directory(dir, 'R', host, "make directory {} on {}".format(odir, host), logact): return self.FAILURE
      if self.make_one_remote_directory(op.dirname(dir), odir, host, logact):
         tmpsync = self.get_tmpsync_path()
         if self.pgsystem("{} {} {}".format(self.get_sync_command(host), tmpsync, dir), logact, 5):
            self.set_remote_mode(dir, 0, host, self.PGLOG['EXECMODE'])
            return self.SUCCESS
      return self.FAILURE

   # Make a quasar directory
   # dir - directory path to be made
   def make_backup_directory(self, dir, endpoint, logact = 0):
      return self.make_one_backup_directory(dir, None, endpoint, logact)

   # Make a quasar directory recursively
   def make_one_backup_directory(self, dir, odir, endpoint = None, logact = 0):
      if not dir or dir == '/': return self.SUCCESS
      if not endpoint: endpoint = self.PGLOG['BACKUPEP']
      info = self.check_backup_file(dir, endpoint, 0, logact)
      if info:
         if info['isfile']: return self.errlog("{}-{}: is file, cannot make backup directory".format(endpoint, dir), 'B', 1, logact)
         return self.SUCCESS
      elif info != None:
         return self.FAILURE
      if not odir: odir = dir
      if not self.make_one_backup_directory(op.dirname(dir), odir, endpoint, logact): return self.FAILURE
      cmd = f"{self.BACKCMD} mkdir -ep {endpoint} -p {dir}"
      for loop in range(2):
         buf = self.pgsystem(cmd, logact, self.CMDRET)
         syserr = self.PGLOG['SYSERR']
         if buf:
            if(buf.find('The directory was created successfully') > -1 or
               buf.find("Path '{}' already exists".format(dir)) > -1):
               ret = self.SUCCESS
               break
         if syserr:
            if syserr.find("No such file or directory") > -1:
               ret = self.make_one_backup_directory(op.dirname(dir), odir, endpoint, logact)
               if ret == self.SUCCESS or loop: break
               time.sleep(self.PGSIG['ETIME'])
            else:
               errmsg = "Error Execute: {}\n{}".format(cmd, syserr)
               (hstat, msg) = self.host_down_status('', self.QHOSTS[endpoint], 1, logact)
               if hstat: errmsg += "\n" + msg
               self.errlog(errmsg, 'B', loop, logact)
      if ret == self.SUCCESS: self.ECNTS['B'] = 0   # reset error count
      return ret

   # check and return 1 if a root directory
   def is_root_directory(self, dir, etype, host = None, action = None, logact = 0):
      ret = cnt = 0
      if re.match(r'^{}'.format(self.PGLOG['DSSDATA']), dir):
         ms = re.match(r'^({})(.*)$'.format(self.PGLOG['GPFSROOTS']), dir)
         if ms:
            m2 = ms.group(2) 
            if not m2 or m2 == '/': ret = 1 
         else:
            cnt = 4
      else:
         ms = re.match(r'^({})(.*)$'.format(self.PGLOG['HOMEROOTS']), dir)
         if ms:
            m2 = ms.group(2) 
            if not m2 or m2 == '/': ret = 1 
         else:
            cnt = 2
      if cnt and re.match(r'^(/[^/]+){0,%d}(/*)$' % cnt, dir):
         ret = 1
      if ret and action:
         cnt = 0
         errmsg = "{}: Cannot {} from {}".format(dir, action, self.PGLOG['HOSTNAME'])
         (hstat, msg) = self.host_down_status(dir, host, 0, logact)
         if hstat: errmsg += "\n" + msg
         self.errlog(errmsg, etype, 1, logact|self.ERRLOG)
      return ret

   # set mode for a given direcory/file on a given host (include local host)
   def set_gdex_mode(self, file, isfile, host, nmode = None, omode = None, logname = None, logact = 0):
      shost = self.strip_host_name(host)
      if self.pgcmp(shost, self.LHOST, 1) == 0:
         return self.set_local_mode(file, isfile, nmode, omode, logname, logact)
      else:
         return self.set_remote_mode(file, isfile, host, nmode, omode, logact)      
   set_rda_mode = set_gdex_mode

   # set mode for given local directory or file
   def set_local_mode(self, file, isfile = 1, nmode = 0, omode = 0, logname = None, logact = 0):
      if not nmode: nmode = (self.PGLOG['FILEMODE'] if isfile else self.PGLOG['EXECMODE'])
      if not (omode and logname):
         info = self.check_local_file(file, 6)
         if not info:
            if info != None: return self.FAILURE 
            return self.lmsg(file, "{} to set mode({})".format(self.PGLOG['MISSFILE'], self.int2base(nmode, 8)), logact)   
         omode = info['mode']
         logname = info['logname']
      if nmode == omode: return self.SUCCESS
      try:
         os.chmod(file, nmode)
      except Exception as e:
         return self.errlog(str(e), 'L', 1, logact)
      return self.SUCCESS

   # set mode for given directory or file on remote host
   def set_remote_mode(self, file, isfile, host, nmode = 0, omode = 0, logact = 0):
      if not nmode: nmode = (self.PGLOG['FILEMODE'] if isfile else self.PGLOG['EXECMODE'])
      if not omode:
         info = self.check_remote_file(file, host, 6)
         if not info:
            if info != None: return self.FAILURE
            return self.errlog("{}-{}: {} to set mode({})".format(host, file, self.PGLOG['MISSFILE'], self.int2base(nmode, 8)), 'R', 1, logact)
         omode = info['mode']
      if nmode == omode: return self.SUCCESS
      return self.pgsystem("{} -m {} {}".format(self.get_sync_command(host), self.int2base(nmode, 8), file), logact, 5)

   # change group for given local directory or file
   def change_local_group(self, file, ngrp = None, ogrp = None, logname = None, logact = 0):
      if not ngrp:
         ngid = self.PGLOG['GDEXGID']
      else:
         ngid = grp.getgrnam(ngrp).gr_gid
      if logact and logact&self.EXITLG: logact &=~self.EXITLG
      if not (ogrp and logname):
         info = self.check_local_file(file, 10, logact)
         if not info:
            if info != None: return self.FAILURE
            return self.errlog("{}: {} to change group({})".format(file, self.PGLOG['MISSFILE'], ngrp), 'L', 1, logact)   
         ogid = info['gid']
         ouid = info['uid']
      else:
         ouid = pwd.getpwnam(logname).pw_uid
         ogid = grp.getgrnam(logname).gr_gid
      if ngid == ogid: return self.SUCCESS
      try:
         os.chown(file, ouid, ngid)
      except Exception as e:
         return self.errlog(str(e), 'L', 1, logact)

   # Check if given path on a specified host or the host itself are down
   #   path: path name to be checked
   #   host: host name the file on, default to self.LHOST
   # chkopt: 1 - do a file/path check, 0 - do not 
   # Return array of 2 (hstat, msg)
   #         hstat: 0 if system is up and accessible,
   #                1 - host is down,
   #                2 - if path not accessible
   #                negative values if planned system down
   #           msg: None - stat == 0
   #                an unempty string for system down message - stat != 0 
   def host_down_status(self, path, host, chkopt = 0, logact = 0):
      shost = self.strip_host_name(host)
      hstat = 0
      rets = [0, None]
      msg = hostname = None
      if self.pgcmp(shost, self.LHOST, 1) == 0:
         if not path or (chkopt and self.check_local_file(path)): return rets
         msg = path + ": is not accessible"
         flag = "L"
         if re.match(r'^(/{}/|{})'.format(self.PGLOG['GPFSNAME'], self.PGLOG['DSSDATA']), path):
            hstat = 1
            hostname = self.PGLOG['GPFSNAME']
         else:
            hstat = 2
      elif self.pgcmp(shost, self.PGLOG['GPFSNAME'], 1) == 0:
         if not path or (chkopt and self.check_local_file(path)): return rets
         msg = path + ": is not accessible"
         flag = "L"
         hstat = 1
         hostname = self.PGLOG['GPFSNAME']
      elif self.pgcmp(shost, self.BHOST, 1) == 0:
         if path:
            hstat = 2
         else:
            hstat = 1
            path = self.DPATHS['B']
         if chkopt and self.check_backup_file(path, self.QPOINTS['B']): return rets
         hostname = self.BHOST
         msg = "{}-{}: is not accessible".format(hostname, path)
         flag = "B"
      elif self.pgcmp(shost, self.DHOST, 1) == 0:
         if path:
            hstat = 2
         else:
            hstat = 1
            path = self.DPATHS['B']
         if chkopt and self.check_backup_file(path, self.QPOINTS['D']): return rets
         hostname = self.DHOST
         msg = "{}-{}: is not accessible".format(hostname, path)
         flag = "D"
      elif self.pgcmp(shost, self.OHOST, 1) == 0:
         if path:
            hstat = 2
         else:
            hstat = 1
            path = self.PGLOG['OBJCTBKT']  
         if chkopt and self.check_object_file(path): return rets
         hostname = self.OHOST
         msg = "{}-{}: is not accessible".format(hostname, path)
         flag = "O"
      elif self.pgcmp(shost, self.PGLOG['PGBATCH'], 1):
         if path and chkopt and self.check_remote_file(path, host): return rets
         estat = self.ping_remote_host(host)
         if estat:
            hstat = 1
            hostname = host
         else:
            if not path: return rets
            if re.match(r'^/{}/'.format(self.PGLOG['GPFSNAME']), path):
               hstat = 1
               hostname = self.PGLOG['GPFSNAME']
            else:
               hstat = 2
               hostname = host      
         flag = "R"
         msg = "{}-{}: is not accessible".format(host, path)
      elif self.get_host(1) == self.PGLOG['PGBATCH']:   # local host is a batch node
         if not path or (chkopt and self.check_local_file(path)): return rets
         msg = path + ": is not accessible"
         flag = "L"
         if re.match(r'^(/{}/|{})'.format(self.PGLOG['GPFSNAME'], self.PGLOG['DSSDATA']), path):
            hstat = 1
            hostname = self.PGLOG['GPFSNAME']
         else:
            hstat = 2   
      msg += " at the moment Checked on " + self.PGLOG['HOSTNAME']
      if hostname:
        estat = self.system_down_message(hostname, path, 0, logact) 
        if estat:
           hstat = -hstat
           msg += "\n" + estat
      if logact and (chkopt or hstat < 0): self.errlog(msg, flag, 1, logact)
      return (hstat, msg)

   # Check if given path on a specified host is down or not
   # path: path name to be checked
   # host: host name the file on, default to self.LHOST
   # Return errmsg if not accessible and None otherwise
   def check_host_down(self, path, host, logact = 0):
      (hstat, msg) = self.host_down_status(path, host, 1, logact)
      return msg if hstat else None

   # Check if given service name is accessible from a specified host
   #  sname: service name to be checked
   #  fhost: from host name to connect to service, default to self.LHOST
   #  reset the service flag to A or I accordingly
   # Return 0 if accessible, dsservice.sindex if not, and -1 if can not be checked
   def check_service_accessibilty(self, sname, fhost = None, logact = 0):
      if not fhost: fhost = self.PGLOG['HOSTNAME']
      pgrec = self.pgget("dsservice", "*", "service = '{}' AND hostname = '{}'".format(sname, fhost), logact)
      if not pgrec:
         self.pglog("dsservice: Access {} from {} is not defined in GDEX Configuration".format(sname, fhost), logact)
         return -1
      path = sname if (pgrec['flag'] == "H" or pgrec['flag'] == "G") else None
      (hstat, msg) = self.host_down_status(path, fhost, 1, logact)
      return msg if hstat else None

   # check if this host is a local host for given host name
   def is_local_host(self, host):
      host = self.strip_host_name(host)
      if host == self.LHOST or self.valid_batch_host(host): return 1
      return 0

   # check and return action string on a node other than local one
   def local_host_action(self, host, action, info, logact = 0):
      if self.is_local_host(host): return 1
      if not logact: return 0
      if host == "partition":
         msg = "for individual partition"
      elif host == "rda_config":
         msg = "via https://gdex.ucar.edu/rda_pg_config"
      elif host in self.BCHCMDS:
         msg = "on a {} Node".format(host)
      else:
         msg = "on " + host
      return self.pglog("{}: Cannot {}, try {}".format(info, action, msg), logact)

   # ping a given remote host name
   # return None if system is up error messge if not
   def ping_remote_host(self, host):
      while True:
         buf = self.pgsystem("ping -c 3 " + host, self.LOGWRN, self.CMDRET)
         if buf:
            ms = re.search(r'3 packets transmitted, (\d)', buf)
            if ms:
               if int(ms.group(1)) > 0:
                  return None
               else:
                  return host + " seems down not accessible"
         if self.PGLOG['SYSERR']:
            if self.PGLOG['SYSERR'].find("ping: unknown host") > -1 and host.find('.') > -1:
               host += ".ucar.edu"
               continue
            return self.PGLOG['SYSERR']
         else:
            return "Cannot ping " + host

   # compare given two host names, return 1 if same and 0 otherwise
   def same_hosts(self, host1, host2):   
      host1 = self.strip_host_name(host1)
      host2 = self.strip_host_name(host2)
      return (1 if self.pgcmp(host1, host2, 1) == 0 else 0)

   #  strip and identify the proper host name
   def strip_host_name(self, host):
      if not host: return self.LHOST
      ms = re.match(r'^([^\.]+)\.', host)
      if ms: host = ms.group(1)
      if self.pgcmp(host, self.PGLOG['HOSTNAME'], 1) == 0:
         return self.LHOST
      else:
         return host

   # Check a file stuatus info on a given host name (including local host) no background process for checking
   # file: file name to be checked
   # host: host name the file on, default to self.LHOST
   #  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
   #       1 - get date/time modified (date_modified, time_modfied)
   #       2 - get file owner's login name (logname)
   #       4 - get permission mode in 3 octal digits (mode)
   #       8 - get group name (group)
   #      16 - get week day 0-Sunday, 1-Monday (week_day)
   #      32 - get checksum (checksum), work for local file only
   # Return a dict of file info, or None if file not exists
   def check_gdex_file(self, file, host = None, opt = 0, logact = 0):
      if host is None: host = self.LHOST
      shost = self.strip_host_name(host)
      if self.pgcmp(shost, self.LHOST, 1) == 0:
         return self.check_local_file(file, opt, logact)
      elif self.pgcmp(shost, self.OHOST, 1) == 0:
         return self.check_object_file(file, None, opt, logact)      
      elif self.pgcmp(shost, self.BHOST, 1) == 0:
         return self.check_backup_file(file, self.QPOINTS['B'], opt, logact)      
      elif self.pgcmp(shost, self.DHOST, 1) == 0:
         return self.check_backup_file(file, self.QPOINTS['D'], opt, logact)      
      else:
         return self.check_remote_file(file, host, opt, logact)
   check_rda_file = check_gdex_file

   # wrapper to self.check_local_file() and self.check_globus_file() to check info for a file
   # on local or remote Globus endpoints
   def check_globus_file(self, file, endpoint = None, opt = 0, logact = 0):
      if not endpoint: endpoint = self.PGLOG['BACKUPEP']
      if endpoint == 'gdex-glade':
         if re.match(r'^/(data|decsdata)/', file): file = self.PGLOG['DSSDATA'] + file
         return self.check_local_file(file, opt, logact)
      else:
         return self.check_backup_file(file, endpoint, opt, logact)

   # check and get local file status information
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
   # Return: a dict of file info, or None if not exists
   def check_local_file(self, file, opt = 0, logact = 0):
      ret = None
      if not file: return ret
      loop = 0
      while loop < 2:
         if op.exists(file):
            try:
               fstat = os.stat(file)
               ret = self.local_file_stat(file, fstat, opt, logact)
               break
            except Exception as e:
               errmsg = "{}: {}".format(file, str(e))
               (hstat, msg) = self.host_down_status(file, self.LHOST, 0, logact)
               if hstat: errmsg += "\n" + msg
               self.errlog(errmsg, 'L', loop, logact)
         else:
            if loop > 0 or opt&128 == 0: break
            self.pglog(file + ": check it again in a moment", self.LOGWRN)
            time.sleep(6)
         loop += 1
      if loop > 1: return self.FAILURE
      self.ECNTS['L'] = 0   # reset error count
      return ret

   # local function to get local file stat
   def local_file_stat(self, file, fstat, opt, logact):
      if not fstat:
         self.errlog(file + ": Error check file stat", 'L', 1, logact)
         return None
      info = {}
      info['isfile'] = (1 if stat.S_ISREG(fstat.st_mode) else 0)
      if info['isfile'] == 0 and logact&self.PFSIZE:
         info['data_size'] = self.local_path_size(file)
      else:
         info['data_size'] = fstat.st_size
      info['fname'] = op.basename(file)
      if not opt: return info
      if opt&64 and info['isfile'] and info['data_size'] < self.PGLOG['MINSIZE']:
         self.pglog("{}: Remove {} file".format(file, ("Small({}B)".format(info['data_size']) if info['data_size'] else "Empty")), logact&~self.EXITLG)
         self.delete_local_file(file, logact)
         return None
      if opt&17:
         mdate, mtime = self.get_date_time(fstat.st_mtime)
         if opt&1:
            info['date_modified'] = mdate
            info['time_modified'] = mtime
            cdate, ctime = self.get_date_time(fstat.st_ctime)
            info['date_created'] = cdate
            info['time_created'] = ctime
         if opt&16: info['week_day'] = self.get_weekday(mdate)
      if opt&2:
         info['uid'] = fstat.st_uid
         info['logname'] = pwd.getpwuid(info['uid']).pw_name
      if opt&4: info['mode'] = stat.S_IMODE(fstat.st_mode)
      if opt&8:
         info['gid'] = fstat.st_gid
         info['group'] = grp.getgrgid(info['gid']).gr_name
      if opt&32 and info['isfile']: info['checksum'] = self.get_md5sum(file, 0, logact)
      return info

   # get total size of files under a given path
   @staticmethod
   def local_path_size(pname):
      if not pname: pname = '.'   # To get size of current directory
      size = 0
      for path, dirs, files in os.walk(pname):
         for f in files:
            size += os.path.getsize(os.path.join(path, f))
      return size

   # check and get file status information of a file on remote host
   # file: remote File name
   #  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
   #       1 - get date/time modified (date_modified, time_modfied)
   #       2 - file owner's login name (logname), assumed 'gdexdata'
   #       4 - get permission mode in 3 octal digits (mode)
   #       8 - get group name (group), assumed 'dss'
   #      16 - get week day 0-Sunday, 1-Monday (week_day)
   # Return: a dict of file info, or None if not exists 
   def check_remote_file(self, file, host, opt = 0, logact = 0):
      if not file: return None
      ms = re.match(r'^(.+)/$', file)
      if ms: file = ms.group(1)    # remove ending '/' in case
      cmd = "{} {}".format(self.get_sync_command(host), file)
      loop = 0
      while loop < 2:
         buf = self.pgsystem(cmd, self.LOGWRN, self.CMDRET)
         if buf or not self.PGLOG['SYSERR'] or self.PGLOG['SYSERR'].find(self.PGLOG['MISSFILE']) > -1: break
         errmsg = self.PGLOG['SYSERR']
         (hstat, msg) = self.host_down_status(file, host, 0, logact)
         if hstat: errmsg += "\n" + msg
         self.errlog(errmsg, 'R', loop, logact)
         loop += 1
      if loop > 1: return self.FAILURE
      self.ECNTS['R'] = 0   # reset error count
      if buf:
         for line in re.split(r'\n', buf):
            info = self.remote_file_stat(line, opt)
            if info: return info
      return None

   # local function to get remote file stat
   def remote_file_stat(self, line, opt):
      info = {}
      items = re.split(r'\s+', line)
      if len(items) < 5 or items[4] == '.': return None
      ms = re.match(r'^([d\-])([\w\-]{9})$',  items[0])
      info['isfile'] = (1 if ms and ms.group(1) == "-" else 0)
      if opt&4: info['mode'] = self.get_file_mode(ms.group(2))
      fsize = items[1]
      if fsize.find(',') > -1: fsize = re.sub(r',', '', fsize)
      info['data_size'] = int(fsize)
      info['fname'] = op.basename(items[4])
      if not opt: return info
      if opt&17:
         mdate = self.format_date(items[2], "YYYY-MM-DD", "YYYY/MM/DD")
         mtime = items[3]
         if self.PGLOG['GMTZ']: (mdate, mtime) = self.addhour(mdate, mtime, self.PGLOG['GMTZ'])
         if opt&1:
            info['date_modified'] = mdate
            info['time_modified'] = mtime
         if opt&16: info['week_day'] = self.get_weekday(mdate)
      if opt&2: info['logname'] = "gdexdata"
      if opt&8: info['group'] = self.PGLOG['GDEXGRP']
      return info

   # check and get object file status information
   # file: object store File key name
   #  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
   #       1 - get date/time modified (date_modified, time_modfied)
   #       2 - get file owner's login name (logname)
   #       4 - get metadata hash
   #       8 - get group name (group)
   #      16 - get week day 0-Sunday, 1-Monday (week_day)
   #      32 - get checksum (checksum)
   #      64 - check once, no rechecking
   # Return a dict of file info, or None if file not exists
   def check_object_file(self, file, bucket = None, opt = 0, logact = 0):
      if not bucket: bucket = self.PGLOG['OBJCTBKT']
      ret = None
      if not file: return ret
      cmd = "{} lo {} -b {}".format(self.OBJCTCMD, file, bucket)
      ucmd = "{} gm -k {} -b {}".format(self.OBJCTCMD, file, bucket) if opt&14 else None
      loop = 0
      while loop < 2:
         buf = self.pgsystem(cmd, self.LOGWRN, self.CMDRET)
         if buf:
            if re.match(r'^\[\]', buf): break
            if re.match(r'^\[\{', buf):
               ary = json.loads(buf)
               cnt = len(ary)
               if cnt > 1: return self.pglog("{}-{}: {} records returned\n{}".format(bucket, file, cnt, buf), logact|self.ERRLOG)
               hash = ary[0]
               uhash = None
               if ucmd:
                  ubuf = self.pgsystem(ucmd, self.LOGWRN, self.CMDRET)
                  if ubuf and re.match(r'^\{', ubuf): uhash = json.loads(ubuf)
               ret = self.object_file_stat(hash, uhash, opt)
               break
         if opt&64: return self.FAILURE
         errmsg = "Error Execute: {}\n{}".format(cmd, self.PGLOG['SYSERR'])
         (hstat, msg) = self.host_down_status(bucket, self.OHOST, 0, logact)
         if hstat: errmsg += "\n" + msg
         self.errlog(errmsg, 'O', loop, logact)
         loop += 1
      if loop > 1: return self.FAILURE
      self.ECNTS['O'] = 0   # reset error count
      return ret

   # check an object path status information
   # path: object store path name
   # Return count of object key names, 0 if not file exists; None if error checking
   def check_object_path(self, path, bucket = None, logact = 0):
      if not bucket: bucket = self.PGLOG['OBJCTBKT']
      ret = None
      if not path: return ret
      cmd = "{} lo {} -ls -b {}".format(self.OBJCTCMD, path, bucket)
      loop = 0
      while loop < 2:
         buf = self.pgsystem(cmd, self.LOGWRN, self.CMDRET)
         if buf:
            ary = json.loads(buf)
            return len(ary)
         errmsg = "Error Execute: {}\n{}".format(cmd, self.PGLOG['SYSERR'])
         (hstat, msg) = self.host_down_status(bucket, self.OHOST, 0, logact)
         if hstat: errmsg += "\n" + msg
         self.errlog(errmsg, 'O', loop, logact)
         loop += 1
      self.ECNTS['O'] = 0   # reset error count
      return ret

   # object store function to get file stat
   def object_file_stat(self, hash, uhash, opt):
      info = {'isfile': 1, 'data_size': int(hash['Size']), 'fname': op.basename(hash['Key'])}
      if not opt: return info   
      if opt&17:
         ms = re.match(r'^(\d+-\d+-\d+)\s+(\d+:\d+:\d+)', hash['LastModified'])
         if ms:
            (mdate, mtime) = ms.groups()
            if self.PGLOG['GMTZ']: (mdate, mtime) = self.addhour(mdate, mtime, self.PGLOG['GMTZ'])
            if opt&1:
               info['date_modified'] = mdate
               info['time_modified'] = mtime
            if opt&16: info['week_day'] = self.get_weekday(mdate)
      if opt&32:
         ms = re.match(r'"(.+)"',  hash['ETag'])
         if ms: info['checksum'] = ms.group(1)
      if uhash:
         if opt&2: info['logname'] = uhash['user']
         if opt&4: info['meta'] = uhash
         if opt&8: info['group'] = uhash['group']
      return info

   # check and get backup file status information
   # file: backup File key name
   #  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
   #       1 - get date/time modified (date_modified, time_modfied)
   #       2 - get file owner's login name (logname)
   #       4 - get metadata hash
   #       8 - get group name (group)
   #      16 - get week day 0-Sunday, 1-Monday (week_day)
   #      64 - rechecking
   # Return a dict of file info, or None if file not exists
   def check_backup_file(self, file, endpoint = None, opt = 0, logact = 0):
      ret = None
      if not file: return ret
      if not endpoint: endpoint = self.PGLOG['BACKUPEP']
      bdir = op.dirname(file)
      bfile = op.basename(file)
      cmd = f"{self.BACKCMD} ls -ep {endpoint} -p {bdir} --filter {bfile}"
      ccnt = loop = 0
      while loop < 2:
         buf = self.pgsystem(cmd, logact, self.CMDRET)
         syserr = self.PGLOG['SYSERR']
         if buf:
            getstat = 0
            for line in re.split(r'\n', buf):
               if re.match(r'^(User|-+)\s*\|', line):
                  getstat += 1
               elif getstat > 1:
                  ret = self.backup_file_stat(line, opt)
                  if ret: break
            if ret: break
            if loop or opt&64 == 0: return ret
            time.sleep(self.PGSIG['ETIME'])
         elif syserr:
            if syserr.find("Directory '{}' not found on endpoint".format(bdir)) > -1:
               if loop or opt&64 == 0: return ret
               time.sleep(self.PGSIG['ETIME'])
            elif ccnt < 2 and syserr.find("The connection to the server was broken") > -1:
               time.sleep(self.PGSIG['ETIME'])
               ccnt += 1
               continue
            else:
               if opt&64 == 0: return self.FAILURE
               errmsg = "Error Execute: {}\n{}".format(cmd, syserr)
               (hstat, msg) = self.host_down_status('', self.QHOSTS[endpoint], 0, logact)
               if hstat: errmsg += "\n" + msg
               self.errlog(errmsg, 'B', loop, logact)
         loop += 1
      if ret: self.ECNTS['B'] = 0   # reset error count
      return ret

   # backup store function to get file stat
   def backup_file_stat(self, line, opt):
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
         if self.PGLOG['GMTZ']: (mdate, mtime) = self.addhour(mdate, mtime, self.PGLOG['GMTZ'])
         if opt&1:
            info['date_modified'] = mdate
            info['time_modified'] = mtime
         if opt&16: info['week_day'] = self.get_weekday(mdate)
      if opt&2: info['logname'] = items[0]
      if opt&4: info['mode'] = self.get_file_mode(items[2])
      if opt&8: info['group'] = items[1]
      return info

   # check and get a file status information inside a tar file
   # file: File name to be checked
   # tfile: the tar file name
   #  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
   #       1 - get date/time modified (date_modified, time_modfied)
   #       2 - get file owner's login name (logname)
   #       4 - get permission mode in 3 octal digits (mode)
   #       8 - get group name (group)
   #      16 - get week day 0-Sunday, 1-Monday (week_day)
   # Return a dict of file info, or None if file not exists
   def check_tar_file(self, file, tfile, opt = 0, logact = 0):
      ret = None 
      if not (file and tfile): return ret
      for loop in range(2):
         buf = self.pgsystem("tar -tvf {} {}".format(tfile, file), self.LOGWRN, self.CMDRET)
         if buf or not self.PGLOG['SYSERR'] or self.PGLOG['SYSERR'].find('Not found in archive') > -1: break
         errmsg = self.PGLOG['SYSERR']
         (hstat, msg) = self.host_down_status(tfile, self.LHOST, 0, logact)
         self.errlog(errmsg, 'L', loop, logact)
      if loop > 0: return self.FAILURE
      if buf:
         for line in re.split(r'\n', buf):
            ret = self.tar_file_stat(line, opt)
            if ret: break
      self.ECNTS['L'] = 0   # reset error count
      return ret

   # local function to get file stat in a tar file
   def tar_file_stat(self, line, opt):
      items = re.split(r'\s+', line)
      if len(items) < 6: return None
      ms = re.match(r'^([d\-])([\w\-]{9})$', items[0])
      if not ms: return None
      info = {}
      info['isfile'] = (1 if ms and ms.group(1) == "-" else 0)
      info['data_size'] = int(items[2])
      info['fname'] = op.basename(items[5])
      if not opt: return info
      if opt&4: info['mode'] = self.get_file_mode(ms.group(2))
      if opt&17:
         mdate = items[3]
         mtime = items[4]
         if self.PGLOG['GMTZ']: (mdate, mtime) = self.addhour(mdate, mtime, self.PGLOG['GMTZ'])
         if opt&1:
            info['date_modified'] = mdate
            info['time_modified'] = mtime
         if opt&16: info['week_day'] = self.get_weekday(mdate)
      if opt&10:
         ms = re.match(r'^(\w+)/(\w+)', items[1])
         if ms:
            if opt&2: info['logname'] = ms.group(1)
            if opt&8: info['group'] = ms.group(2)
      return info

   # check and get a file status information on ftp server
   # file: File name to be checked
   # name: login user name
   # pswd: login password
   #  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
   #       1 - get date/time modified (date_modified, time_modfied)
   #       2 - get file owner's login name (logname)
   #       4 - get permission mode in 3 octal digits (mode)
   #       8 - get group name (group)
   #      16 - get week day 0-Sunday, 1-Monday (week_day)
   # Return a dict of file info, or None if file not exists
   def check_ftp_file(self, file, opt = 0, name = None, pswd = None, logact = 0):
      if not file: return None
      ms = re.match(r'^(.+)/$', file)
      if ms: file = ms.group(1)     # remove ending '/' in case
      cmd = "ncftpls -l "
      if name: cmd += "-u {} ".format(name)
      if pswd: cmd += "-p {} ".format(pswd)
      fname = op.basename(file)
      for loop in range(2):
         buf = self.pgsystem(cmd + file, self.LOGWRN, self.CMDRET)
         if buf: break
         if self.PGLOG['SYSERR']:
            self.errlog(self.PGLOG['SYSERR'], 'O', loop, logact|self.LOGERR)         
         if loop == 0: file = op.dirname(file) + '/'
      if loop > 1: return self.FAILURE
      for line in re.split(r'\n', buf):
         if not line or line.find(fname) < 0: continue
         info = self.ftp_file_stat(line, opt)
         if info: return info
      return None   

   # local function to get stat of a file on ftp server
   def ftp_file_stat(self, line, opt):
      items = re.split(r'\s+', line)
      if len(items) < 9: return None
      ms = re.match(r'^([d\-])([\w\-]{9})$', items[0])
      info = {}
      info['isfile'] = (1 if ms and ms.group(1) == "-" else 0)
      info['data_size'] = int(items[4])
      info['fname'] = op.basename(items[8])
      if not opt: return info
      if opt&4: info['mode'] = self.get_file_mode(ms.group(2))
      if opt&17:
         dy = int(items[6])
         mn = self.get_month(items[5])
         if re.match(r'^\d+$', items[7]):
            yr = int(items[7])
            mtime = "00:00:00"
         else:
            mtime = items[7] + ":00"
            cdate = self.curdate()
            ms = re.match(r'^(\d+)-(\d\d)', cdate)
            if ms:
               yr = int(ms.group(1))
               cm = int(ms.group(2))   # current month
               if cm < mn: yr -= 1     # previous year
         mdate = "{}-{:02}-{:02}".format(yr, mn, dy)
         if opt&1:
            info['date_modified'] = mdate
            info['time_modified'] = mtime
         if opt&16: info['week_day'] = self.get_weekday(mdate)
      if opt&2: info['logname'] = items[2]
      if opt&8: info['group'] = items[3]
      return info

   # get an array of directories/files under given dir on a given host name (including local host)
   #  dir: directory name to be listed
   # host: host name the directory on, default to self.LHOST
   #  opt:  0 - get data size only (fname, data_size, isfile), fname is the file basename
   #        1 - get date/time modified (date_modified, time_modfied)
   #        2 - get file owner's login name (logname)
   #        4 - get permission mode in 3 octal digits (mode)
   #        8 - get group name (group)
   #       16 - get week day 0-Sunday, 1-Monday (week_day)
   #       32 - get checksum (checksum), work for local file only
   # Return: a dict with filenames as keys None if empty directory
   def gdex_glob(self, dir, host, opt = 0, logact = 0):
      shost = self.strip_host_name(host)
      if self.pgcmp(shost, self.LHOST, 1) == 0:
         return self.local_glob(dir, opt, logact)
      elif self.pgcmp(shost, self.OHOST, 1) == 0:
         return self.object_glob(dir, None, opt, logact)      
      elif self.pgcmp(shost, self.BHOST, 1) == 0:
         return self.backup_glob(dir, None, opt, logact)
      else:
         return self.remote_glob(dir, host, opt, logact)
   rda_glob = gdex_glob

   # get an array of directories/files under given dir on local host
   #  dir: directory name to be listed
   #  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
   #       1 - get date/time modified (date_modified, time_modfied)
   #       2 - get file owner's login name (logname)
   #       4 - get permission mode in 3 octal digits (mode)
   #       8 - get group name (group)
   #      16 - get week day 0-Sunday, 1-Monday (week_day)
   #      32 - get checksum (checksum), work for local file only
   #     256 - get files only and ignore directories
   # Return: dict with filenames as keys or None if empty directory
   def local_glob(self, dir, opt = 0, logact = 0):
      flist = {}
      if not re.search(r'[*?]', dir):
         if op.exists(dir):
            dir = self.join_paths(dir, "*")
         else:
            dir += "*"
      for file in glob.glob(dir):
         info = self.check_local_file(file, opt, logact)
         if info and (info['isfile'] or not 256&opt): flist[file] = info
      return flist

   # check and get file status information of a file on remote host
   #  dir: remote directory name
   # host: host name the directory on, default to self.LHOST
   #  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
   #       1 - get date/time modified (date_modified, time_modfied)
   #       2 - file owner's login name (logname), assumed 'gdexdata'
   #       4 - get permission mode in 3 octal digits (mode)
   #       8 - get group name (group), assumed 'dss'
   #      16 - get week day 0-Sunday, 1-Monday (week_day)
   # Return: dict with filenames as keys or None if empty directory
   def remote_glob(self, dir, host, opt = 0, logact = 0):
      flist = {}
      if not re.search(r'/$', dir): dir += '/'
      buf = self.pgsystem(self.get_sync_command(host) + " dir", self.LOGWRN, self.CMDRET)
      if not buf:
         if self.PGLOG['SYSERR'] and self.PGLOG['SYSERR'].find(self.PGLOG['MISSFILE']) < 0:
            self.errlog("{}-{}: Error list directory\n{}".format(host, dir, self.PGLOG['SYSERR']), 'R', 1, logact)   
         return flist
      for line in re.split(r'\n', buf):
         info = self.remote_file_stat(line, opt)
         if info: flist[dir + info['fname']] = info
      return flist

   # check and get muiltiple object store file status information
   #  dir: object directory name
   #  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
   #       1 - get date/time modified (date_modified, time_modfied)
   #       2 - get file owner's login name (logname)
   #       8 - get group name (group)
   #      16 - get week day 0-Sunday, 1-Monday (week_day)
   # Return: a dict with filenames as keys, or None if not exists
   def object_glob(self, dir, bucket = None, opt = 0, logact = 0):
      flist = {}
      if not bucket: bucket = self.PGLOG['OBJCTBKT']
      ms = re.match(r'^(.+)/$', dir)
      if ms: dir = ms.group(1)
      cmd = "{} lo {} -b {}".format(self.OBJCTCMD, dir, bucket)
      ary = err = None
      buf = self.pgsystem(cmd, self.LOGWRN, self.CMDRET)
      if buf:
         if re.match(r'^\[\{', buf):
            ary = json.loads(buf)
         elif not re.match(r'^\[\]', buf):
            err = "{}\n{}".format(self.PGLOG['SYSERR'], buf)
      else:
         err = self.PGLOG['SYSERR']
      if not ary:
         if err:
            self.errlog("{}-{}-{}: Error list files\n{}".format(self.OHOST, bucket, dir, err), 'O', 1, logact)
            return self.FAILURE
         else:
            return flist
      for hash in ary:
         uhash = None
         if opt&10:
            ucmd = "{} gm -l {} -b {}".format(self.OBJCTCMD, hash['Key'], bucket)
            ubuf = self.pgsystem(ucmd, self.LOGWRN, self.CMDRET)
            if ubuf and re.match(r'^\{.+', ubuf): uhash = json.loads(ubuf)
         info = self.object_file_stat(hash, uhash, opt)
         if info: flist[hash['Key']] = info
      return flist

   # check and get muiltiple Quasar backup file status information
   #  dir: backup path
   #  opt: 0 - get data size only (fname, data_size, isfile), fname is the file basename
   #       1 - get date/time modified (date_modified, time_modfied)
   #       2 - get file owner's login name (logname)
   #       8 - get group name (group)
   #      16 - get week day 0-Sunday, 1-Monday (week_day)
   #      64 - rechecking
   # Return: a dict with filenames as keys, or None if not exists
   def backup_glob(self, dir, endpoint = None, opt = 0, logact = 0):
      if not dir: return None
      if not endpoint: endpoint = self.PGLOG['BACKUPEP']
      cmd = f"{self.BACKCMD} ls -ep {endpoint} -p {dir}"
      flist = {}
      for loop in range(2):
         buf = self.pgsystem(cmd, logact, self.CMDRET)
         syserr = self.PGLOG['SYSERR']
         if buf:
            getstat = 0
            for line in re.split(r'\n', buf):
               if re.match(r'^(User|-+)\s*\|', line):
                  getstat += 1
               elif getstat > 1:
                  info = self.backup_file_stat(line, opt)
                  if info: flist[info['fname']] = info
            if flist: break
            if loop or opt&64 == 0: return None
            time.sleep(self.PGSIG['ETIME'])
         elif syserr:
            if syserr.find("Directory '{}' not found on endpoint".format(dir)) > -1:
               if loop or opt&64 == 0: return None
               time.sleep(self.PGSIG['ETIME'])
            else:
               if opt&64 == 0: return self.FAILURE
               errmsg = "Error Execute: {}\n{}".format(cmd, syserr)
               (hstat, msg) = self.host_down_status('', self.QHOSTS[endpoint], 0, logact)
               if hstat: errmsg += "\n" + msg
               self.errlog(errmsg, 'B', loop, logact)
      if flist:
         self.ECNTS['B'] = 0   # reset error count
         return flist
      else:
         return self.FAILURE

   # local function to get file/directory mode for given permission string, for example, rw-rw-r--
   @staticmethod
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

   # Evaluate md5 checksum
   #  file: file name for MD5 checksum
   # count: defined if filename is a array 
   # Return: one or a array of 128-bits md5 'fingerprint' None if failed
   def get_md5sum(self, file, count = 0, logact = 0):
      cmd = 'md5sum '
      if count > 0:
         checksum = [None]*count
         for i in range(count):
            if op.isfile(file[i]):
               chksm = self.pgsystem(cmd + file[i], logact, 20)
               if chksm:
                  ms = re.search(r'(\w{32})', chksm)
                  if ms: checksum[i] = ms.group(1)
      else:
         checksum = None
         if op.isfile(file):
            chksm = self.pgsystem(cmd + file, logact, 20)
            if chksm:
               ms = re.search(r'(\w{32})', chksm)
               if ms: checksum = ms.group(1)
      return checksum

   # Evaluate md5 checksums and compare them for two given files
   #  file1, file2: file names
   # Return: 0 if same and 1 if not
   def compare_md5sum(self, file1, file2, logact = 0):
      if op.isdir(file1) or op.isdir(file2):
         files1 = self.get_directory_files(file1)
         fcnt1 = len(files1) if files1 else 0
         files2 = self.get_directory_files(file2)
         fcnt2 = len(files2) if files2 else 0
         if fcnt1 != fcnt2: return 1
         chksm1 = self.get_md5sum(files1, fcnt1, logact)
         chksm1 = ''.join(chksm1)
         chksm2 = self.get_md5sum(files1, fcnt2, logact)
         chksm2 = ''.join(chksm2)
      else:
         chksm1 = self.get_md5sum(file1, 0, logact)
         chksm2 = self.get_md5sum(file2, 0, logact)
      return (0 if (chksm1 and chksm2 and chksm1 == chksm2) else 1)

   #  change local directory to todir, and return odir upon success
   def change_local_directory(self, todir, logact = 0):
      if logact:
         lact = logact&~(self.EXITLG|self.ERRLOG)
      else:
         logact = lact = self.LOGWRN
      if not op.isdir(todir):
         if op.isfile(todir): return self.errlog(todir + ": is file, cannot change directory", 'L', 1, logact)
         if not self.make_local_directory(todir, logact): return self.FAILURE 
      odir = self.PGLOG['CURDIR']
      if todir == odir:
         self.pglog(todir + ": in Directory", lact)
         return odir
      try:
         os.chdir(todir)
      except Exception as e:
         return self.errlog(str(e), 'L', 1, logact)
      else:
         if not op.isabs(todir): todir = os.getcwd()
         self.PGLOG['CURDIR'] = todir
         self.pglog(todir + ": Change to Directory", lact)
      return odir

   # record the directory for the deleted file
   # pass in empty dir to turn the recording delete directory on
   def record_delete_directory(self, dir, val):
      if dir is None:
         if isinstance(val, int):
            self.DIRLVLS = val
         elif re.match(r'^\d+$'):
            self.DIRLVLS = int(val)
      elif dir and not re.match(r'^(\.|\./|/)$', dir) and dir not in self.DELDIRS:
         self.DELDIRS[dir] = val

   # remove the recorded delete directory if it is empty
   def clean_delete_directory(self, logact = 0):
      if not self.DIRLVLS: return
      if logact:
         lact = logact&~(self.EXITLG)
      else:
         logact = lact = self.LOGWRN
      lvl = self.DIRLVLS
      self.DIRLVLS = 0     # set to 0 to stop recording directory
      while lvl > 0:
         lvl -= 1
         dirs = {}
         for dir in self.DELDIRS:
            host = self.DELDIRS[dir]
            dinfo = (dir if host == self.LHOST else  "{}-{}".format(host, dir))
            dstat = self.gdex_empty_directory(dir, self.DELDIRS[dir])
            if dstat == 0:
               if self.delete_gdex_file(dir, host, logact):
                  self.pglog(dinfo + ": Empty directory removed", lact)
            elif dstat > 0:
               if dstat == 1 and lvl > 0: self.pglog(dinfo + ": Directory not empty yet", lact)
               continue
            if lvl: dirs[op.dirname(dir)] = host
         if not dirs: break
         self.DELDIRS = dirs
      self.DELDIRS = {}   # empty cache afterward

   # remove the empty given directory and its all subdirectories
   # return 1 if empty dirctory removed 0 otherwise
   def clean_empty_directory(self, dir, host, logact = 0):
      if not dir: return 0
      dirs = self.gdex_glob(dir, host)
      cnt = 0
      if logact:
         lact = logact&~self.EXITLG
      else:
         lact = logact = self.LOGWRN
      if dirs:
         for name in dirs:
            cnt += 1
            if dirs[name]['isfile']: continue
            cnt -= self.clean_empty_directory(name, host, logact)
      
      dinfo = (dir if self.same_hosts(host, self.LHOST) else "{}-{}".format(host, dir))
      if cnt == 0:
         if self.delete_gdex_file(dir, host, logact):
            self.pglog(dinfo + ": Empty directory removed", lact)
            return 1
      else:
          self.pglog(dinfo + ": Directory not empty yet", lact)
      return 0

   # check if given directory is empty
   # Return: 0 if empty directory, 1 if not empty and -1 if invalid directory
   def gdex_empty_directory(self, dir, host):
      shost = self.strip_host_name(host)
      if self.pgcmp(shost, self.LHOST, 1) == 0:
         return self.local_empty_directory(dir)
      else:
         return self.remote_empty_directory(dir, host)
   rda_empty_directory = gdex_empty_directory

   # return 0 if empty local directory, 1 if not; -1 if cannot remove
   def local_empty_directory(self, dir):
      if not op.isdir(dir): return -1
      if self.is_root_directory(dir, 'L'): return 2
      if not re.search(r'/$', dir): dir += '/'
      dir += '*'
      return (1 if glob.glob(dir) else 0)

   # return 0 if empty remote directory, 1 if not; -1 if cannot remove
   def remote_empty_directory(self, dir, host):
      if self.is_root_directory(dir, 'R', host): return 2
      if not re.search(r'/$', dir): dir += '/'
      buf = self.pgsystem("{} {}".format(self.get_sync_command(host), dir), self.LOGWRN, self.CMDRET)
      if not buf: return -1
      for line in re.split(r'\n', buf):
         if self.remote_file_stat(line, 0): return 1
      return 0

   # get sizes of files on a given host
   # files: file names to get sizes
   # host: host name the file on, default to self.LHOST
   # return: array of file sizes size is -1 if file does not exist
   def gdex_file_sizes(self, files, host, logact = 0):
      sizes = []
      for file in files: sizes.append(self.gdex_file_size(file, host, 2, logact))
      return sizes
   rda_file_sizes = gdex_file_sizes

   # get sizes of local files
   # files: file names to get sizes
   # return: array of file sizes size is -1 if file does not exist
   def local_file_sizes(self, files, logact = 0):
      sizes = []
      for file in files: sizes.append(self.local_file_size(file, 6, logact))
      return sizes

   # check if a file on a given host is empty or too small to be considered valid
   # file: file name to be checked
   # host: host name the file on, default to self.LHOST
   #  opt: 1 - to remove empty file
   #       2 - show message for empty file
   #       4 - show message for non-existing file
   # return: file size in unit of byte
   #         0 - empty file or small file, with size < self.PGLOG['MINSIZE']
   #        -1 - file not exists
   #        -2 - error check file
   def gdex_file_size(self, file, host, opt = 0, logact = 0):
      info = self.check_gdex_file(file, host, 0, logact)
      if info:
         if info['isfile'] and info['data_size'] < self.PGLOG['MINSIZE']:
            if opt:
               if opt&2: self.errlog("{}-{}: {} file".format(host, file, ("Too small({}B)".format(info['data_size']) if info['data_size'] > 0 else "Empty")),
                                'O', 1, logact)
               if opt&1: self.delete_gdex_file(file, host, logact)
            return 0
         else:
            return info['data_size']  # if not regular file or not empty
      elif info != None:
         return -2   # error access
      else:
         if opt&4: self.errlog("{}-{}: {}".format(host, file, self.PGLOG['MISSFILE']), 'O', 1, logact)
         return -1   # file not exist   
   rda_file_size = gdex_file_size

   # check if a local file is empty or too small to be considered valid
   # file: file name to be checked
   #  opt: 1 - to remove empty file
   #       2 - show message for empty file
   #       4 - show message for non-existing file
   # return: file size in unit of byte
   #         0 - empty file or small file, with size < self.PGLOG['MINSIZE']
   #        -1 - file not exists
   #        -2 - error check file
   def local_file_size(self, file, opt = 0, logact = 0):
      if not op.exists(file):
         if opt&4: self.lmsg(file, self.PGLOG['MISSFILE'], logact)
         return -1   # file not eixsts
      info = self.check_local_file(file, 0, logact|self.PFSIZE)
      if info:
         if info['isfile'] and info['data_size'] < self.PGLOG['MINSIZE']:
            if opt:
               if opt&2: self.lmsg(file, ("Too small({}B)".format(info['data_size']) if info['data_size'] > 0 else "Empty file") , logact)
               if opt&1: self.delete_local_file(file, logact)
            return 0
         else:
            return info['data_size']  # if not regular file or not empty
      elif info != None:
         return -2   # error check file

   # compress/uncompress a single local file
   # ifile: file name to be compressed/uncompressed
   #   fmt: archive format
   #   act: 0 - uncompress
   #        1 - compress
   #        2 - get uncompress file name
   #        3 - get compress file name
   # return: array of new file name and archive format if changed otherwise original one
   def compress_local_file(self, ifile, fmt = None, act = 0, logact = 0):
      ms = re.match(r'^(.+)\.({})'.format(self.CMPSTR), ifile)
      if ms:
         ofile = ms.group(1)
      else:
         ofile = ifile
      if fmt:
         if act&1:
            for ext in self.PGCMPS:
               if re.search(r'(^|\.)({})(\.|$)'.format(ext), fmt, re.I):
                  ofile += '.' + ext
                  break   
         else:
            ms = re.search(r'(^|\.)({})$'.format(self.CMPSTR), fmt, re.I)
            if ms: fmt = re.sub(r'{}{}$'.format(ms.group(1), ms.group(2)), '', fmt, 1)
      if act < 2 and ifile != ofile: self.convert_files(ofile, ifile, 0, logact)
      return (ofile, fmt)

   # get file archive format from a givn file name; None if not found
   def get_file_format(self, fname):
      ms = re.search(r'\.({})$'.format(self.TARSTR), fname, re.I)
      if ms: return self.PGTARS[ms.group(1)][2]
      ms = re.search(r'\.({})$'.format(self.CMPSTR), fname, re.I)
      if ms: return self.PGCMPS[ms.group(1)][2]
      return None

   # tar/untar mutliple local file into/from a single tar/tar.gz/tgz/zip file
   # tfile: tar file name to be tar/untarred
   # files: member file names in the tar file
   #   fmt: archive format (defaults to tar file name extension must be defined in self.PGTARS
   #   act: 0 - untar
   #        1 - tar
   # return: self.SUCCESS upon successful self.FAILURE otherwise
   def tar_local_file(self, tfile, files, fmt, act, logact = 0):
      if not fmt:
         ms = re.search(r'\.({})$'.format(self.TARSTR), tfile, re.I)
         if ms: fmt = ms.group(1)
      logact |= self.ERRLOG   
      if not fmt: return self.pglog(tfile + ": Miss archive format", logact)
      if fmt not in self.PGTARS: return self.pglog(tfile + ": unknown format fmt provided", logact)
      tarray = self.PGTARS[fmt]
      if not act:  #untar member files
         cmd = "{} {}".format(tarray[1], tfile)
         if files: cmd += ' ' + ' '.join(files)
      else:
         if not files: return self.pglog(tfile + ": Miss member file to archive", logact)
         cmd = "{} {} {}".format(tarray[0], tfile, ' '.join(files))
      return self.pgsystem(cmd, logact, 7)

   # get local file archive format by checking extension of given local file name
   # file: local file name
   def local_archive_format(self,file):
      ms = re.search(r'\.({})$'.format(self.CMPSTR), file)
      if ms:
         fmt = ms.group(1)
         if re.search(r'\.tar\.{}$'.format(fmt), file):
            return "TAR." + fmt.upper()
         else:
            return fmt.upper()
      elif re.search(r'\.tar$', file):
         return "TAR"
      return ''

   # local function to show message with full local file path
   def lmsg(self, file, msg, logact = 0):
      if not op.isabs(file): file = self.join_paths(os.getcwd(), file)
      return self.errlog("{}: {}".format(file, msg), 'L', 1, logact)

   # check if given path is executable locally
   # return self.SUCCESS if yes self.FAILURE if not
   def check_local_executable(self, path, actstr = '', logact = 0):
      if os.access(path, os.W_OK): return self.SUCCESS
      if self.check_local_accessible(path, actstr, logact):
         if actstr: actstr += '-'
         self.errlog("{}{}: Accessible, but Unexecutable on'{}'".format(actstr, path, self.PGLOG['HOSTNAME']), 'L', 1, logact)
      return self.FAILURE

   # check if given path is writable locally
   # return self.SUCCESS if yes self.FAILURE if not
   def check_local_writable(self, path, actstr = '', logact = 0):
      if os.access(path, os.W_OK): return self.SUCCESS
      if self.check_local_accessible(path, actstr, logact):
         if actstr: actstr += '-'
         self.errlog("{}{}: Accessible, but Unwritable on'{}'".format(actstr, path, self.PGLOG['HOSTNAME']), 'L', 1, logact)
      return self.FAILURE

   # check if given path is accessible locally
   # return self.SUCCESS if yes, self.FAILURE if not
   def check_local_accessible(self, path, actstr = '', logact = 0):
      if os.access(path, os.F_OK): return self.SUCCESS
      if actstr: actstr += '-'
      self.errlog("{}{}: Unaccessible on '{}'".format(actstr, path, self.PGLOG['HOSTNAME']), 'L', 1, logact)
      return self.FAILURE

   # check if given webfile under self.PGLOG['DSSDATA'] is writable
   # return self.SUCCESS if yes self.FAILURE if not
   def check_webfile_writable(self, action, wfile, logact = 0):
      ms = re.match(r'^({}/\w+)'.format(self.PGLOG['DSSDATA']), wfile)
      if ms:
         return self.check_local_writable(ms.group(1), "{} {}".format(action, wfile), logact)
      else:
         return self.SUCCESS    # do not need check

   # convert the one file to another via uncompress, move/copy, and/or compress
   def convert_files(self, ofile, ifile, keep = 0, logact = 0):
      if ofile == ifile: return self.SUCCESS
      oname = ofile
      iname = ifile
      if keep: kfile = ifile + ".keep"
      oext = iext = None
      for ext in self.PGCMPS:
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
               self.local_copy_local(kfile, ifile, logact)
         if self.pgsystem("{} {}".format(self.PGCMPS[iext][1], ifile), logact, 5):
            if iext == "zip":
               path = op.dirname(iname)
               if path and path != '.': self.move_local_file(iname, op.basename(iname), logact)
               if not keep: self.delete_local_file(ifile, logact)
      if oname != iname:   # move/copy
         path = op.dirname(oname)
         if path and not op.exists(path): self.make_local_directory(path, logact)
         if keep and not op.exists(kfile):
            self.local_copy_local(oname, iname, logact)
            kfile = iname
         else:
            self.move_local_file(oname, iname, logact)
      if oext: # compress
         if keep and not op.exists(kfile):
            if oext == "zip":
               kfile = oname
            else:
               self.local_copy_local(kfile, oname, logact)
         if oext == "zip":
            path = op.dirname(oname)
            if path:
               if path != '.': path = self.change_local_directory(path, logact)
               bname = op.basename(oname)
               self.pgsystem("{} {}.zip {}".format(self.PGCMPS[oext][0], bname, bname), logact, 5)
               if path != '.': self.change_local_directory(path, logact)
            else:
               self.pgsystem("{} {} {}".format(self.PGCMPS[oext][0], ofile, oname), logact, 5)
            if not keep and op.exists(ofile): self.delete_local_file(oname, logact)
         else:
            self.pgsystem("{} {}".format(self.PGCMPS[oext][0], oname), logact, 5)
      if keep and op.exists(kfile) and kfile != ifile:
         if op.exist(ifile):
            self.delete_local_file(kfile, logact)
         else:
            self.move_local_file(ifile, kfile, logact)
      if op.exists(ofile):
         return self.SUCCESS
      else:
         return self.errlog("{}: ERROR convert from {}".format(ofile, ifile), 'L', 1, logact)

   #  comapre two files from given two hash references to the file information
   #  return 0 if same, 1  different, -1 if can not compare
   @staticmethod
   def compare_file_info(ainfo, binfo):
      if not (ainfo and binfo): return -1   # at least one is missing
      return (0 if (ainfo['data_size'] == binfo['data_size'] and
                    ainfo['date_modified'] == binfo['date_modified'] and
                    ainfo['time_modified'] == binfo['time_modified']) else 1)

   # get local_dirname
   @staticmethod
   def get_local_dirname(file):
      dir = op.dirname(file)
      if dir == '.': dir = os.getcwd()
      return dir

   # collect valid file names under a given directory, current directory if empty
   def get_directory_files(self, dir = None, limit = 0, level = 0):
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
               fs = self.get_directory_files(file, limit, level+1)
               if fs: files.extend(fs)
         else:
            files.append(file)
      return files if files else None

   # reads a local file into a string and returns it
   def read_local_file(self, file, logact = 0):
      try:
         fd = open(file, 'r')
      except Exception as e:
         return self.errlog("{}: {}".format(file, str(e)), 'L', 1, logact)
      else:
         fstr = fd.read()
         fd.close()
      return fstr

   # open a local file and return the file handler
   def open_local_file(self, file, mode = 'r', logact = None):
      if logact is None: logact = self.LOGERR
      try:
         fd = open(file, mode)
      except Exception as e:
         return self.errlog("{}: {}".format(file, str(e)), 'L', 1, logact)
      return fd

   # change absolute paths to relative paths
   def get_relative_paths(self, files, cdir, logact = 0):
      cnt = len(files)
      if cnt == 0: return files
      if not cdir: cdir = os.getcwd()
      for i in range(cnt):
         afile = files[i]
         if op.isabs(afile):
            files[i] = self.join_paths(afile, cdir, 1)
         else:
            self.pglog("{}: is not under the working directory '{}'".format(afile, cdir), logact)
      return files

   # check if the action to path is blocked
   def check_block_path(self, path, act = '', logact = 0):
      blockpath = self.PGLOG['USRHOME']
      if not act: act = 'Copy'
      if re.match(r'^{}'.format(blockpath), path):
         return self.pglog("{}: {} to {} is blocked".format(path, act, blockpath), logact)
      else:
         return 1

   # join two filenames by uing the common prefix/suffix and keeping the different main bodies,
   # the bodies are seprated by sep replace fext with text if provided
   def join_filenames(self, name1, name2, sep = '-', fext = None, text = None):
      if fext:
         name1 = self.remove_file_extention(name1, fext)   
         name2 = self.remove_file_extention(name2, fext)   
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
   # otherwise try to remove predfined compression extention in self.PGCMPS
   def remove_file_extention(self, fname, fext):
      if not fname: return ''
      if fext:
         fname = re.sub(r'\.{}$'.format(fext), '', fname, 1, re.I)
      else:
         for fext in self.PGCMPS:
            mp = r'\.{}$'.format(fext)
            if re.search(mp, fname):
               fname = re.sub(mp, '', fname, 1, re.I)
               break
      return fname
   
   # check if a previous down storage system is up now for given dflag
   # return error message if failed checking, and None otherwise
   def check_storage_down(self, dflag, dpath, dscheck, logact = 0):
      if dflag not in self.DHOSTS:
         if logact: self.pglog(dflag + ": Unknown Down Flag for Storage Systems", logact)
         return None
      dhost = self.DHOSTS[dflag]
      if not dpath and dflag in self.DPATHS: dpath = self.DPATHS[dflag]
      for loop in range(2):
         (stat, msg) = self.host_down_status(dpath, dhost, 1, logact)
         if stat < 0: break    # stop retry for planned down
   
      if not dscheck and self.PGLOG['DSCHECK']: dscheck = self.PGLOG['DSCHECK']
      if dscheck:
         didx = dscheck['dflags'].find(dflag)
         if msg:
            if didx < 0: dscheck['dflags'] += dflag
         else:
            if didx > -1: dscheck['dflags'].replace(dflag, '', 1)
      
      return msg

   # check if previous down storage systems recorded in the dflags
   # return an array of strings for storage systems that are still down,
   #        and empty array if all up
   def check_storage_dflags(self, dflags, dscheck = None, logact = 0):
      if not dflags: return 0
      isdict = isinstance(dflags, dict)
      msgary = []
      for dflag in dflags:
         msg = self.check_storage_down(dflag, dflags[dflag] if isdict else None, dscheck, logact)
         if msg: msgary.append(msg)
      if not msgary:
         if not dscheck and self.PGLOG['DSCHECK']: dscheck = self.PGLOG['DSCHECK']
         cidx = dscheck['cindex'] if dscheck else 0
         # clean dflags if the down storage systems are all up
         if cidx: self.pgexec("UPDATE dscheck SET dflags = '' WHERE cindex = {}".format(cidx), logact)
      return msgary

   # check a GDEX file is backed up or not for given file record;
   # clear the cached bfile records if frec is None.
   # return 0 if not yet, 1 if backed up, or -1 if backed up but modified
   def file_backup_status(self, frec, chgdays = 1, logact = 0):
      if frec is None:
         self.BFILES.clear()
         return 0
      bid = frec['bid']
      if not bid: return 0
      fields = 'bfile, dsid, date_modified'
      if chgdays > 0: fields += ', note'
      if bid not in self.BFILES: self.BFILES[bid] = self.pgget('bfile', fields, 'bid = {}'.format(bid), logact)
      brec = self.BFILES[bid]
      if not brec: return 0
      if 'sfile' in frec:
         fname = frec['sfile']
         ftype = 'Saved'
      else:
         fname = frec['wfile']
         ftype = 'Web'
      ret = 1
      fdate = frec['date_modified']
      bdate = brec['date_modified']
      if chgdays > 0 and self.diffdate(fdate, bdate) >= chgdays:
         ret = -1
         if brec['note']:
            mp = r'{}<:>{}<:>(\d+)<:>(\w+)<:>'.format(fname, frec['type']) 
            ms = re.search(mp, brec['note'])
            if ms:
               fsize = int(ms.group(1))
               cksum = ms.group(2)
               if cksum and cksum == frec['checksum'] or not cksum and fsize == frec['data_size']:
                  ret = 1
      if logact:
         if ret == 1:
            msg = "{}-{}: {} file backed up to /{}/{} by {}".format(frec['dsid'], fname, ftype, brec['dsid'], brec['bfile'], bdate)
         else:
            msg = "{}-{}: {} file changed on {}".format(frec['dsid'], fname, ftype, fdate)
         self.pglog(msg, logact)
      return ret
