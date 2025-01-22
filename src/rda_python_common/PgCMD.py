#
###############################################################################
#
#     Title : PgCMD.py
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 08/25/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module for functions to record commands for delayed
#             mode or command recovery
#
#    Github : https://github.com/NCAR/rda-python-common.git
#
###############################################################################
#
import os
import re
import sys
import time
from . import PgLOG
from . import PgSIG
from . import PgUtil
from . import PgLock
from . import PgDBI

# cached dscheck info
DSCHK = {}
BOPTIONS = {"hostname" : None, "qoptions" : None, "modules" : None, "environments" : None}
BFIELDS = ', '.join(BOPTIONS)

TRYLMTS = {
   'dsquasar'  : 3,
   'dsarch'  : 2,
   'default' : 1
}

DLYPTN = r'(^|\s)-(d|BP|BatchProcess|DelayedMode)(\s|$)'
DLYOPT = {
   'dsarch' : ' -d',
   'dsupdt' : ' -d',
   'dsrqst' : ' -d'
}
#
#  params: dict array holding option values
#     opt: 2 - each value of the dict array is a list; otherwise 1
# addhost: 1 to add host name too
# initial set Batch options passed in from command line
#
def set_batch_options(params, opt, addhost = 0):

   if 'QS' in params: BOPTIONS['qoptions'] = (params['QS'][0] if opt == 2 else params['QS'])
   if 'MO' in params: BOPTIONS['modules'] = (params['MO'][0] if opt == 2 else params['MO'])
   if 'EV' in params: BOPTIONS['environments'] = (params['EV'][0] if opt == 2 else params['EV'])
   if addhost and 'HN' in params: BOPTIONS['hostname'] = (params['HN'][0] if opt == 2 else params['HN'])

#
# boptions: dict array holding batch options
#  refresh: 1 to clean the previous cached global batch options
# checkkey: 1 to check and valid pre-defined fields
#
# fill Batch options recorded in RDADB 
#
def fill_batch_options(boptions, refresh = 0, checkkey = 0):

   if refresh:
      for bkey in BOPTIONS:
         BOPTIONS[bkey] = None   # clean the hash before filling it up

   if not boptions: return
   for bkey in boptions:
      if not checkkey or bkey in BOPTIONS:
         BOPTIONS[bkey] = boptions[bkey]

#
#     bkey: batch option field name
#     bval: batch option value
# override: 1 to override an existing option
#
# fill a single Batch option 
#
def set_one_boption(bkey, bval, override = 0):
   
   if bval:
      if override or not ( bkey in BOPTIONS and BOPTIONS[bkey]): BOPTIONS[bkey] = bval
   elif override and bkey in BOPTIONS and BOPTIONS[bkey]:
      BOPTIONS[bkey] = None

#
# fill the passed in dict record with the pre-saved batch options 
#
def get_batch_options(pgrec = None):

   record = {}   
   for bkey in BOPTIONS:
      if pgrec and bkey in pgrec and pgrec[bkey]:
         record[bkey] = pgrec[bkey]
      elif BOPTIONS[bkey]:
         record[bkey] = BOPTIONS[bkey]

   return record

#
# return delay mode option to append to argv string for a specified cmd
#
def append_delayed_mode(cmd, argv):

   if cmd in DLYOPT and not re.search(DLYPTN, argv, re.I):
      return DLYOPT[cmd]
   else:
      return ''

#
# check given doptions and cmd, and return the try limit and specified hosts
#
def get_delay_options(doptions, cmd):

   mcount = 0
   hosts = None

   if doptions:
      for bval in doptions:
         if re.match(r'^(\d+)$', bval):
            mcount = int(bval)
            if mcount > 99: mcount = 99
         else:
            hosts = bval

   if mcount == 0: mcount = get_try_limit(cmd)
   if hosts: set_one_boption('hostname', hosts, 1)

   return (mcount, hosts)

#
# find an existing dscheck record from the cached command argument; create and initialize one if not exist
#
def init_dscheck(oindex, otype, cmd, dsid, action, workdir = None, specialist = None, doptions = None, logact = 0):

   cidx = 0
   argv = PgLOG.argv_to_string(sys.argv[1:], 0, "Process in Delayed Mode")
   argextra = None

   if not logact: logact = PgLOG.LGEREX
   if not workdir: workdir = os.getcwd()
   if not specialist: specialist = PgLOG.PGLOG['CURUID']

   (mcount, hosts) = get_delay_options(doptions, cmd)

   if len(argv) > 100:
      argextra = argv[100:]
      argv = argv[0:100]

   bck = PgLOG.PGLOG['BCKGRND']
   PgLOG.PGLOG['BCKGRND'] = 0
   cinfo = "{}-{}-Chk".format(PgLOG.PGLOG['HOSTNAME'], PgLOG.current_datetime())
   pgrec = get_dscheck(cmd, argv, workdir, specialist, argextra, logact)   
   if pgrec:  # found existing dscheck record
      cidx = pgrec['cindex']
      cmsg = "{}{}: {} batch process ".format(cinfo, cidx, get_command_info(pgrec))
      cidx = PgLock.lock_dscheck(cidx, 1, PgLOG.LOGWRN)
      if cidx < 0:
         PgLOG.pglog(cmsg + "is Running, No restart", PgLOG.LOGWRN)
         sys.exit(0)
      if cidx > 0:
         if not hosts and pgrec['hostname']:
            hosts = pgrec['hostname']
            set_one_boption('hostname', hosts, 0)
         if mcount: pgrec['mcount'] = mcount
         DSCHK['chkcnd'] = "cindex = {}".format(cidx)
         if(pgrec['status'] == 'D' or pgrec['fcount'] and pgrec['dcount'] >= pgrec['fcount'] or
            pgrec['tcount'] > pgrec['mcount'] or not pgrec['pid'] and pgrec['tcount'] == pgrec['mcount']):
            PgLOG.pglog("{}is {}".format(cmsg, ('Done' if pgrec['status'] == 'D' else 'Finished')), PgLOG.LOGWRN)
            PgLock.lock_dscheck(cidx, 0, logact)
            sys.exit(0)

   if not cidx:  # add new dscheck record
      record = {}
      if hosts and re.match(r'^(ds\d|\d)\d\d.\d$', hosts):
         PgLOG.pglog(hosts + ": Cannot pass DSID for hostname to submit batch process", PgLOG.LGEREX)
      if oindex: set_command_control(oindex, otype, cmd, logact)
      record['oindex'] = oindex
      record['dsid'] = dsid
      record['action'] = action
      record['otype'] = otype
      (record['date'], record['time']) = PgUtil.get_date_time()
      record['command'] = cmd
      record['argv'] = argv
      if mcount > 0: record['mcount'] = mcount
      record['specialist'] = specialist
      record['workdir'] = workdir
      if argextra: record['argextra'] = argextra
      record.update(get_batch_options())
      cidx = PgDBI.pgadd("dscheck", record, logact|PgLOG.AUTOID)
      if cidx:
         cmsg = "{}{}: {} Adds a new check".format(cinfo, cidx, get_command_info(record))
         PgLOG.pglog(cmsg, PgLOG.LOGWRN)
      sys.exit(0)

   (chost, cpid) = PgLOG.current_process_info()
   (rhost, rpid) = PgLOG.current_process_info(1)

   if not check_command_specialist_host(hosts, chost, specialist, cmd, action, PgLOG.LOGERR):
      PgLock.lock_dscheck(cidx, 0, logact)
      sys.exit(1)

   record = {}
   record['status'] = "R"
   if mcount > 0: record['mcount'] = mcount
   record['bid'] = (cpid if PgLOG.PGLOG['CURBID'] else 0)
   if pgrec['stttime'] and pgrec['chktime'] > pgrec['stttime']:
      (record['ttltime'], record['quetime']) = get_dscheck_runtime(pgrec)
   record['chktime'] = record['stttime'] = int(time.time())
   if not pgrec['subtime']: record['subtime'] = record['stttime']
   if dsid and not pgrec['dsid']: record['dsid'] = dsid
   if action and not pgrec['action']: record['action'] = action
   if oindex and not pgrec['oindex']: record['oindex'] = oindex
   if otype and not pgrec['otype']: record['otype'] = otype
   if argv and not pgrec['argv']: record['argv'] = argv
   record['runhost'] = rhost
   if pgrec['command'] == "dsrqst" and pgrec['oindex']:
      (record['fcount'], record['dcount'], record['size']) = get_dsrqst_counts(pgrec, logact)
   PgDBI.pgupdt("dscheck", record, DSCHK['chkcnd'], logact)

   DSCHK['dcount'] = pgrec['dcount']
   DSCHK['fcount'] = pgrec['fcount']
   DSCHK['size'] = pgrec['size']
   DSCHK['cindex'] = cidx
   DSCHK['dflags'] = pgrec['dflags']
   PgLOG.PGLOG['DSCHECK'] = DSCHK   # add global access link
   if not PgLOG.PGLOG['BCKGRND']: PgLOG.PGLOG['BCKGRND'] = 1         # turn off screen output if not yet
   tcnt = pgrec['tcount']
   if not pgrec['pid']: tcnt += 1
   tstr = "the {} run".format(PgLOG.int2order(tcnt)) if tcnt > 1 else "running"
   pstr = "{}<{}>".format(chost, cpid)
   if rhost != chost: pstr += "/{}<{}>".format(rhost, rpid)
   PgLOG.pglog("{}Starts {} ({})".format(cmsg, tstr, pstr), PgLOG.LOGWRN)
   PgLOG.PGLOG['BCKGRND'] = bck

   return cidx

#
# check and validate if the current host is configured for the specialist
#
def check_command_specialist_host(hosts, chost, specialist, cmd, act = 0, logact = 0):

   if cmd == 'dsrqst' and act == 'PR':
      mflag = 'G'
   else:
      cnd = "command = '{}' AND specialist = '{}' AND hostname = '{}'".format(cmd, specialist, chost)
      pgrec = PgDBI.pgget("dsdaemon", 'matchhost', cnd, logact)
      mflag = (pgrec['matchhost'] if pgrec else 'G')

   return PgLOG.check_process_host(hosts, chost, mflag, "{}-{}".format(specialist, cmd), logact)

#
# set command control info
#
def set_command_control(oindex, otype, cmd, logact = 0):

   if not oindex: return

   pgctl = None
   if cmd == "dsrqst":
      if otype == 'P':
         pgrec = PgDBI.pgget("ptrqst", "rindex", "pindex = {}".format(oindex), logact)
         if pgrec: pgctl = get_partition_control(pgrec, None, None, logact)
      else:
         pgrec = PgDBI.pgget("dsrqst", "dsid, gindex, cindex, rqsttype", "rindex = {}".format(oindex), logact)
         if pgrec: pgctl = get_dsrqst_control(pgrec, logact)
   elif cmd == "dsupdt":
      if otype == 'L':
         pgrec = PgDBI.pgget("dlupdt", "cindex", "lindex = {}".format(oindex),  logact)
         if not (pgrec and pgrec['cindex']): return
         oindex = pgrec['cindex']
      pgctl = PgDBI.pgget("dcupdt", BFIELDS, "cindex = {}".format(oindex), logact)
   if pgctl:
      for bkey in pgctl:
         set_one_boption(bkey, pgctl[bkey], 0)

#
# get dsrqst control info
#
def get_dsrqst_control(pgrqst, logact = 0):

   cflds = BFIELDS
   if 'ptcount' in pgrqst and pgrqst['ptcount'] == 0: cflds += ", ptlimit, ptsize"
   if pgrqst['cindex']:
      pgctl = PgDBI.pgget("rcrqst", cflds, "cindex = {}".format(pgrqst['cindex']), logact)
   else:
      pgctl = None
   if not pgctl:
      gcnd = "dsid = '{}' AND gindex = ".format(pgrqst['dsid'])
      tcnd = " AND rqsttype = '{}'".format(pgrqst['rqsttype'])
      gindex = pgrqst['gindex']
      while True:
         pgctl = PgDBI.pgget("rcrqst", cflds, "{}{}{}".format(gcnd, gindex, tcnd), logact)
         if pgctl or not gindex: break
         pgctl = PgDBI.pgget("dsgroup", "pindex", "{}{}".format(gcnd, gindex), logact)
         if not pgctl: break
         gindex = pgctl['pindex']

   return pgctl

#
# get dsrqst partition control info
#
def get_partition_control(pgpart, pgrqst = None, pgctl = None, logact = 0):

   if not pgctl:
      if not pgrqst and pgpart['rindex']:
         pgrqst = PgDBI.pgget("dsrqst", "dsid, gindex, cindex, rqsttype", "rindex = {}".format(pgpart['rindex']), logact)
      if pgrqst: pgctl = get_dsrqst_control(pgrqst, logact)

   return pgctl

#
# build the dynamic options
#
def get_dynamic_options(cmd, oindex, otype):

   if oindex: cmd +=  " {}".format(oindex)
   if otype: cmd +=  ' ' + otype
   ret = options = ''
   for loop in range(3):
      ret = PgLOG.pgsystem(cmd, PgLOG.LOGWRN, 279)  # 1+2+4+16+256
      if loop < 2 and PgLOG.PGLOG['SYSERR'] and 'Connection timed out' in PgLOG.PGLOG['SYSERR']:
         time.sleep(PgSIG.PGSIG['ETIME'])
      else:
         break   
   if ret:
      ret = ret.strip()
      ms = re.match(r'^(-.+)/(-.+)$', ret)
      if ms:
         options = ms.group(1) if otype == 'R' else ms.group(2)
      elif re.match(r'^(-.+)$', ret):
         options = ret
   if not options:
      if ret: PgLOG.PGLOG['SYSERR'] += ret
      PgLOG.PGLOG['SYSERR'] += " for {}".format(cmd)

   return options

#
# retrieve a dscheck record for provided cmd, argv and other conditions
#
def get_dscheck(cmd, argv, workdir, specialist, argextra = None, logact = 0):

   cnd = "command = '{}' AND specialist = '{}' AND argv = '{}'".format(cmd, specialist, argv)
   pgrecs = PgDBI.pgmget("dscheck", "*", cnd, logact)
   cnt = len(pgrecs['cindex']) if pgrecs else 0
   if cnt == 0 and cmd in DLYOPT:
      ms = re.match(r'^(.+){}$'.format(DLYOPT[cmd]), argv)
      if ms:
         argv = ms.group(1)
         cnt = 1
      elif not argextra:
         dopt = append_delayed_mode(cmd, argv)
         if dopt:
            argv += dopt
            cnt = 1
      if cnt:
         cnd = "command = '{}' AND specialist = '{}' AND argv = '{}'".format(cmd, specialist, argv)
         pgrecs = PgDBI.pgmget("dscheck", "*", cnd, logact)
         cnt = len(pgrecs['cindex']) if pgrecs else 0

   for i in range(cnt):
      pgrec = PgUtil.onerecord(pgrecs, i)
      if pgrec['workdir'] and PgUtil.pgcmp(workdir, pgrec['workdir']): continue
      if PgUtil.pgcmp(argextra, pgrec['argextra']): continue 
      return pgrec

   return None

#
# delete one dsceck record fo given cindex
#
def delete_dscheck(pgrec, chkcnd, logact = 0):

   if not chkcnd:
      if pgrec:
         chkcnd = "cindex = {}".format(pgrec['cindex'])
      elif 'chkcnd' in DSCHK:
         chkcnd = DSCHK['chkcnd']
      else:
         return 0   # nothing to delete
   if not pgrec:
      pgrec = PgDBI.pgget("dscheck", "*", chkcnd, logact)
      if not pgrec: return 0          # dscheck record is gone

   record = {}
   record['cindex'] = pgrec['cindex']
   record['command'] = pgrec['command']
   record['dsid'] = (pgrec['dsid'] if pgrec['dsid'] else PgLOG.PGLOG['DEFDSID'])
   record['action'] = (pgrec['action'] if pgrec['action'] else "UN")
   record['specialist'] = pgrec['specialist']
   record['hostname'] = pgrec['runhost']
   if pgrec['bid']: record['bid'] = pgrec['bid']
   if pgrec['command'] == "dsrqst" and pgrec['oindex']:
      (record['fcount'], record['dcount'], record['size']) = get_dsrqst_counts(pgrec, logact)
   else:
      record['fcount'] = pgrec['fcount']
      record['dcount'] = pgrec['dcount']
      record['size'] = pgrec['size']
   record['tcount'] = pgrec['tcount']
   record['date'] = pgrec['date']
   record['time'] = pgrec['time']
   record['closetime'] = PgUtil.curtime(1)
   (record['ttltime'], record['quetime']) = get_dscheck_runtime(pgrec)
   record['argv'] = pgrec['argv']
   if pgrec['argextra']:
      record['argv'] += pgrec['argextra']
      if len(record['argv']) > 255: record['argv'] = record['argv'][0:255]
   if pgrec['errmsg']: record['errmsg'] = pgrec['errmsg']
   record['status'] = ('F' if pgrec['status'] == "R" else pgrec['status'])

   if PgDBI.pgget("dschkhist", "", chkcnd):
      stat = PgDBI.pgupdt("dschkhist", record, chkcnd, logact)
   else:
      stat = PgDBI.pgadd("dschkhist", record, logact)      
   if stat:
      cmsg = "{} cleaned as '{}' at {} on {}".format(get_command_info(pgrec), record['status'], PgLOG.current_datetime(), PgLOG.PGLOG['HOSTNAME'])
      PgLOG.pglog("Chk{}: {}".format(pgrec['cindex'], cmsg), PgLOG.LOGWRN|PgLOG.FRCLOG)
      stat = PgDBI.pgdel("dscheck", chkcnd, logact)
   if record['status'] == "E" and 'errmsg' in record:
      PgLOG.pglog("Chk{}: {} Exits with Error\n{}".format(pgrec['cindex'], get_command_info(pgrec), record['errmsg']), logact)

   return stat

#
# get dsrqst fcount and dcount
#
def get_dsrqst_counts(pgchk, logact = 0):

   fcount = pgchk['fcount']
   dcount = pgchk['dcount']
   size = pgchk['size']

   if pgchk['otype'] == 'P':
      table = 'ptrqst'
      cnd = "pindex = {}".format(pgchk['oindex'])
      fields = "fcount"
   else:
      table = 'dsrqst'
      cnd = "rindex = {}".format(pgchk['oindex'])
      fields = "fcount, pcount, size_input, size_request"
   pgrec = PgDBI.pgget(table, fields, cnd, logact)
   if pgrec:
      fcnt = pgrec['fcount']
   else:
      fcnt = 0
      pgrec = {'fcount' : 0}
   if not fcnt: fcnt = PgDBI.pgget("wfrqst", "", cnd, logact)
   if fcnt and fcount != fcnt: fcount = fcnt
   if fcount:
      if 'pcount' in pgrec and pgrec['pcount']:
         dcnt = pgrec['pcount']
      else:
         dcnt = PgDBI.pgget("wfrqst", "", cnd + " AND status = 'O'", logact)
      if dcnt and dcnt != dcount: dcount = dcnt
   if not size:
      if 'size_input' in pgrec and pgrec['size_input']:
         if size != pgrec['size_input']: size = pgrec['size_input']
      elif 'size_request' in pgrec and pgrec['size_request']:
         if size != pgrec['size_request']: size = pgrec['size_request']
      elif fcnt:    # evaluate total size only if file count is set in request/partition record
         pgrec = PgDBI.pgget("wfrqst", "sum(size) data_size", cnd, logact)
         if pgrec and pgrec['data_size']: size = pgrec['data_size']

   return (fcount, dcount, size)

#
# set dscheck fcount
#
def set_dscheck_fcount(count, logact = 0):
   
   record = {'fcount' : count, 'chktime' : int(time.time())}
   PgDBI.pgupdt("dscheck", record, DSCHK['chkcnd'], logact)
   DSCHK['fcount'] = count

   return DSCHK['dcount']     # return Done count

#
# set dscheck dcount
#
def set_dscheck_dcount(count, size, logact = 0):

   record = {'dcount' : count, 'size' : size, 'chktime' : int(time.time())}
   PgDBI.pgupdt("dscheck", record, DSCHK['chkcnd'], logact)
   DSCHK['dcount'] = count
   DSCHK['size'] = size

   return DSCHK['dcount']     # return Done count

#
# add dscheck dcount
#
def add_dscheck_dcount(count, size, logact = 0):

   record = {}
   if count:
      DSCHK['dcount'] += count
      record['dcount'] = DSCHK['dcount']
   if size:
      DSCHK['size'] += size
      record['size'] = DSCHK['size']
   record['chktime'] = int(time.time())
   PgDBI.pgupdt("dscheck", record, DSCHK['chkcnd'], logact)

   return DSCHK['dcount']     # return Done count

#
# set dscheck source information
#
def set_dscheck_attribute(fname, value, logact = 0):

   record = {}
   if value: record[fname] = value
   record['chktime'] = int(time.time())
   PgDBI.pgupdt("dscheck", record, DSCHK['chkcnd'], logact)

#
# update dscheck status
#
def record_dscheck_status(stat, logact = 0):

   pgrec = PgDBI.pgget("dscheck", "lockhost, pid", DSCHK['chkcnd'], logact)
   if not pgrec: return 0
   if not (pgrec['pid'] and pgrec['lockhost']): return 0
   (chost, cpid) = PgLOG.current_process_info()
   if pgrec['pid'] != cpid or pgrec['lockhost'] != chost: return 0

   # update dscheck status only if it is still locked by the current process
   record = {'status' : stat, 'chktime' : int(time.time()), 'pid' : 0}
   return PgDBI.pgupdt("dscheck", record, DSCHK['chkcnd'], logact)

#
# get the number of tries to execute for a given cmd under dscheck control
#
def get_try_limit(cmd):

   return (TRYLMTS[cmd] if cmd in TRYLMTS else TRYLMTS['default'])

#
# get the execution time for a dscheck command
#
def get_dscheck_runtime(pgrec, current = 0):

   ttime = (0 if current else pgrec['ttltime'])
   qtime = (0 if current else pgrec['quetime'])

#   if pgrec['bid'] and PgLOG.PGLOG['CURBID']:
#      if PgLOG.PGLOG['PGBATCH'] == PgLOG.PGLOG['SLMNAME']:
#         stat = PgSIG.check_slurm_status(pgrec['bid'], PgLOG.LOGERR)
#         if stat:
#            if stat['PEND']: qtime += stat['PEND']
#            if stat['TOTAL']: ttime += stat['TOTAL']
#            return (ttime, qtime)

   if pgrec['subtime']:
      ttime += (pgrec['chktime'] - pgrec['subtime'])
      if pgrec['stttime']: qtime += (pgrec['stttime'] - pgrec['subtime'])

   return (ttime, qtime)

#
# retrieve a command string from a given dscheck record
#
def get_command_info(pgrec):

   if pgrec['oindex']:
      if pgrec['command'] == "dsupdt":
         cinfo = "UC{}".format(pgrec['oindex'])
      elif pgrec['command'] == "dsrqst":
         if pgrec['otype'] == "P":
            cinfo = "RPT{}".format(pgrec['oindex'])
         else:
            cinfo = "Rqst{}".format(pgrec['oindex'])
      else:
         cinfo ="{}-{}".format(pgrec['command'], pgrec['oindex'])
   else:
      cinfo =pgrec['command']
   if pgrec['dsid']: cinfo += " " + pgrec['dsid']
   if pgrec['action']: cinfo += " " + pgrec['action']
   cinfo += " of " + pgrec['specialist']

   return cinfo

#
# change the dscheck original command information
#
def change_dscheck_oinfo(oidx, otype, nidx, ntype):
   
   cnd = "oindex = {} AND otype = '{}'".format(oidx, otype)
   pgchk = PgDBI.pgget('dscheck', 'cindex, oindex, otype', cnd, PgLOG.LGEREX)
   if not pgchk: return 0    # miss dscheck record to change

   record = {}
   DSCHK['oindex'] = record['oindex'] = nidx
   DSCHK['otype'] = record['otype'] = ntype
   cnd = "cindex = {}".format(pgchk['cindex'])
   return PgDBI.pgupdt('dscheck', record, cnd, PgLOG.LGEREX)
