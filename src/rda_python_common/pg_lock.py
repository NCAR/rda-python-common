###############################################################################
#     Title: pg_lock.py
#    Author: Zaihua Ji,  zji@ucar.edu
#      Date: 08/118/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-12-01 convert to class PgLock
#   Purpose: python library module for functions to lock RDADB records
#    Github: https://github.com/NCAR/rda-python-common.git
###############################################################################
import re
import time
from .pg_file import PgFile

class PgLock(PgFile):

   def __init__(self):
      super().__init__()  # initialize parent class
      self.DOLOCKS = {-2: 'Force Unlock', -1: 'Unlock', 0: 'Unlock', 1: 'Relock', 2: 'Force Relock'}
   
   def end_db_transaction(self, idx):
      if idx > 0:
         self.endtran()
      else:
         self.aborttran()
      return idx

   # check and return running process status: 1-running/uncheckable,0-stopped
   def check_process_running_status(self, host, pid, dolock, lmsg, logact):
      if not self.local_host_action(host, self.DOLOCKS[dolock], lmsg, logact): return 1
      stat = self.check_host_pid(host, pid)
      if stat > 0:
         if logact: self.pglog("{}: Cannot {}".format(lmsg, self.DOLOCKS[dolock]), logact)
         return 1
      if stat < 0 and dolock > -2 and dolock < 2:
         if logact: self.pglog("{}: Fail checking lock info to {}".format(lmsg, self.DOLOCKS[dolock]), logact)
         return 1
      return 0

   # lock/unlock dscheck record
   # lock if dolock > 0, unlock if <= 0, skip for locked on different host if 0 or 1
   # force unlock if < -1 or force lock if 2
   def lock_dscheck(self, cidx, dolock, logact = 0):
      if not cidx: return 0
      if logact:
         logerr = logact|self.ERRLOG
         logout = logact&(~self.EXITLG)
      else:
         logerr = self.LOGERR
         logout = self.LOGWRN if dolock > 1 or dolock < 0 else 0
      table = "dscheck"
      cnd = "cindex = {}".format(cidx)
      fields = "command, pid, lockhost, lockcmd"
      pgrec = self.pgget(table, fields, cnd, logerr)
      if not pgrec: return 0   # dscheck is gone or db error
      pid = pgrec['pid']
      host = pgrec['lockhost']
      lockcmd = pgrec['lockcmd']
      (chost, cpid) = self.current_process_info()
      clockcmd = self.get_command()
      if pid == 0 and dolock <= 0: return cidx   # no need unlock
      lckpid = -pid if pid > 0 and pid == cpid and not self.pgcmp(host, chost, 1) else pid
      if dolock > 0 and lckpid < 0: return cidx   # no need lock again
      cinfo = "{}-{}-Chk{}({})".format(self.PGLOG['HOSTNAME'], self.current_datetime(), cidx, pgrec['command'])
      if lckpid > 0 and (clockcmd == "dscheck" or lockcmd != "dscheck"):
         lmsg = "{} Locked by {}/{}/{}".format(cinfo, pid, host, lockcmd)
         if self.check_process_running_status(host, pid, dolock, lmsg, logout): return -cidx
      record = {}
      if dolock > 0:
         if pid != cpid: record['pid'] = cpid
         if host != chost: record['lockhost'] = chost
         if lockcmd != clockcmd: record['lockcmd'] = clockcmd
      else:
         if pid: record['pid'] = 0
      if not record: return cidx
      lkrec = self.pgget(table, fields, cnd, logerr|self.DOLOCK)
      if not lkrec: return self.end_db_transaction(0)   # dscheck is gone or db error
      if (not lkrec['pid'] or 
          lkrec['pid'] == pid and self.pgcmp(lkrec['lockhost'], host, 1) == 0 or
          lkrec['pid'] == cpid and self.pgcmp(lkrec['lockhost'], chost, 1) == 0):
         if not self.pgupdt(table, record, cnd, logerr):
            if logout: self.pglog(cinfo + ": Error update lock", logout)
            cidx = -cidx
      else:
         if logout: self.pglog("{}: Relocked {}/{}".format(cinfo, lkrec['pid'], lkrec['lockhost']), logout)
         cidx = -cidx
      return self.end_db_transaction(cidx)

   # lock dscheck record for given cidx, pid and host 
   def lock_host_dscheck(self, cidx, pid, host, logact = 0):
      if not (cidx and pid): return 0
      if logact:
         logerr = logact|self.ERRLOG
         logout = logact&(~self.EXITLG)
      else:
         logerr = self.LOGERR
         logout = 0
      table = "dscheck"
      cnd = "cindex = {}".format(cidx)
      fields = "command, pid, lockhost, lockcmd"
      pgrec = self.pgget(table, fields, cnd, logerr)
      if not pgrec: return 0    # dscheck is gone or db error
      (chost, cpid) = self.current_process_info()
      cinfo = "{}-{}-Chk{}({})".format(self.PGLOG['HOSTNAME'], self.current_datetime(), cidx, pgrec['command'])
      if pgrec['pid']:
         if pid == pgrec['pid'] and self.pgcmp(pgrec['lockhost'], host, 1) == 0:
            return -cidx   # locked by the real process already
         elif cpid != pgrec['pid'] or self.pgcmp(pgrec['lockhost'], chost, 1):
            if logout:
               lmsg = "{} Locked by {}/{}/{}".format(cinfo, pid, host, pgrec['lockcmd'])
               self.pglog(lmsg +": Cannot Lock", logout)
            return -cidx   # locked by other process
      record = {}
      record['pid'] = pid
      record['lockhost'] = host
      record['lockcmd'] = self.get_command(pgrec['command'])
      lkrec = self.pgget(table, fields, cnd, logerr|self.DOLOCK)
      if not lkrec: return self.end_db_transaction(0)
      if (not lkrec['pid'] or
          lkrec['pid'] == pid and self.pgcmp(lkrec['lockhost'], host, 1) == 0 or
          lkrec['pid'] == cpid and self.pgcmp(lkrec['lockhost'], chost, 1) == 0):
         if not self.pgupdt(table, record, cnd, logerr):
            if logout: self.pglog(cinfo + ": Error update lock", logout)
            cidx = -cidx
      else:
         if logout: self.pglog("{}: Relocked {}/{}".format(cinfo, lkrec['pid'], lkrec['lockhost']), logout)
         cidx = -cidx
      return self.end_db_transaction(cidx)

   # lock/unlock data request record
   # lock if dolock > 0, unlock if <= 0, skip for locked on different host if 0 or 1
   # force unlock if < -1 or 2 
   def lock_request(self, ridx, dolock, logact = 0):
      if not ridx: return 0
      if logact:
         logerr = logact|self.ERRLOG
         logout = logact&(~self.EXITLG)
      else:
         logerr = self.LOGERR
         logout = self.LOGWRN if dolock > 1 or dolock < 0 else 0
      table = "dsrqst"
      cnd = "rindex = {}".format(ridx)
      fields = "pid, lockhost"
      pgrec = self.pgget(table, fields, cnd, logerr)
      if not pgrec: return 0   # request is gone or db error
      pid = pgrec['pid']
      host = pgrec['lockhost']
      (chost, cpid) = self.current_process_info()
      if pid == 0 and dolock <= 0: return ridx   # no need unlock
      lckpid = -pid if pid > 0 and pid == cpid and not self.pgcmp(host, chost, 1) else pid
      if dolock > 0 and lckpid < 0: return ridx    # no need lock again
      rinfo = "{}-{}-Rqst{}".format(self.PGLOG['HOSTNAME'], self.current_datetime(), ridx)
      if lckpid > 0:
         lmsg = "{} Locked by {}/{}".format(rinfo, pid, host)
         if self.check_process_running_status(host, pid, dolock, lmsg, logout): return -ridx
      record = {}
      if dolock > 0:
         if pid != cpid: record['pid'] = cpid
         if host != chost: record['lockhost'] = chost
         if record: record['locktime'] = int(time.time())
      else:
         if pid: record['pid'] = 0
         if host: record['lockhost'] = ""
      if not record: return ridx
      lkrec = self.pgget(table, fields, cnd, logerr|self.DOLOCK)
      if not lkrec: return self.end_db_transaction(0)   # request is gone or db error
      if (not lkrec['pid']  or
          lkrec['pid'] == pid and self.pgcmp(lkrec['lockhost'], host, 1) == 0 or
          lkrec['pid'] == cpid and self.pgcmp(lkrec['lockhost'], chost, 1) == 0):
         if not self.pgupdt(table, record, cnd, logerr):
            if logout: self.pglog(rinfo + ": Error update lock", logout)
            ridx = -ridx
      else:
         if logout: self.pglog("{}: Relocked {}/{}".format(rinfo, lkrec['pid'], lkrec['lockhost']), logout)
         ridx = -ridx
      return self.end_db_transaction(ridx)

   # lock dsrqst record for given cidx, pid and host 
   def lock_host_request(self, ridx, pid, host, logact = 0):
      if not (ridx and pid): return 0
      if logact:
         logerr = logact|self.ERRLOG
         logout = logact&(~self.EXITLG)
      else:
         logerr = self.LOGERR
         logout = 0
      table = "dsrqst"
      cnd = "rindex = {}".format(ridx)
      fields = "pid, lockhost"
      pgrec = self.pgget(table, fields, cnd, logerr)
      if not pgrec: return 0   # dscheck is gone or db error
      rinfo = "{}-{}-Rqst{}".format(self.PGLOG['HOSTNAME'], self.current_datetime(), ridx)
      if pgrec['pid']:
         if pid == pgrec['pid'] and self.pgcmp(pgrec['lockhost'], host, 1) == 0: return ridx
         if logout:
            lmsg = "{} Locked by {}/{}".format(rinfo, pid, host)
            self.pglog(lmsg +": Cannot Lock", logout)
         return -ridx
      record = {}
      record['pid'] = pid
      record['lockhost'] = host
      record['locktime'] = int(time.time())
      pgrec = self.pgget(table, fields, cnd, logerr|self.DOLOCK)
      if not pgrec: return self.end_db_transaction(0)
      if not pgrec['pid'] or pid == pgrec['pid'] and self.pgcmp(pgrec['lockhost'], host, 1) == 0:
         if not self.pgupdt(table, record, cnd, logerr):
            if logout: self.pglog(rinfo + ": Error update lock", logout)
            ridx = -ridx
      else:
         if logout: self.pglog("{}: Relocked {}/{}".format(rinfo, pgrec['pid'], pgrec['lockhost']), logout)
         ridx = -ridx
      return self.end_db_transaction(ridx)

   # lock/unlock dataset update record
   # lock if dolock > 0, unlock if <= 0, skip for locked on different host if 0 or 1
   # force unlock if < -1 or 2 
   def lock_update(self, lidx, linfo, dolock, logact = 0):
      if not lidx: return 0
      if logact:
         logerr = logact|self.ERRLOG
         logout = logact&(~self.EXITLG)
      else:
         logerr = self.LOGERR
         logout = self.LOGWRN if dolock > 1 or dolock < 0 else 0
      table = "dlupdt"
      cnd = "lindex = {}".format(lidx)
      fields = "pid, hostname"
      pgrec = self.pgget(table, fields, cnd, logerr)
      if not pgrec: return 0   # update record is deleted
      pid = pgrec['pid']
      host = pgrec['hostname']
      (chost, cpid) = self.current_process_info()
      if pid == 0 and dolock <= 0: return lidx   # no need unlock
      lckpid = -pid if pid > 0 and pid == cpid and not self.pgcmp(host, chost, 1) else pid
      if dolock > 0 and lckpid < 0: return lidx   # no need lock again
      if not linfo: linfo = "{}-{}-Updt{}".format(self.PGLOG['HOSTNAME'], self.current_datetime(), lidx)
      if lckpid > 0:
         lmsg = "{} Locked by {}/{}".format(linfo, pid, host)
         if self.check_process_running_status(host, pid, dolock, lmsg, logout): return -lidx
      record = {}
      if dolock > 0:
         if pid != cpid: record['pid'] = cpid
         if host != chost: record['hostname'] = chost
         if record: record['locktime'] = int(time.time())
      else:
         if pid: record['pid'] = 0
         if host: record['hostname'] = ''
      if not record: return lidx
      lkrec = self.pgget(table, fields, cnd, logerr|self.DOLOCK)
      if not lkrec: return self.end_db_transaction(0)   # update record is deleted
      if not lkrec['pid'] or lkrec['pid'] == pid and self.pgcmp(lkrec['hostname'], host, 1) == 0:
         if not self.pgupdt(table, record, cnd, logerr):
            if logout: self.pglog(linfo + ": Error update lock", logout)
            lidx = -lidx
      else:
         if logout: self.pglog("{}: Relocked {}/{}".format(linfo, lkrec['pid'], lkrec['hostname']), logout)
         lidx = -lidx
      return self.end_db_transaction(lidx)

   # lock/unlock dataset update control record
   # lock if dolock > 0, unlock if <= 0, skip for locked on different host if 0 or 1,
   #  unlock dead process if < -1 or 2, force unlock if -2
   def lock_update_control(self, cidx, dolock, logact = 0):
      if not cidx: return 0
      if logact:
         logerr = logact|self.ERRLOG
         logout = logact&(~self.EXITLG)
      else:
         logerr = self.LOGERR
         logout = self.LOGWRN if dolock > 1 or dolock < 0 else 0
      table = "dcupdt"
      cnd = "cindex = {}".format(cidx)
      fields = "pid, lockhost"
      pgrec = self.pgget(table, fields, cnd, logerr)
      if not pgrec: return 0   # update control record is deleted
      pid = pgrec['pid']
      host = pgrec['lockhost']
      (chost, cpid) = self.current_process_info()
      if pid == 0 and dolock <= 0: return cidx  # no need unlock
      lckpid = -pid if pid > 0 and pid == cpid and not self.pgcmp(host, chost, 1) else pid
      if dolock > 0 and lckpid < 0: return cidx   # no need lock again
      cinfo = "{}-{}-UC{}".format(self.PGLOG['HOSTNAME'], self.current_datetime(), cidx)
      if lckpid > 0:
         lmsg = "{} Locked by {}/{}".format(cinfo, pid, host)
         if self.check_process_running_status(host, pid, dolock, lmsg, logout): return -cidx
      record = {}
      if dolock > 0:
         if pid != cpid: record['pid'] = cpid
         if host != chost: record['lockhost'] = chost
         if record: record['chktime'] = int(time.time())
      else:
         if pid: record['pid'] = 0
         if host: record['lockhost'] = ''
      if not record: return cidx
      lkrec = self.pgget(table, fields, cnd, logerr|self.DOLOCK)
      if not lkrec: return self.end_db_transaction(0)   # update control record is deleted
      if (not lkrec['pid'] or
          lkrec['pid'] == pid and self.pgcmp(lkrec['lockhost'], host, 1) == 0 or
          lkrec['pid'] == cpid and self.pgcmp(lkrec['lockhost'], chost, 1) == 0):
         if not self.pgupdt(table, record, cnd, logerr):
            if logout: self.pglog(cinfo + ": Error update lock", logout)
            cidx = -cidx
      else:
         if logout: self.pglog("{}: Relocked {}/{}".format(cinfo, lkrec['pid'], lkrec['lockhost']), logout)
         cidx = -cidx
      return self.end_db_transaction(cidx)

   # lock dscheck record for given cidx, pid and host 
   def lock_host_update_control(self, cidx, pid, host, logact = 0):
      if not (cidx and pid): return 0
      if logact:
         logerr = logact|self.ERRLOG
         logout = logact&(~self.EXITLG)
      else:
         logerr = self.LOGERR
         logout = 0
      table = "dcupdt"
      cnd = "cindex = {}".format(cidx)
      fields = "pid, lockhost"
      pgrec = self.pgget(table, fields, cnd, logerr)
      if not pgrec: return 0   # dscheck is gone or db error
      cinfo = "{}-{}-UC{}".format(self.PGLOG['HOSTNAME'], self.current_datetime(), cidx)
      if pgrec['pid']:
         if pid == pgrec['pid'] and self.pgcmp(pgrec['lockhost'], host, 1) == 0: return cidx
         if logout:
            lmsg = "{} Locked by {}/{}".format(cinfo, pid, host)
            self.pglog(lmsg +": Cannot Lock", logout)
         return -cidx
      record = {}
      record['pid'] = pid
      record['lockhost'] = host
      record['chktime'] = int(time.time())
      pgrec = self.pgget(table, fields, cnd, logerr|self.DOLOCK)
      if not pgrec: return self.end_db_transaction(0)
      if not pgrec['pid'] or pid == pgrec['pid'] and self.pgcmp(pgrec['lockhost'], host, 1) == 0:
         if not self.pgupdt(table, record, cnd, logerr):
            if logout: self.pglog(cinfo + ": Error update lock", logout)
            cidx = -cidx
      else:
         if logout: self.pglog("{}: Relocked {}/{}".format(cinfo, pgrec['pid'], pgrec['lockhost']), logout)
         cidx = -cidx
      return self.end_db_transaction(cidx)
   
   # return lock information of a locked process
   @staticmethod
   def lock_process_info(pid, lockhost, runhost = None, pcnt = 0):
      retstr = " {}<{}".format(lockhost, pid)
      if pcnt: retstr += "/{}".format(pcnt)
      retstr += ">"
      if runhost and runhost != lockhost: retstr += '/' + runhost
      return retstr

   # lock/unlock data request partition record
   # lock if dolock > 0, unlock if <= 0, skip for locked on different host if 0 or 1
   # force unlock if < -1 or 2
   def lock_partition(self, pidx, dolock, logact = 0):
      if not pidx: return 0
      if logact:
         logerr = logact|self.ERRLOG
         logout = logact&(~self.EXITLG)
      else:
         logerr = self.LOGERR
         logout = self.LOGWRN if dolock > 1 or dolock < 0 else 0
      table = "ptrqst"
      cnd = "pindex = {}".format(pidx)
      fields = "pid, lockhost"
      pgrec = self.pgget(table, "rindex, ptorder, " + fields, cnd, logerr)
      if not pgrec: return 0   # request is gone or db error
      ridx = pgrec['rindex']
      pid = pgrec['pid']
      host = pgrec['lockhost']
      (chost, cpid) = self.current_process_info()
      if pid == 0 and dolock <= 0: return pidx   # no need unlock
      lckpid = -pid if pid > 0 and pid == cpid and not self.pgcmp(host, chost, 1) else pid
      if dolock > 0 and lckpid < 0: return pidx   # no need lock again
      pinfo = "{}-{}-RPT{}(Rqst{}/PTO{})".format(self.PGLOG['HOSTNAME'], self.current_datetime(), pidx, ridx, pgrec['ptorder'])
      if lckpid > 0:
         lmsg = "{} Locked by {}/{}".format(pinfo, pid, host)
         if self.check_process_running_status(host, pid, dolock, lmsg, logout): return -pidx
      record = {}
      if dolock > 0:
         if pid != cpid: record['pid'] = cpid
         if host != chost: record['lockhost'] = chost
         if record: record['locktime'] = int(time.time())
      else:
         if pid: record['pid'] = 0
         if host: record['lockhost'] = ""
      if not record: return pidx
      lkrec = self.pgget(table, fields, cnd, logerr|self.DOLOCK)
      if not lkrec: return self.end_db_transaction(0)   # request partition is gone or db error
      if (not lkrec['pid'] or
          lkrec['pid'] == pid and self.pgcmp(lkrec['lockhost'], host, 1) == 0 or
          lkrec['pid'] == cpid and self.pgcmp(lkrec['lockhost'], chost, 1) == 0):
         lmsg = self.update_partition_lock(ridx, record, logout)
         if lmsg:
            if logout: self.pglog("{}: {}".format(pinfo, lmsg), logout)
            pidx = -pidx
         elif not self.pgupdt(table, record, cnd, logerr):
            if logout: self.pglog(pinfo + ": error update lock", logout)
            pidx = -pidx
      else:
         self.pglog("{}: Relocked {}/{}".format(pinfo, lkrec['pid'], lkrec['lockhost']), logout)
         pidx = -pidx
      return self.end_db_transaction(pidx)

   # lock dsrqst partition record for given cidx, pid and host 
   def lock_host_partition(self, pidx, pid, host, logact = 0):
      if not (pidx and pid): return 0
      if logact:
         logerr = logact|self.ERRLOG
         logout = logact&(~self.EXITLG)
      else:
         logerr = self.LOGERR
         logout = 0
      table = "ptrqst"
      cnd = "pindex = {}".format(pidx)
      fields = "pid, lockhost"
      pgrec = self.pgget(table, "rindex, ptorder, " + fields, cnd, logerr)
      if not pgrec: return 0   # dscheck is gone or db error
      ridx = pgrec['rindex']
      pinfo = "{}-{}-RPT{}(Rqst{}/PTO{})".format(self.PGLOG['HOSTNAME'], self.current_datetime(), pidx, ridx, pgrec['ptorder'])
      if pgrec['pid']:
         if pid == pgrec['pid'] and self.pgcmp(pgrec['lockhost'], host, 1) == 0: return pidx
         if logout:
            lmsg = "{} Locked by {}/{}".format(pinfo, pid, host)
            self.pglog(lmsg +": Cannot Lock", logout)
         return -pidx
      record = {}
      record['pid'] = pid
      record['lockhost'] = host
      record['locktime'] = int(time.time())
      pgrec = self.pgget(table, fields, cnd, logerr|self.DOLOCK)
      if not pgrec: return self.end_db_transaction(0)
      if not pgrec['pid']  or pid == pgrec['pid'] and self.pgcmp(pgrec['lockhost'], host, 1) == 0:
         lmsg = self.update_partition_lock(ridx, record, logout)
         if lmsg:
            if logout: self.pglog("{}: {}".format(pinfo, lmsg), logout)
            pidx = -pidx
         elif not self.pgupdt(table, record, cnd, logerr):
            if logout: self.pglog(pinfo + ": error update lock", logout)
            pidx = -pidx
      else:
         if logout: self.pglog("{}: Relocked {}/{}".format(pinfo, pgrec['pid'], pgrec['lockhost']), logout)
         pidx = -pidx
      return self.end_db_transaction(pidx)

   # update dsrqst lock info for given partition lock status
   # Return None if all is fine; error message otherwise
   def update_partition_lock(self, ridx, ptrec, logact = 0):
      if not ridx: return 0
      if logact:
         logerr = logact|self.ERRLOG
         logout = logact&(~self.EXITLG)
      else:
         logerr = self.LOGERR
         logout = self.LOGWRN
      table = "dsrqst"
      lockhost = "partition"
      cnd = "rindex = {}".format(ridx)
      pgrec = self.pgget(table, "pid, lockhost", cnd, logact|self.DOLOCK)
      if not pgrec: return "Error get Rqst{} record".format(ridx)   # should not happen
      if pgrec['pid'] > 0 and pgrec['lockhost'] != lockhost:
         return "Rqst{} locked by non-lockhost process ({}/{})".format(ridx, pgrec['pid'], pgrec['lockhost'])      
      record = {}
      if ptrec['pid'] > 0:
         record['pid'] = pgrec['pid'] + 1
         record['lockhost'] = lockhost
         record['locktime'] = ptrec['locktime']
      else:
         if pgrec['pid'] > 1:
            pcnt = self.pgget('ptrqst', '', cnd + " AND pid > 0")
            if pgrec['pid'] > pcnt: pgrec['pid'] = pcnt
            record['pid'] = pgrec['pid'] - 1
            record['lockhost'] = lockhost
         else:
            record['pid'] = 0
            record['lockhost'] = ''
      if not self.pgupdt(table, record, cnd, logact):
         return "Error update Rqst{} lock".format(ridx)
      return None

   # lock/unlock dataset record for Quasar Backup
   # lock if dolock > 0, unlock if <= 0, skip for locked on different host if 0 or 1,
   #  unlock dead process if < -1 or 2, force unlock if -2
   def lock_dataset(self, dsid, dolock, logact = 0):
      if not dsid: return 0
      if logact:
         logerr = logact|self.ERRLOG
         logout = logact&(~self.EXITLG)
      else:
         logerr = self.LOGERR
         logout = self.LOGWRN if dolock > 1 or dolock < 0 else 0
      table = "dataset"
      cnd = "dsid = '{}'".format(dsid)
      fields = "pid, lockhost"
      pgrec = self.pgget(table, fields, cnd, logerr)
      if not pgrec: return 0   # dataset not exists
      pid = pgrec['pid']
      host = pgrec['lockhost']
      (chost, cpid) = self.current_process_info()
      if pid == 0 and dolock <= 0: return 1  # no need unlock
      lckpid = -pid if pid > 0 and pid == cpid and not self.pgcmp(host, chost, 1) else pid
      if dolock > 0 and lckpid < 0: return 1   # no need lock again
      dinfo = "{}-{}-{}".format(self.PGLOG['HOSTNAME'], self.current_datetime(), dsid)
      if lckpid > 0:
         lmsg = "{} Locked by {}/{}".format(dinfo, pid, host)
         if self.check_process_running_status(host, pid, dolock, lmsg, logout): return -1
      record = {}
      if dolock > 0:
         if pid != cpid: record['pid'] = cpid
         if host != chost: record['lockhost'] = chost
      else:
         if pid: record['pid'] = 0
      if not record: return 1
      lkrec = self.pgget(table, fields, cnd, logerr|self.DOLOCK)
      if not lkrec: return self.end_db_transaction(0)   # dscheck is gone or db error
      lstat = 1
      if (not lkrec['pid'] or 
          lkrec['pid'] == pid and self.pgcmp(lkrec['lockhost'], host, 1) == 0 or
          lkrec['pid'] == cpid and self.pgcmp(lkrec['lockhost'], chost, 1) == 0):
         if not self.pgupdt(table, record, cnd, logerr):
            if logout: self.pglog(dinfo + ": Error update lock", logout)
            lstat = -1
      else:
         if logout: self.pglog("{}: Relocked {}/{}".format(dinfo, lkrec['pid'], lkrec['lockhost']), logout)
         lstat = -1
      return self.end_db_transaction(lstat)
