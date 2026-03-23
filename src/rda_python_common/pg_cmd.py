###############################################################################
#     Title: pg_cmd.py
#    Author: Zaihua Ji,  zji@ucar.edu
#      Date: 08/25/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose: python library module for functions to record commands for delayed
#             mode or command recovery
#    Github: https://github.com/NCAR/rda-python-common.git
###############################################################################
import os
import re
import sys
import time
from .pg_lock import PgLock

class PgCMD(PgLock):
   """Manages batch/delayed-mode command execution records in the RDADB dscheck table.

   Allows commands to be queued, tracked across runs, and cleaned up on
   completion. Wraps the lower-level database and locking primitives provided
   by PgLock to provide a complete command-lifecycle interface.

   Attributes:
      DSCHK (dict): Cached dscheck info dict for the currently running command.
      BOPTIONS (dict): Batch options keyed by field name: hostname, qoptions,
         modules, environments.
      BFIELDS (str): Comma-joined string of BOPTIONS keys for use in SQL
         SELECT clauses.
      TRYLMTS (dict): Per-command retry limits, with a 'default' fallback.
      DLYPTN (str): Compiled regex pattern string that matches the delayed-mode
         flag in an argument string.
      DLYOPT (dict): Per-command delayed-mode option strings appended to argv
         when queuing a new dscheck record.
   """

   def __init__(self):
      """Initializes PgCMD and all instance attributes."""
      super().__init__()  # initialize parent class
      # cached dscheck info
      self.DSCHK = {}
      self.BOPTIONS = {"hostname": None, "qoptions": None, "modules": None, "environments": None}
      self.BFIELDS = ', '.join(self.BOPTIONS)
      self.TRYLMTS = {
         'dsquasar': 3,
         'dsarch': 2,
         'default': 1
      }
      self.DLYPTN = r'(^|\s)-(d|BP|BatchProcess|DelayedMode)(\s|$)'
      self.DLYOPT = {
         'dsarch': ' -d',
         'dsupdt': ' -d',
         'dsrqst': ' -d'
      }

   def set_batch_options(self, params, opt, addhost=0):
      """Sets batch options from a parsed parameter dictionary.

      Args:
         params (dict): Dict holding option values, keyed by short option
            names (e.g. 'QS', 'MO', 'EV', 'HN').
         opt (int): If 2, each value in params is a list and index 0 is used;
            otherwise the value is used directly.
         addhost (int): If 1, also set the hostname option from params['HN'].
      """
      if 'QS' in params: self.BOPTIONS['qoptions'] = (params['QS'][0] if opt == 2 else params['QS'])
      if 'MO' in params: self.BOPTIONS['modules'] = (params['MO'][0] if opt == 2 else params['MO'])
      if 'EV' in params: self.BOPTIONS['environments'] = (params['EV'][0] if opt == 2 else params['EV'])
      if addhost and 'HN' in params: self.BOPTIONS['hostname'] = (params['HN'][0] if opt == 2 else params['HN'])

   def fill_batch_options(self, boptions, refresh=0, checkkey=0):
      """Fills BOPTIONS from a dict of batch option values recorded in RDADB.

      Args:
         boptions (dict): Dict mapping batch option field names to values.
         refresh (int): If 1, reset all BOPTIONS values to None before filling.
         checkkey (int): If 1, only copy keys that already exist in BOPTIONS.
      """
      if refresh:
         for bkey in self.BOPTIONS:
            self.BOPTIONS[bkey] = None   # clean the hash before filling it up
      if not boptions: return
      for bkey in boptions:
         if not checkkey or bkey in self.BOPTIONS:
            self.BOPTIONS[bkey] = boptions[bkey]

   def set_one_boption(self, bkey, bval, override=0):
      """Sets a single batch option value.

      Args:
         bkey (str): Batch option field name (must be a key in BOPTIONS).
         bval (str or None): The value to set.
         override (int): If 1, overwrite an existing value; if 0, only set
            when the option is currently unset. When bval is falsy and
            override is 1, clears the existing value.
      """
      if bval:
         if override or not (bkey in self.BOPTIONS and self.BOPTIONS[bkey]): self.BOPTIONS[bkey] = bval
      elif override and bkey in self.BOPTIONS and self.BOPTIONS[bkey]:
         self.BOPTIONS[bkey] = None

   def get_batch_options(self, pgrec=None):
      """Returns a dict of effective batch options, preferring pgrec values.

      Args:
         pgrec (dict or None): Optional existing DB record whose non-empty
            field values take precedence over the cached BOPTIONS.

      Returns:
         dict: Mapping of option field names to their effective values.
            Only fields with a non-None/non-empty value are included.
      """
      record = {}
      for bkey in self.BOPTIONS:
         if pgrec and bkey in pgrec and pgrec[bkey]:
            record[bkey] = pgrec[bkey]
         elif self.BOPTIONS[bkey]:
            record[bkey] = self.BOPTIONS[bkey]
      return record

   def append_delayed_mode(self, cmd, argv):
      """Returns the delayed-mode option string for cmd if not already in argv.

      Args:
         cmd (str): Command name to look up in DLYOPT.
         argv (str): Current argument string to check for an existing flag.

      Returns:
         str: The delayed-mode option string (e.g. ' -d') if cmd is in DLYOPT
            and no delayed-mode flag is already present; otherwise ''.
      """
      if cmd in self.DLYOPT and not re.search(self.DLYPTN, argv, re.I):
         return self.DLYOPT[cmd]
      else:
         return ''

   def get_delay_options(self, doptions, cmd):
      """Parses delay options and returns the try count and optional host.

      Args:
         doptions (list or None): List of option strings; numeric strings set
            the try count (capped at 99), non-numeric strings are treated as
            a hostname override.
         cmd (str): Command name used to look up the default try limit.

      Returns:
         tuple: A 2-tuple (mcount, hosts) where mcount (int) is the maximum
            number of tries and hosts (str or None) is the hostname override.
      """
      mcount = 0
      hosts = None
      if doptions:
         for bval in doptions:
            if re.match(r'^(\d+)$', bval):
               mcount = int(bval)
               if mcount > 99: mcount = 99
            else:
               hosts = bval
      if mcount == 0: mcount = self.get_try_limit(cmd)
      if hosts: self.set_one_boption('hostname', hosts, 1)
      return (mcount, hosts)

   def init_dscheck(self, oindex, otype, cmd, dsid, action, workdir=None, specialist=None, doptions=None, logact=0):
      """Finds or creates a dscheck record and prepares it for the current run.

      Looks up an existing dscheck record matching the current command
      invocation. If found and eligible to run, locks it and updates its
      status to 'R'. If not found, inserts a new record and exits. Exits the
      process in several terminal conditions (already running, finished,
      lock failure).

      Args:
         oindex (int): Object index (e.g. rindex for dsrqst).
         otype (str): Object type code (e.g. 'P', 'R', 'L').
         cmd (str): Command name (e.g. 'dsrqst', 'dsarch').
         dsid (str): Dataset identifier string.
         action (str): Action code for this invocation.
         workdir (str or None): Working directory; defaults to os.getcwd().
         specialist (str or None): Specialist UID; defaults to PGLOG['CURUID'].
         doptions (list or None): Delay options (try count and/or hostname).
         logact (int): Logging action flags.

      Returns:
         int: The cindex (check index) of the locked dscheck record, or
            exits the process if the record was newly created or a terminal
            condition was reached.
      """
      cidx = 0
      argv = self.argv_to_string(sys.argv[1:], 0, "Process in Delayed Mode")
      argextra = None
      if not logact: logact = self.LGEREX
      if not workdir: workdir = os.getcwd()
      if not specialist: specialist = self.PGLOG['CURUID']
      (mcount, hosts) = self.get_delay_options(doptions, cmd)
      if len(argv) > 100:
         argextra = argv[100:]
         argv = argv[0:100]
      bck = self.PGLOG['BCKGRND']
      self.PGLOG['BCKGRND'] = 0
      cinfo = "{}-{}-Chk".format(self.PGLOG['HOSTNAME'], self.current_datetime())
      pgrec = self.get_dscheck(cmd, argv, workdir, specialist, argextra, logact)
      if pgrec:  # found existing dscheck record
         cidx = pgrec['cindex']
         cmsg = "{}{}: {} batch process ".format(cinfo, cidx, self.get_command_info(pgrec))
         cidx = self.lock_dscheck(cidx, 1, self.LOGWRN)
         if cidx < 0:
            self.pglog(cmsg + "is Running, No restart", self.LOGWRN)
            sys.exit(0)
         if cidx > 0:
            if not hosts and pgrec['hostname']:
               hosts = pgrec['hostname']
               self.set_one_boption('hostname', hosts, 0)
            if mcount: pgrec['mcount'] = mcount
            self.DSCHK['chkcnd'] = "cindex = {}".format(cidx)
            if(pgrec['status'] == 'D' or pgrec['fcount'] and pgrec['dcount'] >= pgrec['fcount'] or
               pgrec['tcount'] > pgrec['mcount'] or not pgrec['pid'] and pgrec['tcount'] == pgrec['mcount']):
               self.pglog("{}is {}".format(cmsg, ('Done' if pgrec['status'] == 'D' else 'Finished')), self.LOGWRN)
               self.lock_dscheck(cidx, 0, logact)
               sys.exit(0)
      if not cidx:  # add new dscheck record
         record = {}
         if hosts and re.match(r'^\w\d\d\d\d\d\d$', hosts):
            self.pglog(hosts + ": Cannot pass DSID for hostname to submit batch process", self.LGEREX)
         if oindex: self.set_command_control(oindex, otype, cmd, logact)
         record['oindex'] = oindex
         record['dsid'] = dsid
         record['action'] = action
         record['otype'] = otype
         (record['date'], record['time']) = self.get_date_time()
         record['command'] = cmd
         record['argv'] = argv
         if mcount > 0: record['mcount'] = mcount
         record['specialist'] = specialist
         record['workdir'] = workdir
         if argextra: record['argextra'] = argextra
         record.update(self.get_batch_options())
         cidx = self.pgadd("dscheck", record, logact|self.AUTOID)
         if cidx:
            cmsg = "{}{}: {} Adds a new check".format(cinfo, cidx, self.get_command_info(record))
            self.pglog(cmsg, self.LOGWRN)
         sys.exit(0)

      (chost, cpid) = self.current_process_info()
      (rhost, rpid) = self.current_process_info(1)

      if not self.check_command_specialist_host(hosts, chost, specialist, cmd, action, self.LOGERR):
         self.lock_dscheck(cidx, 0, logact)
         sys.exit(1)

      record = {}
      record['status'] = "R"
      if mcount > 0: record['mcount'] = mcount
      record['bid'] = (cpid if self.PGLOG['CURBID'] else 0)
      if pgrec['stttime'] and pgrec['chktime'] > pgrec['stttime']:
         (record['ttltime'], record['quetime']) = self.get_dscheck_runtime(pgrec)
      record['chktime'] = record['stttime'] = int(time.time())
      if not pgrec['subtime']: record['subtime'] = record['stttime']
      if dsid and not pgrec['dsid']: record['dsid'] = dsid
      if action and not pgrec['action']: record['action'] = action
      if oindex and not pgrec['oindex']: record['oindex'] = oindex
      if otype and not pgrec['otype']: record['otype'] = otype
      if argv and not pgrec['argv']: record['argv'] = argv
      record['runhost'] = rhost
      if pgrec['command'] == "dsrqst" and pgrec['oindex']:
         (record['fcount'], record['dcount'], record['size']) = self.get_dsrqst_counts(pgrec, logact)
      self.pgupdt("dscheck", record, self.DSCHK['chkcnd'], logact)
      self.DSCHK['dcount'] = pgrec['dcount']
      self.DSCHK['fcount'] = pgrec['fcount']
      self.DSCHK['size'] = pgrec['size']
      self.DSCHK['cindex'] = cidx
      self.DSCHK['dflags'] = pgrec['dflags']
      self.PGLOG['DSCHECK'] = self.DSCHK   # add global access link
      if not self.PGLOG['BCKGRND']: self.PGLOG['BCKGRND'] = 1         # turn off screen output if not yet
      tcnt = pgrec['tcount']
      if not pgrec['pid']: tcnt += 1
      tstr = "the {} run".format(self.int2order(tcnt)) if tcnt > 1 else "running"
      pstr = "{}<{}>".format(chost, cpid)
      if rhost != chost: pstr += "/{}<{}>".format(rhost, rpid)
      self.pglog("{}Starts {} ({})".format(cmsg, tstr, pstr), self.LOGWRN)
      self.PGLOG['BCKGRND'] = bck
      return cidx

   def check_command_specialist_host(self, hosts, chost, specialist, cmd, act=0, logact=0):
      """Checks whether the current host is configured to run cmd for specialist.

      Args:
         hosts (str or None): Allowed hostname pattern or None.
         chost (str): Current host name.
         specialist (str): Specialist UID.
         cmd (str): Command name.
         act (str or int): Action code; 'PR' triggers global match flag.
         logact (int): Logging action flags.

      Returns:
         bool: True if the current host is permitted to run the command,
            False otherwise.
      """
      if cmd == 'dsrqst' and act == 'PR':
         mflag = 'G'
      else:
         cnd = "command = '{}' AND specialist = '{}' AND hostname = '{}'".format(cmd, specialist, chost)
         pgrec = self.pgget("dsdaemon", 'matchhost', cnd, logact)
         mflag = (pgrec['matchhost'] if pgrec else 'G')
      return self.check_process_host(hosts, chost, mflag, "{}-{}".format(specialist, cmd), logact)

   def set_command_control(self, oindex, otype, cmd, logact=0):
      """Retrieves and applies batch control options from the database for cmd.

      Looks up control records (e.g. rcrqst, dcupdt) associated with the
      given object and merges their batch option fields into BOPTIONS.

      Args:
         oindex (int): Object index.
         otype (str): Object type code.
         cmd (str): Command name ('dsrqst' or 'dsupdt').
         logact (int): Logging action flags.
      """
      if not oindex: return
      pgctl = None
      if cmd == "dsrqst":
         if otype == 'P':
            pgrec = self.pgget("ptrqst", "rindex", "pindex = {}".format(oindex), logact)
            if pgrec: pgctl = self.get_partition_control(pgrec, None, None, logact)
         else:
            pgrec = self.pgget("dsrqst", "dsid, gindex, cindex, rqsttype", "rindex = {}".format(oindex), logact)
            if pgrec: pgctl = self.get_dsrqst_control(pgrec, logact)
      elif cmd == "dsupdt":
         if otype == 'L':
            pgrec = self.pgget("dlupdt", "cindex", "lindex = {}".format(oindex), logact)
            if not (pgrec and pgrec['cindex']): return
            oindex = pgrec['cindex']
         pgctl = self.pgget("dcupdt", self.BFIELDS, "cindex = {}".format(oindex), logact)
      if pgctl:
         for bkey in pgctl:
            self.set_one_boption(bkey, pgctl[bkey], 0)

   def get_dsrqst_control(self, pgrqst, logact=0):
      """Retrieves the batch control record for a dsrqst request.

      Walks the group hierarchy (via pindex) until a matching rcrqst record
      is found or the root is reached.

      Args:
         pgrqst (dict): A dsrqst record containing at least dsid, gindex,
            cindex, and rqsttype fields.
         logact (int): Logging action flags.

      Returns:
         dict or None: The rcrqst control record, or None if not found.
      """
      cflds = self.BFIELDS
      if 'ptcount' in pgrqst and pgrqst['ptcount'] == 0: cflds += ", ptlimit, ptsize"
      if pgrqst['cindex']:
         pgctl = self.pgget("rcrqst", cflds, "cindex = {}".format(pgrqst['cindex']), logact)
      else:
         pgctl = None
      if not pgctl:
         gcnd = "dsid = '{}' AND gindex = ".format(pgrqst['dsid'])
         if pgrqst['rqsttype'] in "ST":
            tcnd = " AND (rqsttype = 'T' OR rqsttype = 'S')"
         else:
            tcnd = " AND rqsttype = '{}'".format(pgrqst['rqsttype'])
         gindex = pgrqst['gindex']
         while True:
            pgctl = self.pgget("rcrqst", cflds, "{}{}{}".format(gcnd, gindex, tcnd), logact)
            if pgctl or not gindex: break
            pgctl = self.pgget("dsgroup", "pindex", "{}{}".format(gcnd, gindex), logact)
            if not pgctl: break
            gindex = pgctl['pindex']
      return pgctl

   def get_partition_control(self, pgpart, pgrqst=None, pgctl=None, logact=0):
      """Retrieves batch control info for a dsrqst partition record.

      Args:
         pgpart (dict): Partition record containing at least rindex.
         pgrqst (dict or None): Parent dsrqst record; fetched automatically
            from pgpart['rindex'] if not provided.
         pgctl (dict or None): Pre-fetched control record; skips DB lookup
            if provided.
         logact (int): Logging action flags.

      Returns:
         dict or None: The rcrqst control record, or None if not found.
      """
      if not pgctl:
         if not pgrqst and pgpart['rindex']:
            pgrqst = self.pgget("dsrqst", "dsid, gindex, cindex, rqsttype", "rindex = {}".format(pgpart['rindex']), logact)
         if pgrqst: pgctl = self.get_dsrqst_control(pgrqst, logact)
      return pgctl

   def get_dynamic_options(self, cmd, oindex, otype):
      """Runs cmd to retrieve dynamic option strings, retrying on timeout.

      Executes the command up to three times, retrying when a connection
      timeout is detected in PGLOG['SYSERR']. Parses the output to extract
      option strings in 'read/write' format (-opt1/-opt2) based on otype.

      Args:
         cmd (str): Base command to execute.
         oindex (int or None): Object index appended to cmd when truthy.
         otype (str or None): Object type appended to cmd when truthy;
            'R' selects the first option from a slash-separated pair.

      Returns:
         str: The parsed option string, or '' if the command produced no
            usable output (error details are appended to PGLOG['SYSERR']).
      """
      if oindex: cmd += " {}".format(oindex)
      if otype: cmd += ' ' + otype
      ret = options = ''
      for loop in range(3):
         ret = self.pgsystem(cmd, self.LOGWRN, 1299)  # 1+2+16+256+1024
         if loop < 2 and self.PGLOG['SYSERR'] and 'Connection timed out' in self.PGLOG['SYSERR']:
            time.sleep(self.PGSIG['ETIME'])
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
         if ret: self.PGLOG['SYSERR'] = (self.PGLOG['SYSERR'] or '') + ret
         self.PGLOG['SYSERR'] = (self.PGLOG['SYSERR'] or '') + " for {}".format(cmd)

      return options

   def get_dscheck(self, cmd, argv, workdir, specialist, argextra=None, logact=0):
      """Retrieves a dscheck record matching the given command invocation.

      Searches for an existing dscheck record by command, specialist, argv,
      workdir, and argextra. Also tries variants with the delayed-mode option
      appended or stripped from argv.

      Args:
         cmd (str): Command name.
         argv (str): Argument string (truncated to 100 chars).
         workdir (str): Working directory path.
         specialist (str): Specialist UID.
         argextra (str or None): Remainder of argv beyond 100 chars.
         logact (int): Logging action flags.

      Returns:
         dict or None: The matching dscheck record, or None if not found.
      """
      cnd = "command = '{}' AND specialist = '{}' AND argv = '{}'".format(cmd, specialist, argv)
      pgrecs = self.pgmget("dscheck", "*", cnd, logact)
      cnt = len(pgrecs['cindex']) if pgrecs else 0
      if cnt == 0 and cmd in self.DLYOPT:
         ms = re.match(r'^(.+){}$'.format(self.DLYOPT[cmd]), argv)
         if ms:
            argv = ms.group(1)
            cnt = 1
         elif not argextra:
            dopt = self.append_delayed_mode(cmd, argv)
            if dopt:
               argv += dopt
               cnt = 1
         if cnt:
            cnd = "command = '{}' AND specialist = '{}' AND argv = '{}'".format(cmd, specialist, argv)
            pgrecs = self.pgmget("dscheck", "*", cnd, logact)
            cnt = len(pgrecs['cindex']) if pgrecs else 0
      for i in range(cnt):
         pgrec = self.onerecord(pgrecs, i)
         if pgrec['workdir'] and self.pgcmp(workdir, pgrec['workdir']): continue
         if self.pgcmp(argextra, pgrec['argextra']): continue
         return pgrec
      return None

   def delete_dscheck(self, pgrec, chkcnd, logact=0):
      """Archives and deletes a dscheck record into dschkhist.

      Copies the dscheck record to dschkhist (inserting or updating), logs
      the cleanup, deletes the original dscheck row, and logs any error
      message when the final status is 'E'.

      Args:
         pgrec (dict or None): The dscheck record to delete; fetched from DB
            using chkcnd if None.
         chkcnd (str or None): SQL WHERE condition identifying the record;
            derived from pgrec or DSCHK['chkcnd'] if None.
         logact (int): Logging action flags.

      Returns:
         int: The return value of pgdel (non-zero on success), or 0 if there
            was nothing to delete or the record was already gone.
      """
      if not chkcnd:
         if pgrec:
            chkcnd = "cindex = {}".format(pgrec['cindex'])
         elif 'chkcnd' in self.DSCHK:
            chkcnd = self.DSCHK['chkcnd']
         else:
            return 0   # nothing to delete
      if not pgrec:
         pgrec = self.pgget("dscheck", "*", chkcnd, logact)
         if not pgrec: return 0          # dscheck record is gone
      record = {}
      record['cindex'] = pgrec['cindex']
      record['command'] = pgrec['command']
      record['dsid'] = (pgrec['dsid'] if pgrec['dsid'] else self.PGLOG['DEFDSID'])
      record['action'] = (pgrec['action'] if pgrec['action'] else "UN")
      record['specialist'] = pgrec['specialist']
      record['hostname'] = pgrec['runhost']
      if pgrec['bid']: record['bid'] = pgrec['bid']
      if pgrec['command'] == "dsrqst" and pgrec['oindex']:
         (record['fcount'], record['dcount'], record['size']) = self.get_dsrqst_counts(pgrec, logact)
      else:
         record['fcount'] = pgrec['fcount']
         record['dcount'] = pgrec['dcount']
         record['size'] = pgrec['size']
      record['tcount'] = pgrec['tcount']
      record['date'] = pgrec['date']
      record['time'] = pgrec['time']
      record['closetime'] = self.curtime(1)
      (record['ttltime'], record['quetime']) = self.get_dscheck_runtime(pgrec)
      record['argv'] = pgrec['argv']
      if pgrec['argextra']:
         record['argv'] += pgrec['argextra']
         if len(record['argv']) > 255: record['argv'] = record['argv'][0:255]
      if pgrec['errmsg']: record['errmsg'] = pgrec['errmsg']
      record['status'] = ('F' if pgrec['status'] == "R" else pgrec['status'])
      if self.pgget("dschkhist", "", chkcnd):
         stat = self.pgupdt("dschkhist", record, chkcnd, logact)
      else:
         stat = self.pgadd("dschkhist", record, logact)
      if stat:
         cmsg = "{} cleaned as '{}' at {} on {}".format(self.get_command_info(pgrec), record['status'], self.current_datetime(), self.PGLOG['HOSTNAME'])
         self.pglog("Chk{}: {}".format(pgrec['cindex'], cmsg), self.LOGWRN|self.FRCLOG)
         stat = self.pgdel("dscheck", chkcnd, logact)
      if record['status'] == "E" and 'errmsg' in record:
         self.pglog("Chk{}: {} Exits with Error\n{}".format(pgrec['cindex'], self.get_command_info(pgrec), record['errmsg']), logact)
      return stat

   def get_dsrqst_counts(self, pgchk, logact=0):
      """Retrieves up-to-date fcount, dcount, and size for a dsrqst check.

      Fetches current file counts and size from dsrqst or ptrqst and wfrqst
      tables, falling back to the values already stored in pgchk.

      Args:
         pgchk (dict): A dscheck record with at least oindex, otype, fcount,
            dcount, and size fields.
         logact (int): Logging action flags.

      Returns:
         tuple: A 3-tuple (fcount, dcount, size) with the most current values.
      """
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
      pgrec = self.pgget(table, fields, cnd, logact)
      if pgrec:
         fcnt = pgrec['fcount']
      else:
         fcnt = 0
         pgrec = {'fcount': 0}
      if not fcnt: fcnt = self.pgget("wfrqst", "", cnd, logact)
      if fcnt and fcount != fcnt: fcount = fcnt
      if fcount:
         if 'pcount' in pgrec and pgrec['pcount']:
            dcnt = pgrec['pcount']
         else:
            dcnt = self.pgget("wfrqst", "", cnd + " AND status = 'O'", logact)
         if dcnt and dcnt != dcount: dcount = dcnt
      if not size:
         if 'size_input' in pgrec and pgrec['size_input']:
            if size != pgrec['size_input']: size = pgrec['size_input']
         elif 'size_request' in pgrec and pgrec['size_request']:
            if size != pgrec['size_request']: size = pgrec['size_request']
         elif fcnt:    # evaluate total size only if file count is set in request/partition record
            pgrec = self.pgget("wfrqst", "sum(size) data_size", cnd, logact)
            if pgrec and pgrec['data_size']: size = pgrec['data_size']
      return (fcount, dcount, size)

   def set_dscheck_fcount(self, count, logact=0):
      """Updates the fcount field of the active dscheck record.

      Args:
         count (int): New file count value to store.
         logact (int): Logging action flags.

      Returns:
         int: The current dcount value from the cached DSCHK dict.
      """
      record = {'fcount': count, 'chktime': int(time.time())}
      self.pgupdt("dscheck", record, self.DSCHK['chkcnd'], logact)
      self.DSCHK['fcount'] = count
      return self.DSCHK['dcount']     # return Done count

   def set_dscheck_dcount(self, count, size, logact=0):
      """Updates the dcount and size fields of the active dscheck record.

      Args:
         count (int): New done-file count value to store.
         size (int): New total size value to store.
         logact (int): Logging action flags.

      Returns:
         int: The current dcount value from the cached DSCHK dict.
      """
      record = {'dcount': count, 'size': size, 'chktime': int(time.time())}
      self.pgupdt("dscheck", record, self.DSCHK['chkcnd'], logact)
      self.DSCHK['dcount'] = count
      self.DSCHK['size'] = size
      return self.DSCHK['dcount']     # return Done count

   def add_dscheck_dcount(self, count, size, logact=0):
      """Increments the dcount and size fields of the active dscheck record.

      Args:
         count (int): Amount to add to the current dcount.
         size (int): Amount to add to the current size.
         logact (int): Logging action flags.

      Returns:
         int: The updated dcount value from the cached DSCHK dict.
      """
      record = {}
      if count:
         self.DSCHK['dcount'] += count
         record['dcount'] = self.DSCHK['dcount']
      if size:
         self.DSCHK['size'] += size
         record['size'] = self.DSCHK['size']
      record['chktime'] = int(time.time())
      self.pgupdt("dscheck", record, self.DSCHK['chkcnd'], logact)
      return self.DSCHK['dcount']     # return Done count

   def set_dscheck_attribute(self, fname, value, logact=0):
      """Updates a single named field of the active dscheck record.

      Args:
         fname (str): Field name to update in dscheck.
         value: Value to store; if falsy, only chktime is updated.
         logact (int): Logging action flags.
      """
      record = {}
      if value: record[fname] = value
      record['chktime'] = int(time.time())
      self.pgupdt("dscheck", record, self.DSCHK['chkcnd'], logact)

   def record_dscheck_status(self, stat, logact=0):
      """Updates the dscheck status field if the record is still locked by this process.

      Verifies that the current process still holds the lock on the dscheck
      record before applying the status update, preventing stale updates.

      Args:
         stat (str): Status code to write (e.g. 'D', 'E').
         logact (int): Logging action flags.

      Returns:
         int: Return value of pgupdt (non-zero on success), or 0 if the
            record is missing, unlocked, or locked by a different process.
      """
      pgrec = self.pgget("dscheck", "lockhost, pid", self.DSCHK['chkcnd'], logact)
      if not pgrec: return 0
      if not (pgrec['pid'] and pgrec['lockhost']): return 0
      (chost, cpid) = self.current_process_info()
      if pgrec['pid'] != cpid or pgrec['lockhost'] != chost: return 0

      # update dscheck status only if it is still locked by the current process
      record = {'status': stat, 'chktime': int(time.time()), 'pid': 0}
      return self.pgupdt("dscheck", record, self.DSCHK['chkcnd'], logact)

   def get_try_limit(self, cmd):
      """Returns the maximum number of execution attempts for a given command.

      Args:
         cmd (str): Command name to look up in TRYLMTS.

      Returns:
         int: The per-command try limit, or the 'default' value if cmd is
            not explicitly listed in TRYLMTS.
      """
      return (self.TRYLMTS[cmd] if cmd in self.TRYLMTS else self.TRYLMTS['default'])

   @staticmethod
   def get_dscheck_runtime(pgrec, current=0):
      """Calculates cumulative total and queue times for a dscheck record.

      Args:
         pgrec (dict): A dscheck record containing subtime, stttime, chktime,
            ttltime, and quetime fields.
         current (int): If 1, ignore previously accumulated times and compute
            only from the current run; if 0, add to existing accumulated totals.

      Returns:
         tuple: A 2-tuple (ttltime, quetime) of accumulated integer seconds.
      """
      ttime = (0 if current else pgrec['ttltime'])
      qtime = (0 if current else pgrec['quetime'])
      if pgrec['subtime']:
         ttime += (pgrec['chktime'] - pgrec['subtime'])
         if pgrec['stttime']: qtime += (pgrec['stttime'] - pgrec['subtime'])
      return (ttime, qtime)

   @staticmethod
   def get_command_info(pgrec):
      """Builds a human-readable command identifier string from a dscheck record.

      Args:
         pgrec (dict): A dscheck record containing at least command, oindex,
            otype, dsid, action, and specialist fields.

      Returns:
         str: A concise string such as 'Rqst42 ds123.0 ST of jsmith'.
      """
      if pgrec['oindex']:
         if pgrec['command'] == "dsupdt":
            cinfo = "UC{}".format(pgrec['oindex'])
         elif pgrec['command'] == "dsrqst":
            if pgrec['otype'] == "P":
               cinfo = "RPT{}".format(pgrec['oindex'])
            else:
               cinfo = "Rqst{}".format(pgrec['oindex'])
         else:
            cinfo = "{}-{}".format(pgrec['command'], pgrec['oindex'])
      else:
         cinfo = pgrec['command']
      if pgrec['dsid']: cinfo += " " + pgrec['dsid']
      if pgrec['action']: cinfo += " " + pgrec['action']
      cinfo += " of " + pgrec['specialist']
      return cinfo

   def change_dscheck_oinfo(self, oidx, otype, nidx, ntype):
      """Updates the oindex and otype fields of a dscheck record.

      Finds the dscheck record matching oidx/otype and replaces the object
      reference with nidx/ntype, also updating the cached DSCHK dict.

      Args:
         oidx (int): Current object index to match.
         otype (str): Current object type to match.
         nidx (int): New object index value.
         ntype (str): New object type value.

      Returns:
         int: Return value of pgupdt (non-zero on success), or 0 if no
            matching dscheck record was found.
      """
      cnd = "oindex = {} AND otype = '{}'".format(oidx, otype)
      pgchk = self.pgget('dscheck', 'cindex, oindex, otype', cnd, self.LGEREX)
      if not pgchk: return 0    # miss dscheck record to change
      record = {}
      self.DSCHK['oindex'] = record['oindex'] = nidx
      self.DSCHK['otype'] = record['otype'] = ntype
      cnd = "cindex = {}".format(pgchk['cindex'])
      return self.pgupdt('dscheck', record, cnd, self.LGEREX)
