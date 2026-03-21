###############################################################################
#     Title: pg_sig.py
#    Author: Zaihua Ji,  zji@ucar.edu
#      Date: 08/05/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-11-20 convert to class PgSIG
#   Purpose: python library module for start and control daemon process
#    Github: https://github.com/NCAR/rda-python-common.git
###############################################################################
import os
import re
import sys
import errno
import signal
import time
from contextlib import contextmanager
from .pg_dbi import PgDBI

class PgSIG(PgDBI):
   """Daemon process control, signal handling, child process management, and PBS job tracking.

   Extends PgDBI to provide facilities for starting and stopping daemon
   processes, forking child processes, handling POSIX signals, managing
   background tasks, and querying PBS/Torque batch-job status.

   Instance Attributes:
      VUSERS (list): Usernames permitted to start this daemon.
      CPIDS (dict): Mapping of child process IDs to their process names.
      CBIDS (dict): Mapping of background process IDs to their command strings.
      SDUMP (dict): Paths for default, stderr, and stdout dump files with
         keys ``'DEF'``, ``'ERR'``, and ``'OUT'``.
      PGSIG (dict): Daemon control parameters including quit flag, process
         counts, wait times, PID tracking, daemon name, and start time.
   """

   def __init__(self):
      """Initialize PgSIG with default daemon control state."""
      super().__init__()  # initialize parent class
      self.VUSERS = []  # allow users to start this daemon
      self.CPIDS = {}    # allow upto 'mproc' processes at one time for daemon
      self.CBIDS = {}    # allow upto 'bproc' background processes at one time for each child
      self.SDUMP = {
        'DEF': '/dev/null',
        'ERR': '',
        'OUT': ''
      }
      self.PGSIG = {
         'QUIT': 0,    # 1 if QUIT signal received, quit server if no child
         'MPROC': 1,    # default number of multiple processes
         'BPROC': 1,    # default number of multiple background processes
         'ETIME': 20,   # default error waiting time (in seconds)
         'WTIME': 120,  # default waiting time (in seconds)
         'DTIME': 600,  # the daemon record refresh time (in seconds)
         'RTIME': 2400, # the web rda config unlocking and unconfigured system down waiting time (in seconds)
         'CTIME': 4800, # the lock cleaning & configued system down waiting time (in seconds)
         'PPID': -1,   # 1 - server, (> 1) - child, 0 - non-daemon mode
         'PID': 0,    # current process ID
         'DNAME': '',   # daemon name
         'DSTR': '',   # string for daemon with user login name
         'MTIME': 0,    # maximum daemon running time in seconds, 0 for unlimited
         'STIME': 0,    # time the daemon is started
         'STRTM': '',   # string format of 'STIME'
      }

   # add users for starting this daemon
   def add_vusers(self, user=None, mores=None):
      """Add permitted users for starting this daemon.

      Args:
         user (str, optional): Username to add. If None, clears all permitted
            users.
         mores (list, optional): Additional usernames to add.
      """
      if not user:
         self.VUSERS = [] # clean all vusers
      else:
         self.VUSERS.append(user)
      if mores: self.VUSERS.extend(mores)

   # valid user for starting this daemon
   def check_vuser(self, user, aname=None):
      """Validate that the given user is permitted to start this daemon.

      Exits the process with an error if the user is not in ``VUSERS``.

      Args:
         user (str): Username to validate.
         aname (str, optional): Application name used in the error message.
      """
      if user and self.VUSERS:
         valid = 0;
         for vuser in self.VUSERS:
            if user == vuser:
               valid = 1;
               break
         if valid == 0:
            vuser = ', '.join(self.VUSERS)
            self.pglog("{}: must be '{}' to run '{}'  in Daemon mode".format(user, vuser, aname), self.LGEREX)

   # turn this process into a daemon
   # aname - application name, or daemon name
   # uname - user login name to started the application
   # mproc - upper limit of muiltiple child processes
   # wtime - waiting time (in seconds) for next process for the daemon
   # logon - turn on the logging if true
   # bproc - multiple background processes if > 1
   # mtime - maximum running time for the daemon if provided
   def start_daemon(self, aname, uname, mproc=1, wtime=120, logon=0, bproc=1, mtime=0):
      """Fork the current process into a background daemon.

      Checks that no other instance is already running, forks into the
      background, sets up signal handlers, and redirects stdio streams.

      Args:
         aname (str): Application/daemon name.
         uname (str): Username that is starting the daemon.
         mproc (int, optional): Maximum number of concurrent child processes.
            Defaults to 1.
         wtime (int or str, optional): Polling wait time in seconds (or with
            unit suffix). Defaults to 120.
         logon (int, optional): Enable logging if non-zero. Defaults to 0.
         bproc (int, optional): Maximum concurrent background processes.
            Defaults to 1.
         mtime (int or str, optional): Maximum daemon run time in seconds.
            0 means unlimited. Defaults to 0.
      """
      dstr = "Daemon '{}'{} on {}".format(aname, (" By {}".format(uname) if uname else ''), self.PGLOG['HOSTNAME'])
      pid = self.check_daemon(aname, uname)
      if pid:
         self.pglog("***************** WARNNING **************************\n" +
                     "**  {} is running as PID={}\n".format(dstr, pid) +
                     "**  You need stop it before starting a new one!\n" +
                     "*****************************************************" , self.WARNLG)
         self.pglog("{} is already running as PID={}".format(dstr, pid), self.FRCLOG|self.MSGLOG)
         sys.exit(0)
      if mproc > 1: self.PGSIG['MPROC'] = mproc
      if bproc > 1: self.PGSIG['BPROC'] = bproc
      self.PGSIG['WTIME'] = self.get_wait_time(wtime, 120, "Polling Wait Time")
      self.PGSIG['MTIME'] = self.get_wait_time(mtime, 0, "Maximum Running Time")
      pid = self.process_fork(dstr)
      cpid = pid if pid > 0 else os.getpid()
      msg = "PID={},PL={},WI={}".format(cpid, self.PGSIG['MPROC'], self.PGSIG['WTIME'])
      if self.PGSIG['MTIME']: msg += ",MT={}".format(self.PGSIG['MTIME'])
      logmsg = "{}({}) started".format(dstr, msg)
      if logon: logmsg += " With Logging On"
      if pid > 0:
         self.pglog(logmsg, self.WARNLG)
         sys.exit(0)
      os.setsid()
      os.umask(0)
      # setup to catch signals in daemon only
      signal.signal(signal.SIGCHLD, self.clean_dead_child)
      signal.signal(signal.SIGQUIT, self.signal_catch)
      signal.signal(signal.SIGUSR1, self.signal_catch)
      signal.signal(signal.SIGUSR2, self.signal_catch)
      self.PGSIG['DSTR'] = dstr
      self.PGSIG['DNAME'] = aname
      self.PGSIG['STIME'] = int(time.time())
      self.PGSIG['STRTM'] = self.current_datetime(self.PGSIG['STIME'])
      self.PGSIG['PPID'] = 1
      self.PGSIG['PID'] = cpid
      sys.stdin = open(self.SDUMP['DEF'])
      self.cmdlog("{} By {}".format(logmsg, self.PGSIG['STRTM']))
      if logon:
         self.PGLOG['LOGMASK'] &= ~(self.WARNLG|self.EMLLOG)   # turn off warn/email in daemon
         self.set_dump()
      else:
         self.PGLOG['LOGMASK'] &= ~(self.LGWNEM)   # turn off log/warn/email in daemon
         self.set_dump(self.SDUMP['DEF'])
      self.PGLOG['BCKGRND'] = 1   # make sure the background flag is always on
      self.pgdisconnect(1)   # disconnect database in daemon

   # set dump output file
   def set_dump(self, default=None):
      """Redirect stderr and stdout to log/error dump files.

      Args:
         default (str, optional): Default path used when no environment
            variable override is set. Defaults to None.
      """
      errdump = self.get_environment("ERRDUMP", default)
      outdump = self.get_environment("OUTDUMP", default)
      if not errdump:
         if not self.PGLOG['ERRFILE']:
            self.PGLOG['ERRFILE'] = re.sub(r'\.log$', '.err', self.PGLOG['LOGFILE'], 1)
         errdump = "{}/{}".format(self.PGLOG['LOGPATH'], self.PGLOG['ERRFILE'])
      if errdump != self.SDUMP['ERR']:
         sys.stderr = open(errdump, 'a')
         self.SDUMP['ERR'] = errdump
      if not outdump: outdump = "{}/{}".format(self.PGLOG['LOGPATH'], self.PGLOG['LOGFILE'])
      if outdump != self.SDUMP['OUT']:
         sys.stdout = open(outdump, 'a')
         self.SDUMP['OUT'] = outdump

   # stop daemon and log the ending info
   def stop_daemon(self, msg):
      """Log a graceful daemon stop message.

      Args:
         msg (str): Optional reason or context appended to the log entry.
      """
      msg = " with " + msg if msg else ''
      self.PGLOG['LOGMASK'] |= self.MSGLOG    # turn on logging before daemon stops
      self.pglog("{} Started at {}, Stopped gracefully{} by {}".format(self.PGSIG['DSTR'], self.PGSIG['STRTM'], msg, self.current_datetime()), self.LOGWRN)

   # check if a daemon is running already
   # aname - application name for the daemon
   # uname - user login name who started the daemon
   # return the process id if yes and 0 if not
   def check_daemon(self, aname, uname=None):
      """Check whether a daemon is already running.

      Args:
         aname (str): Application name for the daemon.
         uname (str, optional): Username who started the daemon.

      Returns:
         int: The process ID of the running daemon, or 0 if not running.
      """
      if uname:
         self.check_vuser(uname, aname)
         pcmd = "ps -u {} -f | grep {} | grep ' 1 '".format(uname, aname)
         mp = r"^\s*{}\s+(\d+)\s+1\s+".format(uname)
      else:
         pcmd = "ps -C {} -f | grep ' 1 '".format(aname)
         mp = r"^\s*\w+\s+(\d+)\s+1\s+"
      buf = self.pgsystem(pcmd, self.LOGWRN, 20+1024)
      if buf:
         cpid = os.getpid()
         lines = buf.split('\n')
         for line in lines:
            ms = re.match(mp, line)
            pid = int(ms.group(1)) if ms else 0
            if pid > 0 and pid != cpid: return pid
      return 0

   # check if an application is running already; other than the current processs
   # aname - application name
   # uname - user login name who started the application
   # argv  - argument string
   # return the process id if yes and 0 if not
   def check_application(self, aname, uname=None, sargv=None):
      """Check whether another instance of the application is running.

      Args:
         aname (str): Application name.
         uname (str, optional): Username who started the application.
         sargv (str, optional): Argument string to match against.

      Returns:
         int: The process ID of the running instance, or 0 if not found.
      """
      if uname:
         self.check_vuser(uname, aname)
         pcmd = "ps -u {} -f | grep {} | grep -v ' grep '".format(uname, aname)
         mp = r"^\s*{}\s+(\d+)\s+(\d+)\s+.*{}\S*\s+(.*)$".format(uname, aname)
      else:
         pcmd = "ps -C {} -f".format(aname)
         mp = r"^\s*\w+\s+(\d+)\s+(\d+)\s+.*{}\S*\s+(.*)$".format(aname)
      buf = self.pgsystem(pcmd, self.LOGWRN, 20+1024)
      if not buf: return 0
      cpids = [os.getpid(), os.getppid()]
      pids = []
      ppids = []
      astrs = []
      lines = buf.split('\n')
      for line in lines:
         ms = re.match(mp, line)
         if not ms: continue
         pid = int(ms.group(1))
         ppid = int(ms.group(2))
         if pid in cpids:
            if ppid not in cpids: cpids.append(ppid)
            continue
         pids.append(pid)
         ppids.append(ppid)
         if sargv: astrs.append(ms.group(3))
      pcnt = len(pids)
      if not pcnt: return 0
      i = 0
      while i < pcnt:
         pid = pids[i]
         if pid and pid in cpids:
            pids[i] = 0
            ppid = ppids[i]
            if ppid not in cpids: cpids.append(ppid)
            i = 0
         else:
            i += 1
      for i in range(pcnt):
         pid = pids[i]
         if pid and (not sargv or sargv.find(astrs[i]) > -1): return pid
      return 0

   # validate if the current process is a single one. Quit if not
   def validate_single_process(self, aname, uname=None, sargv=None, logact=None):
      """Ensure only one instance of the application is running; exit otherwise.

      Args:
         aname (str): Application name.
         uname (str, optional): Username who started the application.
         sargv (str, optional): Argument string to match against.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.
      """
      if logact is None: logact = self.LOGWRN
      pid = self.check_application(aname, uname, sargv)
      if pid:
         msg = aname
         if sargv: msg += ' ' + sargv
         msg += ": already running as PID={} on {}".format(pid, self.PGLOG['HOSTNAME'])
         if uname: msg += ' By ' + uname
         self.pglog(msg + ', Quit Now', logact)
         sys.exit(0)

   # check how many processes are running for an application already
   # aname - application name
   # uname - user login name who started the application
   # argv  - argument string
   # return the the number of processes (exclude the child one)
   def check_multiple_application(self, aname, uname=None, sargv=None):
      """Count how many instances of an application are running.

      Args:
         aname (str): Application name.
         uname (str, optional): Username who started the application.
         sargv (str, optional): Argument string to match against.

      Returns:
         int: Number of running instances (excluding the current process).
      """
      if uname:
         self.check_vuser(uname, aname)
         pcmd = "ps -u {} -f | grep {} | grep -v ' grep '".format(uname, aname)
         mp = r"^\s*{}\s+(\d+)\s+(\d+)\s+.*{}\S*\s+(.*)$".format(uname, aname)
      else:
         pcmd = "ps -C {} -f".format(aname)
         mp = r"^\s*\w+\s+(\d+)\s+(\d+)\s+.*{}\S*\s+(.*)$".format(aname)
      buf = self.pgsystem(pcmd, self.LOGWRN, 20+1024)
      if not buf: return 0
      dpids = [os.getpid(), os.getppid()]
      pids = []
      ppids = []
      astrs = []
      lines = buf.split('\n')
      for line in lines:
         ms = re.match(mp, line)
         if not ms: continue
         pid = int(ms.group(1))
         ppid = int(ms.group(2))
         if pid in dpids:
            if ppid > 1 and ppid not in dpids: dpids.append(ppid)
            continue
         elif ppid in pids:
            if pid not in dpids: dpids.append(pid)
            continue
         pids.append(pid)
         ppids.append(ppid)
         if sargv: astrs.append(ms.group(3))
      pcnt = len(pids)
      if not pcnt: return 0
      i = 0
      while i < pcnt:
         pid = pids[i]
         ppid = ppids[i]
         if pid:
            if pid in dpids:
               if ppid > 1 and ppid not in dpids: dpids.append(ppid)
               i = pids[i] = 0
               continue
            elif ppid in pids:
               if pid not in dpids: dpids.append(pid)
               i = pids[i] = 0
               continue
         i += 1
      ccnt = 0
      for i in range(pcnt):
         if pids[i] and (not sargv or sargv.find(astrs[i]) > -1): ccnt += 1
      return ccnt

   # validate if the running processes reach the limit for the given app; Quit if yes
   def validate_multiple_process(self, aname, plimit, uname=None, sargv=None, logact=None):
      """Exit if the number of running application instances meets or exceeds a limit.

      Args:
         aname (str): Application name.
         plimit (int): Maximum allowed number of concurrent instances.
         uname (str, optional): Username who started the application.
         sargv (str, optional): Argument string to match against.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.
      """
      if logact is None: logact = self.LOGWRN
      pcnt = self.check_multiple_application(aname, uname, sargv)
      if pcnt >= plimit:
         msg = aname
         if sargv: msg += ' ' + sargv
         msg += ": already running in {} processes on {}".format(pcnt, self.PGLOG['HOSTNAME'])
         if uname: msg += ' By ' + uname
         self.pglog(msg + ', Quit Now', logact)
         sys.exit(0)

   # fork process
   # return the defined result from call of fork
   def process_fork(self, dstr):
      """Fork the current process, retrying up to 10 times on EAGAIN.

      Args:
         dstr (str): Descriptive string used in error log messages.

      Returns:
         int: The PID returned by ``os.fork()`` (0 in the child, child PID
            in the parent).
      """
      for i in range(10):   # try 10 times
         try:
            pid = os.fork()
            return pid
         except OSError as e:
            if e.errno == errno.EAGAIN:
               time.sleep(5)  # Bug fix: os.sleep() does not exist; use time.sleep()
            else:
               self.pglog("{}: {}".format(dstr, str(e)), self.LGEREX)
               break
      self.pglog("{}: too many tries (10) for os.fork()".format(dstr), self.LGEREX)

   # process the predefined signals
   def signal_catch(self, signum, frame):
      """Handle SIGQUIT, SIGUSR1, and SIGUSR2 signals for the daemon.

      Adjusts logging state and forwards the signal to all child processes
      when called in the server context.

      Args:
         signum (int): The signal number received.
         frame: The current stack frame (unused).
      """
      if self.PGSIG['PPID'] == 1:
         tmp = 'Server'
      elif self.PGSIG['PPID'] > 1:
         tmp = 'Child'
      else:
         tmp = 'Process'
      if signum == signal.SIGQUIT:
         sname = "<{} - signal.SIGQUIT - Quit>".format(signum)
      elif signum == signal.SIGUSR1:
         linfo = 'Logging On'
         if self.PGLOG['LOGMASK']&self.MSGLOG: linfo += ' & Debugging On'
         sname = "<{} - signal.SIGUSR1 - {}>".format(signum, linfo)
      elif signum == signal.SIGUSR2:
         if self.PGLOG['DBGLEVEL']:
            linfo = 'Logging off & Debugging Off'
         else:
            linfo = 'Logging Off'
         sname = "<{} - signal.SIGUSR2 - {}>".format(signum, linfo)
      else:
         sname = "<{} - Signal Not Supports Yet>".format(signum)
      dumpon = 1 if self.SDUMP['OUT'] and self.SDUMP['OUT'] != self.SDUMP['DEF'] else 0
      if not dumpon: self.set_dump()
      self.pglog("catches {} in {} {}".format(sname, tmp, self.PGSIG['DSTR']), self.LOGWRN|self.FRCLOG)
      if signum == signal.SIGUSR1:
         if self.PGLOG['LOGMASK']&self.MSGLOG:
            self.PGLOG['DBGLEVEL'] = 1000  # turn logon twice
         else:
            self.PGLOG['LOGMASK'] |= self.MSGLOG   # turn on logging
      elif signum == signal.SIGUSR2:
         self.PGLOG['LOGMASK'] &= ~(self.MSGLOG)   # turn off logging
         self.PGLOG['DBGLEVEL'] = 0   # turn off debugging
         self.set_dump(self.SDUMP['DEF'])
      else:
        if not dumpon: self.set_dump(self.SDUMP['DEF'])
        if signum == signal.SIGQUIT: self.PGSIG['QUIT'] = 1
      if self.PGSIG['PPID'] <= 1 and len(self.CPIDS) > 0:  # passing signal to child processes
         for pid in self.CPIDS: self.kill_process(pid, signum)

   # wrapper function to call os.kill() logging caught error based on logact
   # return self.SUCCESS is success; PgLog.FAILURE if not
   def kill_process(self, pid, signum, logact=0):
      """Send a signal to a process, optionally logging errors.

      Args:
         pid (int): Target process ID.
         signum (int or signal.Signals): Signal to send.
         logact (int, optional): Log action flags for error reporting.
            Defaults to 0 (no logging).

      Returns:
         int: ``self.SUCCESS`` on success, ``self.FAILURE`` on error.
      """
      try:
         os.kill(pid, signum)
      except Exception as e:
         ret = self.FAILURE
         if logact:
            if type(signum) is int:
               sigstr = str(signum)
            else:
               sigstr = "{}-{}".format(signum.name, int(signum))
            self.pglog("Error pass signal {} to pid {}: {}".format(sigstr, pid, str(e)), logact)
      else:
         ret = self.SUCCESS
      return ret

   # wait child process to finish
   def clean_dead_child(self, signum, frame):
      """Reap zombie child processes in response to SIGCHLD.

      Args:
         signum (int): The signal number received (expected to be SIGCHLD).
         frame: The current stack frame (unused).
      """
      live = 0
      while True:
         try:
            dpid, status = os.waitpid(-1, os.WNOHANG)
         except ChildProcessError as e:
            break  # no child process any more
         except Exception as e:
            self.pglog("Error check child process: {}".format(str(e)), self.ERRLOG)
            break
         else:
            if dpid == 0:
              if live > 0: break   # wait twice if a process is still a live
              live += 1
            elif self.PGSIG['PPID'] < 2:
               if dpid in self.CPIDS: del self.CPIDS[dpid]

   # send signal to daemon and exit
   def signal_daemon(self, sname, aname, uname):
      """Send a named signal to a running daemon and exit.

      Args:
         sname (str): Signal name: ``'quit'``/``'stop'``, ``'logon'``/``'on'``,
            or ``'logoff'``/``'off'`` (case-insensitive).
         aname (str): Application/daemon name.
         uname (str): Username who started the daemon.
      """
      dstr = "Daemon '{}'{} on {}".format(aname, ((" By " + uname) if uname else ""), self.PGLOG['HOSTNAME'])
      pid = self.check_daemon(aname, uname)
      if pid > 0:
         dstr += " (PID = {})".format(pid)
         if re.match(r'^(quit|stop)$', sname, re.I):
            signum = signal.SIGQUIT
            msg = "QUIT"
         elif re.match(r'^(logon|on)$', sname, re.I):
            signum = signal.SIGUSR1
            msg = "Logging ON"
         elif re.match(r'^(logoff|off)$', sname, re.I):
            signum = signal.SIGUSR2
            msg = "Logging OFF"
            self.PGLOG['DBGLEVEL'] = 0
         else:
            self.pglog("{}: invalid Signal for {}".format(sname, dstr), self.LGEREX)

         if self.kill_process(pid, signum, self.LOGERR) == self.SUCCESS:
            self.pglog("{}: signal sent to {}".format(msg, dstr), self.LOGWRN|self.FRCLOG)
      else:
         self.pglog(dstr + ": not running currently", self.LOGWRN|self.FRCLOG)
      sys.exit(0)

   # start a time child to run the command in case hanging
   def timeout_command(self, cmd, logact=None, cmdopt=4):
      """Run a command under a timeout child process to prevent hangs.

      Args:
         cmd (str): Shell command to execute.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.
         cmdopt (int, optional): Command options passed to ``pgsystem``.
            Defaults to 4.
      """
      if logact is None: logact = self.LOGWRN
      if logact&self.EXITLG: logact &= ~self.EXITLG
      self.pglog("> " + cmd, logact)
      if self.start_timeout_child(cmd, logact):
         self.pgsystem(cmd, logact, cmdopt)
         sys.exit(0)

   # start a timeout child process
   # return: 1 - in child, 0 - in parent
   def start_timeout_child(self, msg, logact=None):
      """Fork a timeout-monitoring child process.

      The child returns 1 immediately so the caller can run the actual
      command.  The parent waits up to ``TIMEOUT`` * 2 seconds, then kills
      the child if it has not finished.

      Args:
         msg (str): Descriptive message / command string used in logging.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.

      Returns:
         int: 1 if in the child process, 0 if in the parent.
      """
      if logact is None: logact = self.LOGWRN
      pid = self.process_fork(msg)
      if pid == 0:  # in child
         signal.signal(signal.SIGQUIT, self.signal_catch)   # catch quit signal only
         self.PGSIG['PPID'] = self.PGSIG['PID']
         self.PGSIG['PID'] = pid = os.getpid()
         self.cmdlog("Timeout child to " + msg, time.time(), 0)
         self.pgdisconnect(0)   # disconnect database in child
         return 1
      # in parent
      for i in range(self.PGLOG['TIMEOUT']):
         if not self.check_process(pid): break
         time.sleep(2)  # Bug fix: sys.sleep() does not exist; use time.sleep()
      if self.check_process(pid):  # Bug fix: removed extra 'self' argument
         msg += ": timeout({} secs) in CPID {}".format(2*self.PGLOG['TIMEOUT'], pid)
         pids = self.kill_children(pid, 0)
         time.sleep(6)  # Bug fix: sys.sleep() does not exist; use time.sleep()
         if self.kill_process(pid, signal.SIGKILL, self.LOGERR): pids.insert(0, pid)
         if pids: msg += "\nProcess({}) Killed".format(','.join(map(str, pids)))
         self.pglog(msg, logact)
      return 0

   # kill children recursively start from the deepest and return the pids got killed
   def kill_children(self, pid, logact=None):
      """Recursively kill all child processes of a given PID.

      Traverses the process tree depth-first (deepest first) and sends
      SIGKILL to each process.

      Args:
         pid (int): Parent process ID whose children should be killed.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.

      Returns:
         list: PIDs of processes that were successfully killed.
      """
      if logact is None: logact = self.LOGWRN
      buf = self.pgsystem("ps --ppid {} -o pid".format(pid), logact, 20)
      pids = []
      if buf:
         lines = buf.split('\n')
         for line in lines:
            ms = re.match(r'^\s*(\d+)', line)
            if not ms: continue
            cid = int(ms.group(1))
            if not self.check_process(cid): continue
            cids = self.kill_children(cid, logact)
            if cids: pids = cids + pids
            if self.kill_process(cid, signal.SIGKILL, logact) == self.SUCCESS: pids.insert(0, cid)
      if logact and len(pids): self.pglog("Process({}) Killed".format(','.join(map(str, pids))), logact)
      return pids

   # start a child process
   # pname - unique process name
   def start_child(self, pname, logact=None, dowait=0):
      """Fork a named child process for the daemon to manage.

      Records the child PID in ``CPIDS`` and resets daemon state inside
      the child.

      Args:
         pname (str): Unique process name for this child.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.
         dowait (int, optional): If non-zero, wait for a slot to open when
            at the process limit. Defaults to 0.

      Returns:
         int: 1 if the child was started (or no child needed), -1 if
            already running, or the result of ``pglog`` on failure.
      """
      if logact is None: logact = self.LOGWRN
      if self.PGSIG['MPROC'] < 2: return 1  # no need child process
      if logact&self.EXITLG: logact &= ~self.EXITLG
      if logact&self.MSGLOG: logact |= self.FRCLOG
      if self.PGSIG['QUIT']:
         return self.pglog("{} is in QUIT mode, cannot start CPID for {}".format(self.PGSIG['DSTR'], pname), logact)
      elif len(self.CPIDS) >= self.PGSIG['MPROC']:
         i = 0
         while True:
            pcnt = self.check_child(None, 0, logact)
            if pcnt < self.PGSIG['MPROC']: break
            if dowait:
               self.show_wait_message(i, "{}-{}: wait any {} child processes".format(self.PGSIG['DSTR'], pname, pcnt), logact, dowait)
               i += 1
            else:
               return self.pglog("{}-{}: {} child processes already running at {}".format(self.PGSIG['DSTR'], pname, pcnt, self.current_datetime()), logact)
      if self.check_child(pname): return -1   # process is running already
      pid = self.process_fork(self.PGSIG['DSTR'])
      if pid:
         self.CPIDS[pid] = pname   # record the child process id
         self.pglog("{}: starts CPID {} for {}".format(self.PGSIG['DSTR'], pid, pname))
      else:
         signal.signal(signal.SIGQUIT, signal.SIG_DFL)   # turn off catch QUIT signal in child
         self.PGLOG['LOGMASK'] &= ~self.WARNLG   # turn off warn in child
         self.PGSIG['PPID'] = self.PGSIG['PID']
         self.PGSIG['PID'] = pid = os.getpid()
         self.PGSIG['MPROC'] = 1   # 1 in child process
         self.CBIDS = {}         # empty backgroud proces info in case not
         self.PGSIG['DSTR'] += ": CPID {} for {}".format(pid, pname)
         self.cmdlog("CPID {} for {}".format(pid, pname))
         self.pgdisconnect(0)  # disconnect database in child
      return 1   # child started successfully

   # get child process id for given pname
   def pname2cpid(self, pname):
      """Look up the child process ID for a given process name.

      Args:
         pname (str): Unique process name to look up.

      Returns:
         int: The child PID, or 0 if not found.
      """
      for cpid in self.CPIDS:
         if self.CPIDS[cpid] == pname: return cpid
      return 0

   # check one or all child processes if they are still running
   # pname - unique process name if given
   # pid - check this specified process id if given
   # dowait - 0 no wait, 1 wait all done, -1 wait only when all children are running
   # return the number of running processes if dowait == 0 or 1
   # return the number of none-running processes if dowait == -1
   def check_child(self, pname, pid=0, logact=None, dowait=0):
      """Check whether one or all managed child processes are still running.

      Args:
         pname (str or None): Process name to check; None checks all.
         pid (int, optional): Specific PID to check. Defaults to 0.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.
         dowait (int, optional): 0 returns immediately; positive waits for
            all to finish; negative waits only while all slots are occupied.
            Defaults to 0.

      Returns:
         int: Number of running processes (``dowait >= 0``) or number of
            idle slots (``dowait < 0``).
      """
      if logact is None: logact = self.LOGWRN
      if self.PGSIG['MPROC'] < 2: return 0   # no child process
      if logact&self.EXITLG: logact &= ~self.EXITLG
      ccnt = i = 0
      if dowait < 0: ccnt = 1 if (pid or pname) else self.PGSIG['MPROC']
      while True:
         pcnt = 0
         if not pid and pname: pid = self.pname2cpid(pname)
         if pid:
            if self.check_process(pid):   # process is not done yet
               if pname:
                  self.pglog("{}({}): Child still running".format(pname, pid), logact)
               else:
                  self.pglog("{}: Child still running".format(pid), logact)
               pcnt = 1
            elif pid in self.CPIDS:
               del self.CPIDS[pid]   # clean the saved info for the process
         elif not pname:
            cpids = list(self.CPIDS)
            for cpid in cpids:
               if self.check_process(cpid):  # process is not done yet
                  pcnt += 1
               elif cpid in self.CPIDS:
                  del self.CPIDS[cpid]
         if pcnt == 0 or dowait == 0 or pcnt < ccnt: break
         self.show_wait_message(i, "{}: wait {}/{} child processes".format(self.PGSIG['DSTR'], pcnt, self.PGSIG['MPROC']), logact, dowait)
         i += 1
      return (ccnt - pcnt) if ccnt else pcnt

   # start this process in none daemon mode
   # aname - application name, or daemon name
   #  cact - short action name
   # uname - user login name to started the application
   # mproc - upper limit of muiltiple child processes
   # wtime - waiting time (in seconds) for next process
   def start_none_daemon(self, aname, cact=None, uname=None, mproc=1, wtime=120, logon=1, bproc=1):
      """Initialize signal handling and process limits for non-daemon mode.

      Args:
         aname (str): Application/daemon name.
         cact (str, optional): Short action name appended to the description.
         uname (str, optional): Username who started the application.
         mproc (int, optional): Maximum concurrent child processes. Defaults
            to 1.
         wtime (int or str, optional): Polling wait time. Defaults to 120.
         logon (int, optional): Enable message logging if non-zero. Defaults
            to 1.
         bproc (int, optional): Maximum concurrent background processes.
            Defaults to 1.
      """
      dstr = aname
      if cact: dstr += " for Action " + cact
      if uname:
         dstr +=  " By " + uname
         self.check_vuser(uname, aname)
      signal.signal(signal.SIGQUIT, self.signal_catch)   # catch quit signal only
      signal.signal(signal.SIGCHLD, self.clean_dead_child)
      self.PGSIG['DSTR'] = dstr
      self.PGSIG['DNAME'] = aname
      self.PGSIG['PPID'] = 0
      self.PGSIG['PID'] = os.getpid()
      self.PGSIG['MPROC'] = mproc
      self.PGSIG['BPROC'] = bproc
      self.PGLOG['CMDTIME'] = self.PGSIG['WTIME'] = self.get_wait_time(wtime, 120, "Polling Wait Time")
      if self.PGSIG['MPROC'] > 1:
         self.cmdlog("starts non-daemon {}(ML={},WI={})".format(aname, self.PGSIG['MPROC'], self.PGSIG['WTIME']))
         if not logon: self.PGLOG['LOGMASK'] &= ~self.MSGLOG  # turn off message logging

   # check one process id other than the current one if it is still running
   # pid - specified process id
   # pmsg - process message if given
   def check_process(self, pid):
      """Check whether a process is still running.

      Args:
         pid (int): Process ID to check.

      Returns:
         int: 1 if the process is running, 0 otherwise.
      """
      buf = self.pgsystem("ps -p {} -o pid".format(pid), self.LGWNEX, 20)
      if buf:
         mp = r'^\s*{}$'.format(pid)
         lines = buf.split('\n')
         for line in lines:
            if re.match(mp, line): return 1
      return 0

   # check a process id on give host
   def check_host_pid(self, host, pid, pmsg=None, logact=None):
      """Check whether a PID is running on a specific host.

      Args:
         host (str): Hostname to check (passed to ``rdaps -h``).
         pid (int): Process ID to look up.
         pmsg (str, optional): Message to log if the process is found.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.

      Returns:
         int: 1 if running, 0 if not, -1 on system error.
      """
      if logact is None: logact = self.LOGWRN
      cmd = 'rdaps'
      if host: cmd += " -h " + host
      cmd += " -p {}".format(pid)
      buf = self.pgsystem(cmd, logact, 276)   # 4+16+256
      if not buf: return (-1 if self.PGLOG['SYSERR'] else 0)
      if pmsg: self.pglog(pmsg, logact&(~self.EXITLG))
      return 1

   # check one process id on a given host name if it is still running, with default timeout
   #   pid - specified process id
   #  ppid - specified parent process id
   # uname - user login name who started the daemon
   #  host - host name the pid supposed to be running on
   # aname - application name
   #  pmsg - process message if given
   # return 1 if process is steal live, 0 died already, -1 error checking
   def check_host_process(self, host, pid, ppid=0, uname=None, aname=None, pmsg=None, logact=None):
      """Check whether a process is running on a host using rdaps.

      Args:
         host (str): Hostname to check.
         pid (int): Process ID.
         ppid (int, optional): Parent process ID. Defaults to 0.
         uname (str, optional): Username filter.
         aname (str, optional): Application name filter.
         pmsg (str, optional): Message to log if the process is found.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.

      Returns:
         int: 1 if process is still alive, 0 if dead, -1 on error.
      """
      if logact is None: logact = self.LOGWRN
      cmd = "rdaps"
      if host: cmd += " -h " + host
      if pid: cmd += " -p {}".format(pid)
      if ppid: cmd += " -P {}".format(ppid)
      if uname: cmd += " -u " + uname
      if aname: cmd += " -a " + aname
      buf = self.pgsystem(cmd, logact, 276)   # 4+16+256
      if not buf: return (-1 if self.PGLOG['SYSERR'] else 0)
      if pmsg: self.pglog(pmsg, logact&(~self.EXITLG))
      return 1

   # get a single pbs status record via qstat
   def get_pbs_info(self, qopts, multiple=0, logact=0, chkcnt=1):
      """Retrieve PBS job status information via ``qstat``.

      Args:
         qopts (str): Options or job ID passed to ``qstat -n -w``.
         multiple (int, optional): If non-zero, collect data for multiple
            jobs into lists. Defaults to 0.
         logact (int, optional): Log action flags. Defaults to 0.
         chkcnt (int, optional): Number of retry attempts if ``qstat``
            returns no output. Defaults to 1.

      Returns:
         dict: Mapping of column names to values (or lists of values when
            ``multiple`` is non-zero). Empty dict on failure.
      """
      stat = {}
      loop = 0
      buf = None
      while loop < chkcnt:
         buf = self.pgsystem("qstat -n -w {}".format(qopts), logact, 16)
         if buf: break
         loop += 1
         time.sleep(6)
      if not buf: return stat
      chkt = chkd = 1
      lines = buf.split('\n')
      for line in lines:
         if chkt:
            if re.match(r'^Job ID', line):
               line = re.sub(r'^Job ID', 'JobID', line, 1)
               ckeys = re.split(r'\s+', self.pgtrim(line))
               ckeys[1] = 'UserName'
               ckeys[3] = 'JobName'
               ckeys[7] = 'Reqd' + ckeys[7]
               ckeys[8] = 'Reqd' + ckeys[8]  # Bug fix: was ckeys[7] (wrong index)
               ckeys[9] = 'State'
               ckeys[10] = 'Elap' + ckeys[7]
               ckeys.append('Node')
               kcnt = len(ckeys)
               if multiple:
                  for i in range(kcnt):
                     stat[ckeys[i]] = []
               chkt = 0
         elif chkd:
            if re.match(r'^-----', line): chkd = 0
         else:
            vals = re.split(r'\s+', self.pgtrim(line))
            vcnt = len(vals)
            if vcnt == 1:
               if multiple:
                  stat[ckeys[kcnt-1]].append(vals[0])
               else:
                  stat[ckeys[kcnt-1]] = vals[0]
                  break
            elif vcnt > 1:
               ms = re.match(r'^(\d+)', vals[0])
               if ms: vals[0] = ms.group(1)
               for i in range(vcnt):
                  if multiple:
                     stat[ckeys[i]].append(vals[i])
                  else:
                     stat[ckeys[i]] = vals[i]
                     if vcnt == kcnt: break
      return stat

   # check status of a pbs batch id
   #   bid - specified batch id
   # return hash of batch status, 0 if cannot check any more
   def check_pbs_status(self, bid, logact=None):
      """Retrieve historical status of a PBS batch job via ``qhist``.

      Args:
         bid (str or int): PBS batch job ID.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.

      Returns:
         dict: Mapping of column names to values, or an empty dict on
            failure.
      """
      if logact is None: logact = self.LOGWRN
      stat = {}
      buf = self.pgsystem("qhist -w -j {}".format(bid), logact, 20)
      if not buf: return stat
      chkt = 1
      lines = buf.split('\n')
      for line in lines:
         if chkt:
            if re.match(r'^Job', line):
               line = re.sub(r'^Job ID', 'JobID', line, 1)
               line = re.sub(r'Finish Time', 'FinishTime', line, 1)
               line = re.sub(r'Req Mem', 'ReqMem', line, 1)
               line = re.sub(r'Used Mem\(GB\)', 'UsedMem(GB)', line, 1)
               line = re.sub(r'Avg CPU \(%\)', 'AvgCPU(%)', line, 1)
               line = re.sub(r'Elapsed \(h\)', 'WallTime(h)', line, 1)
               line = re.sub(r'Job Name', 'JobName', line, 1)
               ckeys = re.split(r'\s+', self.pgtrim(line))
               ckeys[1] = 'UserName'
               kcnt = len(ckeys)
               chkt = 0
         else:
            vals = re.split(r'\s+', self.pgtrim(line))
            for i in range(kcnt):
               stat[ckeys[i]] = vals[i]
            break
      return stat

   # check if a pbs batch id is live
   #   bid - specified batch id
   # return 1 if process is steal live, 0 died already or error checking
   def check_pbs_process(self, bid, pmsg=None, logact=None):
      """Check whether a PBS batch job is currently queued or running.

      Args:
         bid (str or int): PBS batch job ID.
         pmsg (str, optional): Message prefix logged with the result state.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.

      Returns:
         int: 1 if the job is active (B/R/Q/S/H/W/X), 0 if finished,
            -1 if the job record was not found.
      """
      if logact is None: logact = self.LOGWRN
      stat = self.get_pbs_info(bid, 0, logact)
      ret = -1
      if stat:
         ms = re.match(r'^(B|R|Q|S|H|W|X)$', stat['State'])
         if ms:
            if pmsg: pmsg += ", STATE='{}' and returns 1".format(ms.group(1))
            ret = 1
         else:
            if pmsg: pmsg += ", STATE='{}' and returns 0".format(stat['State'])
            ret = 0
      elif pmsg:
         pmsg += ", Process Not Exists and returns -1"
      if pmsg: self.pglog(pmsg, logact&~self.EXITLG)
      return ret

   # get wait time
   def get_wait_time(self, wtime, default, tmsg):
      """Parse a wait-time value and convert it to seconds.

      Accepts an integer, a plain numeric string, or a string with a unit
      suffix (``D`` days, ``H`` hours, ``M`` minutes, ``S`` seconds).

      Args:
         wtime (int or str): Wait time value to parse. If falsy the
            ``default`` is used.
         default (int): Fallback value when ``wtime`` is falsy.
         tmsg (str): Descriptive label used in error log messages.

      Returns:
         int: Wait time in seconds.
      """
      if not wtime: wtime = default   # use default time
      if type(wtime) is int: return wtime
      if re.match(r'^(\d*)$', wtime): return int(wtime)
      ms = re.match(r'^(\d*)([DHMS])$', wtime, re.I)
      if ms:
         ret = int(ms.group(1))
         unit = ms.group(2)
      else:
         self.pglog("{}: '{}' NOT in (D,H,M,S)".format(wtime, tmsg), self.LGEREX)
         return default  # Bug fix: LGEREX exits, but add fallback so 'unit' is always defined
      if unit != 'S':
         ret *= 60   # seconds in a minute
         if unit != 'M':
            ret *= 60   # minutes in an hour
            if unit != 'H':
               ret *= 24   # hours in a day
      return ret   # in seconds

   # start a background process and record its id; check self.pgsystem() in self.pm for
   # valid cmdopt values
   def start_background(self, cmd, logact=None, cmdopt=5, dowait=0):
      """Start a shell command as a background process and record its ID.

      If ``BPROC`` is less than 2, the command is run synchronously via
      ``pgsystem``.

      Args:
         cmd (str): Shell command to run in the background.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.
         cmdopt (int, optional): Bitfield controlling logging and redirection
            (see ``pgsystem`` docs). Defaults to 5.
         dowait (int, optional): If non-zero, wait for a background slot to
            open before starting. Defaults to 0.

      Returns:
         int or str: Result from ``pgsystem`` (synchronous mode) or
            ``record_background``.
      """
      if logact is None: logact = self.LOGWRN
      if self.PGSIG['BPROC'] < 2: return self.pgsystem(cmd, logact, cmdopt)  # no background
      act = logact&(~self.EXITLG)
      if act&self.MSGLOG: act |= self.FRCLOG  # make sure background calls always logged
      if len(self.CBIDS) >= self.PGSIG['BPROC']:
         i = 0
         while True:
            bcnt = self.check_background(None, 0, act)
            if bcnt < self.PGSIG['BPROC']: break
            if dowait:
               self.show_wait_message(i, "{}-{}: wait any {} background calls".format(self.PGSIG['DSTR'], cmd, bcnt), act, dowait)
               i += 1
            else:
               return self.pglog("{}-{}: {} background calls already at {}".format(self.PGSIG['DSTR'], cmd, bcnt, self.current_datetime()), act)
      cmdlog = (act if cmdopt&1 else self.WARNLG)
      if cmdopt&8:
         self.cmdlog("starts '{}'".format(cmd), None, cmdlog)
      else:
         self.pglog("{}({})-{} >{} &".format(self.PGLOG['HOSTNAME'], os.getpid(), self.current_datetime(), cmd), cmdlog)
      bckcmd = cmd
      if cmdopt&2:
         bckcmd += " >> {}/{}".format(self.PGLOG['LOGPATH'], self.PGLOG['LOGFILE'])
      if cmdopt&4:
         if not self.PGLOG['ERRFILE']:
            self.PGLOG['ERRFILE'] = re.sub(r'\.log$', '.err', self.PGLOG['LOGFILE'], 1)
         bckcmd += " 2>> {}/{}".format(self.PGLOG['LOGPATH'], self.PGLOG['ERRFILE'])
      bckcmd += " &"
      os.system(bckcmd)
      return self.record_background(cmd, logact)

   # get background process id for given bcmd
   def bcmd2cbid(self, bcmd):
      """Look up the background process ID for a given command string.

      Args:
         bcmd (str): Background command string to look up.

      Returns:
         int: The background process ID, or 0 if not found.
      """
      for cbid in self.CBIDS:
         if self.CBIDS[cbid] == bcmd: return cbid
      return 0

   # check one or all child processes if they are still running
   # bid - check this specified background process id if given
   # return the number of processes are still running
   def check_background(self, bcmd, bid=0, logact=None, dowait=0):
      """Check whether one or all background processes are still running.

      Args:
         bcmd (str or None): Background command to check; None checks all.
         bid (int, optional): Specific background process ID. Defaults to 0.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.
         dowait (int, optional): If non-zero, keep checking until all
            background processes have finished. Defaults to 0.

      Returns:
         int: Number of background processes still running.
      """
      if logact is None: logact = self.LOGWRN
      if self.PGSIG['BPROC'] < 2: return 0  # no background process
      if logact&self.EXITLG: logact &= ~self.EXITLG
      if not bid and bcmd: bid = self.bcmd2cbid(bcmd)
      bcnt = i = 0
      while True:
         if bid:
            if self.check_process(bid):  # process is not done yet
               if bcmd:
                  self.pglog("{}({}): Background process still running".format(bcmd, bid), logact)
               else:
                  self.pglog("{}: Background process still running".format(bid), logact)
               bcnt = 1
            elif bid in self.CBIDS:
               del self.CBIDS[bid]   # clean the saved info for the process
         elif not bcmd:
            # Bug fix: cannot delete from a dict while iterating over it;
            # iterate over a copy of the keys instead.
            cbids = list(self.CBIDS)
            for bid in cbids:
               if self.check_process(bid):  # process is not done yet
                  bcnt += 1
               else:
                  del self.CBIDS[bid]
         if not (bcnt and dowait): break
         self.show_wait_message(i, "{}: wait {}/{} background processes".format(self.PGSIG['DSTR'], bcnt, self.PGSIG['MPROC']), logact, dowait)
         i += 1
         bcnt = 0
      return bcnt

   # check and record process id for background command; return 1 if success full;
   # 0 otherwise; -1 if done already
   def record_background(self, bcmd, logact=None):
      """Locate and record the process ID for a recently started background command.

      Args:
         bcmd (str): The background command whose PID should be recorded.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.

      Returns:
         int: 1 if the PID was found and recorded, 0 if not found after
            retries, -1 if already recorded.
      """
      if logact is None: logact = self.LOGWRN
      ms = re.match(r'^(\S+)', bcmd)
      if ms:
         aname = ms.group(1)
      else:
         aname = bcmd
      mp = r"^\s*(\S+)\s+(\d+)\s+1\s+.*{}(.*)$".format(aname)
      pc = "ps -u {},{} -f | grep ' 1 ' | grep {}".format(self.PGLOG['CURUID'], self.PGLOG['GDEXUSER'], aname)
      for i in range(2):
         buf = self.pgsystem(pc, logact, 20+1024)
         if buf:
            lines = buf.split('\n')
            for line in lines:
               ms = re.match(mp, line)
               if not ms: continue
               (uid, sbid, acmd) = ms.groups()
               bid = int(sbid)
               if bid in self.CBIDS: return -1
               if uid == self.PGLOG['GDEXUSER']:
                  acmd = re.sub(r'^\.(pl|py)\s+', '', acmd, 1)
               if re.match(r'^{}{}'.format(aname, acmd), bcmd): continue
               self.CBIDS[bid] = bcmd
               return 1
         time.sleep(2)
      return 0

   # sleep for given period for the daemon, stops if maximum running time reached
   def sleep_daemon(self, wtime=0, mtime=None):
      """Sleep for the daemon's configured wait interval.

      Checks the maximum running time and sets the QUIT flag if it has been
      exceeded.

      Args:
         wtime (int, optional): Override sleep duration in seconds. Uses
            ``PGSIG['WTIME']`` when 0. Defaults to 0.
         mtime (int, optional): Maximum running time in seconds. Uses
            ``PGSIG['MTIME']`` when None. Defaults to None.

      Returns:
         int: Actual sleep time in seconds (0 if QUIT was triggered).
      """
      if not wtime: wtime = self.PGSIG['WTIME']
      if mtime is None: mtime = self.PGSIG['MTIME']
      if mtime > 0:
         rtime = int(time.time()) - self.PGSIG['STIME']
         if rtime >= mtime:
            self.PGSIG['QUIT'] = 1
            wtime = 0
      if wtime: time.sleep(wtime)
      return wtime

   # show wait message every dintv and then sleep for PGSIG['WTIME']
   def show_wait_message(self, loop, msg, logact=None, dowait=0):
      """Log a wait message every 30 loops and optionally sleep.

      Args:
         loop (int): Current loop iteration count.
         msg (str): Message to log.
         logact (int, optional): Log action flags. Defaults to ``LOGWRN``.
         dowait (int, optional): If non-zero, sleep for ``PGSIG['WTIME']``
            seconds. Defaults to 0.
      """
      if logact is None: logact = self.LOGWRN
      if loop > 0 and (loop%30) == 0:
         self.pglog("{} at {}".format(msg, self.current_datetime()), logact)
      if dowait: time.sleep(self.PGSIG['WTIME'])

   # register a time out function to raise a time out error
   @contextmanager
   def pgtimeout(self, seconds=0, logact=0):
      """Context manager that raises ``TimeoutError`` after a given interval.

      Args:
         seconds (int, optional): Timeout in seconds. Uses
            ``PGLOG['TIMEOUT']`` when 0. Defaults to 0.
         logact (int, optional): Reserved for future log action use.
            Defaults to 0.

      Yields:
         None: Control is yielded to the ``with`` block body.
      """
      if not seconds: seconds = self.PGLOG['TIMEOUT']
      signal.signal(signal.SIGALRM, self.raise_pgtimeout)
      signal.alarm(seconds)
      try:
         yield
      except TimeoutError as e:
         pass
      finally:
         signal.signal(signal.SIGALRM, signal.SIG_IGN)

   # raise a timeout Error
   @staticmethod
   def raise_pgtimeout(signum, frame):
      """SIGALRM handler that raises ``TimeoutError``.

      Args:
         signum (int): Signal number (expected SIGALRM).
         frame: Current stack frame (unused).

      Raises:
         TimeoutError: Always raised to interrupt the timed block.
      """
      raise TimeoutError

   # Add a timeout block.
   def timeout_func(self):
      """Demonstrate the pgtimeout context manager with a 1-second limit."""
      with self.pgtimeout(1):
          print('entering block')
          time.sleep(10)
          print('This should never get printed because the line before timed out')
