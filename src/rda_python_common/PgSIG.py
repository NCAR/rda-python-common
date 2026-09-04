#
###############################################################################
#
#     Title : PgSIG.py
#
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 08/05/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module for start and control daemon process
#
#    Github : https://github.com/NCAR/rda-python-common.git
# 
###############################################################################
#
import os
import re
import sys
import errno
import signal
import time
import subprocess
import psutil
from contextlib import contextmanager
from . import PgLOG
from . import PgDBI

VUSERS = []  # allow users to start this daemon
CPIDS = {}    # allow upto 'mproc' processes at one time for daemon
CBIDS = {}    # allow upto 'bproc' background processes at one time for each child
SDUMP = {
  'DEF' : '/dev/null',
  'ERR' : '',
  'OUT' : ''
}

PGSIG = {
   'QUIT'  : 0,    # 1 if QUIT signal received, quit server if no child
   'MPROC' : 1,    # default number of multiple processes
   'BPROC' : 1,    # default number of multiple background processes
   'ETIME' : 20,   # default error waiting time (in seconds)
   'WTIME' : 120,  # default waiting time (in seconds)
   'DTIME' : 600,  # the daemon record refresh time (in seconds)
   'RTIME' : 2400, # the web rda config unlocking and unconfigured system down waiting time (in seconds)
   'CTIME' : 4800, # the lock cleaning & configued system down waiting time (in seconds)
   'PPID'  : -1,   # 1 - server, (> 1) - child, 0 - non-daemon mode
   'PID'   : 0,    # current process ID
   'DNAME' : '',   # daemon name
   'DSTR'  : '',   # string for daemon with user login name
   'MTIME' : 0,    # maximum daemon running time in seconds, 0 for unlimited
   'STIME' : 0,    # time the daemon is started
   'STRTM' : '',   # string format of 'STIME'
}

#
# add users for starting this daemon
#
def add_vusers(user = None, mores = None):

   global VUSERS
   if not user:
      VUSERS = [] # clean all vusers
   else:
      VUSERS.append(user)
   
   if mores: VUSERS.extend(mores)
   
#
# valid user for starting this daemon
#
def check_vuser(user, aname = None):

   if user and VUSERS:
      valid = 0;
      for vuser in VUSERS:
         if user == vuser:
            valid = 1;
            break

      if valid == 0:
         vuser = ', '.join(VUSERS)
         PgLOG.pglog("{}: must be '{}' to run '{}'  in Daemon mode".format(user, vuser, aname), PgLOG.LGEREX) 

#
# turn this process into a daemon
#
# aname - application name, or daemon name
# uname - user login name to started the application
# mproc - upper limit of muiltiple child processes
# wtime - waiting time (in seconds) for next process for the daemon
# logon - turn on the logging if true
# bproc - multiple background processes if > 1
# mtime - maximum running time for the daemon if provided 
#
def start_daemon(aname, uname, mproc = 1, wtime = 120, logon = 0, bproc = 1, mtime = 0):

   dstr = "Daemon '{}'{} on {}".format(aname, (" By {}".format(uname) if uname else ''), PgLOG.PGLOG['HOSTNAME'])

   pid = check_daemon(aname, uname)
   if pid:
      PgLOG.pglog("***************** WARNNING **************************\n" +
                  "**  {} is running as PID={}\n".format(dstr, pid) +
                  "**  You need stop it before starting a new one!\n" +
                  "*****************************************************" , PgLOG.WARNLG)
      PgLOG.pglog("{} is already running as PID={}".format(dstr, pid), PgLOG.FRCLOG|PgLOG.MSGLOG)
      sys.exit(0)

   if mproc > 1: PGSIG['MPROC'] = mproc
   if bproc > 1: PGSIG['BPROC'] = bproc
   PGSIG['WTIME'] = get_wait_time(wtime, 120, "Polling Wait Time")
   PGSIG['MTIME'] = get_wait_time(mtime, 0, "Maximum Running Time")

   pid = process_fork(dstr)
   cpid = pid if pid > 0 else os.getpid()
   msg = "PID={},PL={},WI={}".format(cpid, PGSIG['MPROC'], PGSIG['WTIME'])
   if PGSIG['MTIME']: msg += ",MT={}".format(PGSIG['MTIME'])
   logmsg = "{}({}) started".format(dstr, msg)
   if logon: logmsg += " With Logging On"
   if pid > 0:
      PgLOG.pglog(logmsg, PgLOG.WARNLG)
      sys.exit(0)

   os.setsid()
   os.umask(0)

   # setup to catch signals in daemon only
   signal.signal(signal.SIGCHLD, clean_dead_child)
   signal.signal(signal.SIGQUIT, signal_catch)
   signal.signal(signal.SIGUSR1, signal_catch)
   signal.signal(signal.SIGUSR2, signal_catch)
   PGSIG['DSTR'] = dstr
   PGSIG['DNAME'] = aname
   PGSIG['STIME'] = int(time.time())
   PGSIG['STRTM'] = PgLOG.current_datetime(PGSIG['STIME'])
   PGSIG['PPID'] = 1
   PGSIG['PID'] = cpid

   sys.stdin = open(SDUMP['DEF'])
   PgLOG.cmdlog("{} By {}".format(logmsg, PGSIG['STRTM']))

   if logon:
      PgLOG.PGLOG['LOGMASK'] &= ~(PgLOG.WARNLG|PgLOG.EMLLOG)   # turn off warn/email in daemon
      set_dump()
   else:
      PgLOG.PGLOG['LOGMASK'] &= ~(PgLOG.LGWNEM)   # turn off log/warn/email in daemon
      set_dump(SDUMP['DEF'])

   PgLOG.PGLOG['BCKGRND'] = 1   # make sure the background flag is always on
   PgDBI.pgdisconnect(1)   # disconnect database in daemon

#
# set dump output file
#
def set_dump(default = None):

   errdump = PgLOG.get_environment("ERRDUMP", default)
   outdump = PgLOG.get_environment("OUTDUMP", default)

   if not errdump:
      if not PgLOG.PGLOG['ERRFILE']:
         PgLOG.PGLOG['ERRFILE'] = re.sub(r'\.log$', '.err', PgLOG.PGLOG['LOGFILE'], 1)
      errdump = "{}/{}".format(PgLOG.PGLOG['LOGPATH'], PgLOG.PGLOG['ERRFILE'])

   if errdump != SDUMP['ERR']:
      sys.stderr = open(errdump, 'a')
      SDUMP['ERR'] = errdump

   if not outdump: outdump = "{}/{}".format(PgLOG.PGLOG['LOGPATH'], PgLOG.PGLOG['LOGFILE'])
   if outdump != SDUMP['OUT']:
      sys.stdout = open(outdump, 'a')
      SDUMP['OUT'] = outdump

#
# stop daemon and log the ending info
# 
def stop_daemon(msg):

   msg = " with " + msg if msg else ''
   PgLOG.PGLOG['LOGMASK'] |= PgLOG.MSGLOG    # turn on logging before daemon stops
   PgLOG.pglog("{} Started at {}, Stopped gracefully{} by {}".format(PGSIG['DSTR'], PGSIG['STRTM'], msg, PgLOG.current_datetime()), PgLOG.LOGWRN)

#
# scan running processes via psutil and return a list of dicts matching aname/uname
#
# Mirrors the semantics of the previous "ps -u U -f | grep A" / "ps -C A -f" pipelines:
#   - with uname:  substring match of aname anywhere in cmdline (looser, like grep)
#   - without uname: aname matches the executable basename (with optional .ext)
#
# Each entry has keys: 'pid', 'ppid', 'args' ('args' is cmdline[1:] joined).
#
def _scan_app_processes(aname, uname=None):
   results = []
   for proc in psutil.process_iter(['pid', 'ppid', 'username', 'cmdline']):
      try:
         info = proc.info
         cmdline = info.get('cmdline') or []
         if not cmdline: continue
         if uname is not None and info.get('username') != uname: continue
         if uname:
            if not any(aname in arg for arg in cmdline): continue
         else:
            exe = os.path.basename(cmdline[0])
            if exe != aname and re.sub(r'\.\w+$', '', exe) != aname: continue
         results.append({
            'pid': info['pid'],
            'ppid': info['ppid'],
            'args': ' '.join(cmdline[1:]),
         })
      except (psutil.NoSuchProcess, psutil.AccessDenied):
         continue
   return results

#
# check if a daemon is running already
#
# aname - application name for the daemon
# uname - user login name who started the daemon
#
# return the process id if yes and 0 if not
#
def check_daemon(aname, uname=None):
   if uname: check_vuser(uname, aname)
   cpid = os.getpid()
   for p in _scan_app_processes(aname, uname):
      if p['ppid'] != 1: continue
      if p['pid'] != cpid: return p['pid']
   return 0

#
# check if an application is running already; other than the current processs
#
# aname - application name
# uname - user login name who started the application
# argv  - argument string
#
# return the process id if yes and 0 if not
#
def check_application(aname, uname=None, sargv=None):
   if uname: check_vuser(uname, aname)
   procs = _scan_app_processes(aname, uname)
   if not procs: return 0
   cpids = [os.getpid(), os.getppid()]
   pids = []
   ppids = []
   astrs = []
   for p in procs:
      pid, ppid = p['pid'], p['ppid']
      if pid in cpids:
         if ppid not in cpids: cpids.append(ppid)
         continue
      pids.append(pid)
      ppids.append(ppid)
      if sargv: astrs.append(p['args'])
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

#
# validate if the current process is a single one. Quit if not
#
def validate_single_process(aname, uname=None, sargv=None, logact=None):
   if logact is None: logact = PgLOG.LOGWRN
   pid = check_application(aname, uname, sargv)
   if pid:
      msg = aname
      if sargv: msg += ' ' + sargv
      msg += ": already running as PID={} on {}".format(pid, PgLOG.PGLOG['HOSTNAME'])
      if uname: msg += ' By ' + uname
      PgLOG.pglog(msg + ', Quit Now', logact)
      sys.exit(0)

#
# check how many processes are running for an application already
#
# aname - application name
# uname - user login name who started the application
# argv  - argument string
#
# return the the number of processes (exclude the child one)
#
def check_multiple_application(aname, uname=None, sargv=None):
   if uname: check_vuser(uname, aname)
   procs = _scan_app_processes(aname, uname)
   if not procs: return 0
   dpids = [os.getpid(), os.getppid()]
   pids = []
   ppids = []
   astrs = []
   for p in procs:
      pid, ppid = p['pid'], p['ppid']
      if pid in dpids:
         if ppid > 1 and ppid not in dpids: dpids.append(ppid)
         continue
      elif ppid in pids:
         if pid not in dpids: dpids.append(pid)
         continue
      pids.append(pid)
      ppids.append(ppid)
      if sargv: astrs.append(p['args'])
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

#
# validate if the running processes reach the limit for the given app; Quit if yes
#
def validate_multiple_process(aname, plimit, uname=None, sargv=None, logact=None):
   if logact is None: logact = PgLOG.LOGWRN
   pcnt = check_multiple_application(aname, uname, sargv)
   if pcnt >= plimit:
      msg = aname
      if sargv: msg += ' ' + sargv
      msg += ": already running in {} processes on {}".format(pcnt, PgLOG.PGLOG['HOSTNAME'])
      if uname: msg += ' By ' + uname
      PgLOG.pglog(msg + ', Quit Now', logact)
      sys.exit(0)

#
# fork process
#
# return the defined result from call of fork
#
def process_fork(dstr):
   for i in range(10):   # try 10 times
      try:
         pid = os.fork()
         return pid
      except OSError as e:
         if e.errno == errno.EAGAIN:
            time.sleep(5)  # Bug fix: os.sleep() does not exist; use time.sleep()
         else:
            PgLOG.pglog("{}: {}".format(dstr, str(e)), PgLOG.LGEREX)
            break
   PgLOG.pglog("{}: too many tries (10) for os.fork()".format(dstr), PgLOG.LGEREX)

#
# process the predefined signals
#
def signal_catch(signum, frame):

   if PGSIG['PPID'] == 1:
      tmp = 'Server'
   elif PGSIG['PPID'] > 1:
      tmp = 'Child'
   else:
      tmp = 'Process'

   if signum == signal.SIGQUIT:
      sname = "<{} - signal.SIGQUIT - Quit>".format(signum)
   elif signum == signal.SIGUSR1:
      linfo = 'Logging On'
      if PgLOG.PGLOG['LOGMASK']&PgLOG.MSGLOG: linfo += ' & Debugging On'
      sname = "<{} - signal.SIGUSR1 - {}>".format(signum, linfo)
   elif signum == signal.SIGUSR2:
      if PgLOG.PGLOG['DBGLEVEL']:
         linfo = 'Logging off & Debugging Off'
      else:
         linfo = 'Logging Off'
      sname = "<{} - signal.SIGUSR2 - {}>".format(signum, linfo)
   else:
      sname = "<{} - Signal Not Supports Yet>".format(signum)

   dumpon = 1 if SDUMP['OUT'] and SDUMP['OUT'] != SDUMP['DEF'] else 0
   if not dumpon: set_dump()
   PgLOG.pglog("catches {} in {} {}".format(sname, tmp, PGSIG['DSTR']), PgLOG.LOGWRN|PgLOG.FRCLOG)

   if signum == signal.SIGUSR1:
      if PgLOG.PGLOG['LOGMASK']&PgLOG.MSGLOG:
         PgLOG.PGLOG['DBGLEVEL'] = 1000  # turn logon twice
      else:
         PgLOG.PGLOG['LOGMASK'] |= PgLOG.MSGLOG   # turn on logging
   elif signum == signal.SIGUSR2:
      PgLOG.PGLOG['LOGMASK'] &= ~(PgLOG.MSGLOG)   # turn off logging
      PgLOG.PGLOG['DBGLEVEL'] = 0   # turn off debugging
      set_dump(SDUMP['DEF'])
   else:
     if not dumpon: set_dump(SDUMP['DEF'])
     if signum == signal.SIGQUIT: PGSIG['QUIT'] = 1

   if PGSIG['PPID'] <= 1 and len(CPIDS) > 0:  # passing signal to child processes
      for pid in CPIDS: kill_process(pid, signum)

#
# wrapper function to call os.kill() logging caught error based on logact
# return PgLOG.SUCCESS is success; PgLog.FAILURE if not
#
def kill_process(pid, signum, logact = 0):

   try:
      os.kill(pid, signum)
   except Exception as e:
      ret = PgLOG.FAILURE
      if logact:
         if type(signum) is int:
            sigstr = str(signum)
         else:
            sigstr = "{}-{}".format(signum.name, int(signum))
         PgLOG.pglog("Error pass signal {} to pid {}: {}".format(sigstr, pid, str(e)), logact)
   else:
      ret = PgLOG.SUCCESS

   return ret
 
#
# wait child process to finish
#
def clean_dead_child(signum, frame):
   live = 0
   while True:
      try:
         dpid, status = os.waitpid(-1, os.WNOHANG)
      except ChildProcessError as e:
         break  # no child process any more
      except Exception as e:
         PgLOG.pglog("Error check child process: {}".format(str(e)), PgLOG.ERRLOG)
         break
      else:
         if dpid == 0:
           if live > 0: break   # wait twice if a process is still a live
           live += 1
         elif PGSIG['PPID'] < 2:
            if dpid in CPIDS: del CPIDS[dpid]

#
# send signal to daemon and exit
#
def signal_daemon(sname, aname, uname):
   
   dstr = "Daemon '{}'{} on {}".format(aname, ((" By " + uname) if uname else ""), PgLOG.PGLOG['HOSTNAME'])
   pid = check_daemon(aname, uname)

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
         PgLOG.PGLOG['DBGLEVEL'] = 0
      else:
         PgLOG.pglog("{}: invalid Signal for {}".format(sname, dstr), PgLOG.LGEREX)

      if kill_process(pid, signum, PgLOG.LOGERR) == PgLOG.SUCCESS:
         PgLOG.pglog("{}: signal sent to {}".format(msg, dstr), PgLOG.LOGWRN|PgLOG.FRCLOG)
   else:
      PgLOG.pglog(dstr + ": not running currently", PgLOG.LOGWRN|PgLOG.FRCLOG)

   sys.exit(0)

#
# start a time child to run the command in case hanging
#
def timeout_command(cmd, logact=None, cmdopt=4):
   if logact is None: logact = PgLOG.LOGWRN
   if logact&PgLOG.EXITLG: logact &= ~PgLOG.EXITLG
   PgLOG.pglog("> " + cmd, logact)
   if start_timeout_child(cmd, logact):
      PgLOG.pgsystem(cmd, logact, cmdopt)
      sys.exit(0)

#
# start a timeout child process 
#
# return: 1 - in child, 0 - in parent
#
def start_timeout_child(msg, logact=None):
   if logact is None: logact = PgLOG.LOGWRN
   pid = process_fork(msg)
   if pid == 0:  # in child
      signal.signal(signal.SIGQUIT, signal_catch)   # catch quit signal only
      PGSIG['PPID'] = PGSIG['PID']
      PGSIG['PID'] = pid = os.getpid()
      PgLOG.cmdlog("Timeout child to " + msg, time.time(), 0)
      PgDBI.pgdisconnect(0)   # disconnect database in child
      return 1
   # in parent
   for i in range(PgLOG.PGLOG['TIMEOUT']):
      if not check_process(pid): break
      time.sleep(2)  # Bug fix: sys.sleep() does not exist; use time.sleep()
   if check_process(pid):  # Bug fix: removed extra 'self' argument
      msg += ": timeout({} secs) in CPID {}".format(2*PgLOG.PGLOG['TIMEOUT'], pid)
      pids = kill_children(pid, 0)
      time.sleep(6)  # Bug fix: sys.sleep() does not exist; use time.sleep()
      if kill_process(pid, signal.SIGKILL, PgLOG.LOGERR): pids.insert(0, pid)
      if pids: msg += "\nProcess({}) Killed".format(','.join(map(str, pids)))
      PgLOG.pglog(msg, logact)
   return 0

#
# kill children recursively start from the deepest and return the pids got killed
#
def kill_children(pid, logact=None):
   if logact is None: logact = PgLOG.LOGWRN
   pids = []
   try:
      children = psutil.Process(pid).children()
   except psutil.NoSuchProcess:
      children = []
   except Exception as e:
      PgLOG.pglog("Error listing children of pid {}: {}".format(pid, str(e)), logact)
      children = []
   for child in children:
      cid = child.pid
      if not check_process(cid): continue
      cids = kill_children(cid, logact)
      if cids: pids = cids + pids
      if kill_process(cid, signal.SIGKILL, logact) == PgLOG.SUCCESS: pids.insert(0, cid)
   if logact and len(pids): PgLOG.pglog("Process({}) Killed".format(','.join(map(str, pids))), logact)
   return pids

#
# start a child process
# pname - unique process name 
#
def start_child(pname, logact=None, dowait=0):
   global CBIDS
   if logact is None: logact = PgLOG.LOGWRN
   if PGSIG['MPROC'] < 2: return 1  # no need child process
   if logact&PgLOG.EXITLG: logact &= ~PgLOG.EXITLG
   if logact&PgLOG.MSGLOG: logact |= PgLOG.FRCLOG
   if PGSIG['QUIT']:
      return PgLOG.pglog("{} is in QUIT mode, cannot start CPID for {}".format(PGSIG['DSTR'], pname), logact)
   elif len(CPIDS) >= PGSIG['MPROC']:
      i = 0
      while True:
         pcnt = check_child(None, 0, logact)
         if pcnt < PGSIG['MPROC']: break
         if dowait:
            show_wait_message(i, "{}-{}: wait any {} child processes".format(PGSIG['DSTR'], pname, pcnt), logact, dowait)
            i += 1
         else:
            return PgLOG.pglog("{}-{}: {} child processes already running at {}".format(PGSIG['DSTR'], pname, pcnt, PgLOG.current_datetime()), logact)
   if check_child(pname): return -1   # process is running already
   pid = process_fork(PGSIG['DSTR'])
   if pid:
      CPIDS[pid] = pname   # record the child process id
      PgLOG.pglog("{}: starts CPID {} for {}".format(PGSIG['DSTR'], pid, pname))
   else:
      signal.signal(signal.SIGQUIT, signal.SIG_DFL)   # turn off catch QUIT signal in child
      PgLOG.PGLOG['LOGMASK'] &= ~PgLOG.WARNLG   # turn off warn in child
      PGSIG['PPID'] = PGSIG['PID']
      PGSIG['PID'] = pid = os.getpid()
      PGSIG['MPROC'] = 1   # 1 in child process
      CBIDS = {}         # empty backgroud proces info in case not
      PGSIG['DSTR'] += ": CPID {} for {}".format(pid, pname)
      PgLOG.cmdlog("CPID {} for {}".format(pid, pname))
      PgDBI.pgdisconnect(0)  # disconnect database in child
   return 1   # child started successfully

#
# get child process id for given pname 
#
def pname2cpid(pname):

   for cpid in CPIDS:
      if CPIDS[cpid] == pname: return cpid

   return 0

#
# check one or all child processes if they are still running
# pname - unique process name if given
# pid - check this specified process id if given
# dowait - 0 no wait, 1 wait all done, -1 wait only when all children are running
# return the number of running processes if dowait == 0 or 1
# return the number of none-running processes if dowait == -1
#
def check_child(pname, pid=0, logact=None, dowait=0):
   if logact is None: logact = PgLOG.LOGWRN
   if PGSIG['MPROC'] < 2: return 0   # no child process
   if logact&PgLOG.EXITLG: logact &= ~PgLOG.EXITLG
   ccnt = i = 0
   if dowait < 0: ccnt = 1 if (pid or pname) else PGSIG['MPROC']
   while True:
      pcnt = 0
      if not pid and pname: pid = pname2cpid(pname)
      if pid:
         if check_process(pid):   # process is not done yet
            if pname:
               PgLOG.pglog("{}({}): Child still running".format(pname, pid), logact)
            else:
               PgLOG.pglog("{}: Child still running".format(pid), logact)
            pcnt = 1
         elif pid in CPIDS:
            del CPIDS[pid]   # clean the saved info for the process
      elif not pname:
         cpids = list(CPIDS)
         for cpid in cpids:
            if check_process(cpid):  # process is not done yet
               pcnt += 1
            elif cpid in CPIDS:
               del CPIDS[cpid]
      if pcnt == 0 or dowait == 0 or pcnt < ccnt: break
      show_wait_message(i, "{}: wait {}/{} child processes".format(PGSIG['DSTR'], pcnt, PGSIG['MPROC']), logact, dowait)
      i += 1
   return (ccnt - pcnt) if ccnt else pcnt

#
# start this process in none daemon mode
#
# aname - application name, or daemon name
#  cact - short action name
# uname - user login name to started the application
# mproc - upper limit of muiltiple child processes
# wtime - waiting time (in seconds) for next process
#
def start_none_daemon(aname, cact = None, uname = None, mproc = 1, wtime = 120, logon = 1, bproc = 1):

   dstr = aname
   if cact: dstr += " for Action " + cact
   if uname:
      dstr +=  " By " + uname
      check_vuser(uname, aname)

   signal.signal(signal.SIGQUIT, signal_catch)   # catch quit signal only
   signal.signal(signal.SIGCHLD, clean_dead_child)
   PGSIG['DSTR'] = dstr
   PGSIG['DNAME'] = aname
   PGSIG['PPID'] = 0
   PGSIG['PID'] = os.getpid()
   PGSIG['MPROC'] = mproc
   PGSIG['BPROC'] = bproc
   PgLOG.PGLOG['CMDTIME'] = PGSIG['WTIME'] = get_wait_time(wtime, 120, "Polling Wait Time")
   if PGSIG['MPROC'] > 1:
      PgLOG.cmdlog("starts non-daemon {}(ML={},WI={})".format(aname, PGSIG['MPROC'], PGSIG['WTIME']))
      if not logon: PgLOG.PGLOG['LOGMASK'] &= ~PgLOG.MSGLOG  # turn off message logging

#
# check one process id other than the current one if it is still running
# pid - specified process id
# pmsg - process message if given
#
def check_process(pid):
   try:
      os.kill(pid, 0)
   except OSError:
      return 0
   return 1

#
# check a process id on give host
#
def check_host_pid(host, pid, pmsg=None, logact=None):
   if logact is None: logact = PgLOG.LOGWRN
   cmd = 'rdaps'
   if host: cmd += " -h " + host
   cmd += " -p {}".format(pid)
   buf = PgLOG.pgsystem(cmd, logact, 276)   # 4+16+256
   if not buf: return (-1 if PgLOG.PGLOG['SYSERR'] else 0)
   if pmsg: PgLOG.pglog(pmsg, logact&(~PgLOG.EXITLG))
   return 1

#
# check one process id on a given host name if it is still running, with default timeout
#   pid - specified process id
#  ppid - specified parent process id
# uname - user login name who started the daemon
#  host - host name the pid supposed to be running on
# aname - application name
#  pmsg - process message if given
#
# return 1 if process is steal live, 0 died already, -1 error checking
#
def check_host_process(host, pid, ppid=0, uname=None, aname=None, pmsg=None, logact=None):
   if logact is None: logact = PgLOG.LOGWRN
   cmd = "rdaps"
   if host: cmd += " -h " + host
   if pid: cmd += " -p {}".format(pid)
   if ppid: cmd += " -P {}".format(ppid)
   if uname: cmd += " -u " + uname
   if aname: cmd += " -a " + aname
   buf = PgLOG.pgsystem(cmd, logact, 276)   # 4+16+256
   if not buf: return (-1 if PgLOG.PGLOG['SYSERR'] else 0)
   if pmsg: PgLOG.pglog(pmsg, logact&(~PgLOG.EXITLG))
   return 1

#
# get a single pbs status record via qstat
#
def get_pbs_info(qopts, multiple=0, logact=0, chkcnt=1):
   stat = {}
   loop = 0
   buf = None
   while loop < chkcnt:
      buf = PgLOG.pgsystem("qstat -n -w {}".format(qopts), logact, 16)
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
            ckeys = re.split(r'\s+', PgLOG.pgtrim(line))
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
         vals = re.split(r'\s+', PgLOG.pgtrim(line))
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

#
# check status of a pbs batch id
#   bid - specified batch id
#
# return hash of batch status, 0 if cannot check any more
#
def check_pbs_status(bid, logact=None):
   if logact is None: logact = PgLOG.LOGWRN
   stat = {}
   buf = PgLOG.pgsystem("qhist -w -j {}".format(bid), logact, 20)
   if not buf: return stat
   chkt = 1
   lines = buf.split('\n')
   for line in lines:
      if chkt:
         if line.startswith('Job'):
            line = re.sub(r'^Job ID', 'JobID', line, 1)
            line = re.sub(r'Finish Time', 'FinishTime', line, 1)
            line = re.sub(r'Req Mem', 'ReqMem', line, 1)
            line = re.sub(r'Used Mem\(GB\)', 'UsedMem(GB)', line, 1)
            line = re.sub(r'Avg CPU \(%\)', 'AvgCPU(%)', line, 1)
            line = re.sub(r'Elapsed \(h\)', 'WallTime(h)', line, 1)
            line = re.sub(r'Job Name', 'JobName', line, 1)
            ckeys = re.split(r'\s+', PgLOG.pgtrim(line))
            ckeys[1] = 'UserName'
            kcnt = len(ckeys)
            chkt = 0
      else:
         vals = re.split(r'\s+', PgLOG.pgtrim(line))
         for i in range(kcnt):
            stat[ckeys[i]] = vals[i]
         break
   return stat

#
# check if a pbs batch id is live
#   bid - specified batch id
#
# return 1 if process is steal live, 0 died already or error checking
#
def check_pbs_process(bid, pmsg=None, logact=None):
   if logact is None: logact = PgLOG.LOGWRN
   stat = get_pbs_info(bid, 0, logact)
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
   if pmsg: PgLOG.pglog(pmsg, logact&~PgLOG.EXITLG)
   return ret

#
# get wait time
#
def get_wait_time(wtime, default, tmsg):
   if not wtime: wtime = default   # use default time
   if type(wtime) is int: return wtime
   if re.match(r'^(\d*)$', wtime): return int(wtime)
   ms = re.match(r'^(\d*)([DHMS])$', wtime, re.I)
   if ms:
      ret = int(ms.group(1))
      unit = ms.group(2)
   else:
      PgLOG.pglog("{}: '{}' NOT in (D,H,M,S)".format(wtime, tmsg), PgLOG.LGEREX)
      return default  # Bug fix: LGEREX exits, but add fallback so 'unit' is always defined
   if unit != 'S':
      ret *= 60   # seconds in a minute
      if unit != 'M':
         ret *= 60   # minutes in an hour
         if unit != 'H':
            ret *= 24   # hours in a day
   return ret   # in seconds

#
# start a background process and record its id; check PgLOG.pgsystem() in PgLOG.pm for
# valid cmdopt values
#
def start_background(cmd, logact=None, cmdopt=5, dowait=0):
   if logact is None: logact = PgLOG.LOGWRN
   if PGSIG['BPROC'] < 2: return PgLOG.pgsystem(cmd, logact, cmdopt)  # no background
   act = logact&(~PgLOG.EXITLG)
   if act&PgLOG.MSGLOG: act |= PgLOG.FRCLOG  # make sure background calls always logged
   if len(CBIDS) >= PGSIG['BPROC']:
      i = 0
      while True:
         bcnt = check_background(None, 0, act)
         if bcnt < PGSIG['BPROC']: break
         if dowait:
            show_wait_message(i, "{}-{}: wait any {} background calls".format(PGSIG['DSTR'], cmd, bcnt), act, dowait)
            i += 1
         else:
            return PgLOG.pglog("{}-{}: {} background calls already at {}".format(PGSIG['DSTR'], cmd, bcnt, PgLOG.current_datetime()), act)
   cmdlog = (act if cmdopt&1 else PgLOG.WARNLG)
   if cmdopt&8:
      PgLOG.cmdlog("starts '{}'".format(cmd), None, cmdlog)
   else:
      PgLOG.pglog("{}({})-{} >{} &".format(PgLOG.PGLOG['HOSTNAME'], os.getpid(), PgLOG.current_datetime(), cmd), cmdlog)
   bckcmd = cmd
   if cmdopt&2:
      bckcmd += " >> {}/{}".format(PgLOG.PGLOG['LOGPATH'], PgLOG.PGLOG['LOGFILE'])
   if cmdopt&4:
      if not PgLOG.PGLOG['ERRFILE']:
         PgLOG.PGLOG['ERRFILE'] = re.sub(r'\.log$', '.err', PgLOG.PGLOG['LOGFILE'], 1)
      bckcmd += " 2>> {}/{}".format(PgLOG.PGLOG['LOGPATH'], PgLOG.PGLOG['ERRFILE'])
   bckcmd += " &"
   # shell=True is required for the redirections (>> / 2>>) and trailing '&';
   # the '&' makes the shell fork the command and exit, so the command gets
   # reparented to init (ppid=1), matching the lookup logic in record_background().
   subprocess.Popen(bckcmd, shell=True)
   return record_background(cmd, logact)

#
# get background process id for given bcmd 
#
def bcmd2cbid(bcmd):

   for cbid in CBIDS:
      if CBIDS[cbid] == bcmd: return cbid

   return 0

#
# check one or all child processes if they are still running
# bid - check this specified background process id if given
# return the number of processes are still running
#
def check_background(bcmd, bid=0, logact=None, dowait=0):
   if logact is None: logact = PgLOG.LOGWRN
   if PGSIG['BPROC'] < 2: return 0  # no background process
   if logact&PgLOG.EXITLG: logact &= ~PgLOG.EXITLG
   if not bid and bcmd: bid = bcmd2cbid(bcmd)
   bcnt = i = 0
   while True:
      if bid:
         if check_process(bid):  # process is not done yet
            if bcmd:
               PgLOG.pglog("{}({}): Background process still running".format(bcmd, bid), logact)
            else:
               PgLOG.pglog("{}: Background process still running".format(bid), logact)
            bcnt = 1
         elif bid in CBIDS:
            del CBIDS[bid]   # clean the saved info for the process
      elif not bcmd:
         # Bug fix: cannot delete from a dict while iterating over it;
         # iterate over a copy of the keys instead.
         cbids = list(CBIDS)
         for bid in cbids:
            if check_process(bid):  # process is not done yet
               bcnt += 1
            else:
               del CBIDS[bid]
      if not (bcnt and dowait): break
      show_wait_message(i, "{}: wait {}/{} background processes".format(PGSIG['DSTR'], bcnt, PGSIG['MPROC']), logact, dowait)
      i += 1
      bcnt = 0
   return bcnt

#
# check and record process id for background command; return 1 if success full;
# 0 otherwise; -1 if done already
#
def record_background(bcmd, logact=None):
   if logact is None: logact = PgLOG.LOGWRN
   ms = re.match(r'^(\S+)', bcmd)
   aname = ms.group(1) if ms else bcmd
   curuid = PgLOG.PGLOG['CURUID']
   commonuser = PgLOG.PGLOG['COMMONUSER']
   for i in range(2):
      for proc in psutil.process_iter(['pid', 'ppid', 'username', 'cmdline']):
         try:
            info = proc.info
            if info.get('ppid') != 1: continue
            uid = info.get('username')
            if uid != curuid and uid != commonuser: continue
            cmdline = info.get('cmdline') or []
            if not cmdline: continue
            line = ' '.join(cmdline)
            idx = line.find(aname)
            if idx < 0: continue
            bid = info['pid']
            if bid in CBIDS: return -1
            acmd = line[idx+len(aname):]
            if uid == commonuser:
               acmd = re.sub(r'^\.(pl|py)\s+', '', acmd, 1)
            if re.match(r'^{}{}'.format(aname, acmd), bcmd): continue
            CBIDS[bid] = bcmd
            return 1
         except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
      time.sleep(2)
   return 0

#
# sleep for given period for the daemon, stops if maximum running time reached
#
def sleep_daemon(wtime = 0, mtime = None):

   if not wtime: wtime = PGSIG['WTIME']
   if mtime is None: mtime = PGSIG['MTIME']
   
   if mtime > 0:
      rtime = int(time.time()) - PGSIG['STIME']
      if rtime >= mtime:
         PGSIG['QUIT'] = 1
         wtime = 0

   if wtime: time.sleep(wtime)
   return wtime

#
# show wait message every dintv and then sleep for PGSIG['WTIME']
#
def show_wait_message(loop, msg, logact=None, dowait=0):
   if logact is None: logact = PgLOG.LOGWRN
   if loop > 0 and (loop%30) == 0:
      PgLOG.pglog("{} at {}".format(msg, PgLOG.current_datetime()), logact)
   if dowait: time.sleep(PGSIG['WTIME'])

#
# register a time out function to raise a time out error
#
@contextmanager
def pgtimeout(seconds = 0, logact = 0):
   
   if not seconds: seconds = PgLOG.PGLOG['TIMEOUT']
   signal.signal(signal.SIGALRM, raise_pgtimeout)
   signal.alarm(seconds)
   try:
      yield
   except TimeoutError as e:
      pass
   finally:
      signal.signal(signal.SIGALRM, signal.SIG_IGN)

def raise_pgtimeout(signum, frame):
    raise TimeoutError

def timeout_func():
   with pgtimeout(1):
       print('entering block')
       time.sleep(10)
       print('This should never get printed because the line before timed out')
