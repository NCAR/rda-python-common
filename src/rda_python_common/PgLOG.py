#
###############################################################################
#
#     Title : PgLOG.py  -- Module for logging messages.
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 03/02/2016
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : Python library module to log message and also do other things
#             according to the value of logact, like display the error
#             message on screen and exit script
#
#    Github : https://github.com/NCAR/rda-python-common.git
# 
###############################################################################

import sys
import os
import re
import pwd
import grp
import shlex
from subprocess import Popen, PIPE
from os import path as op
import time
import socket
import shutil
import traceback

# define some constants for logging actions
MSGLOG = (0x00001)   # logging message
WARNLG = (0x00002)   # show logging message as warning 
EXITLG = (0x00004)   # exit after logging 
LOGWRN = (0x00003)   # MSGLOG|WARNLG
LOGEXT = (0x00005)   # MSGLOG|EXITLG
WRNEXT = (0x00006)   # WARNLG|EXITLG
LGWNEX = (0x00007)   # MSGLOG|WARNLG|EXITLG
EMLLOG = (0x00008)   # append message to email buffer
LGWNEM = (0x0000B)   # MSGLOG|WARNLG|EMLLOG
LWEMEX = (0x0000F)   # MSGLOG|WARNLG|EMLLOG|EXITLG
ERRLOG = (0x00010)   # error log only, output to STDERR
LOGERR = (0x00011)   # MSGLOG|ERRLOG
LGEREX = (0x00015)   # MSGLOG|ERRLOG|EXITLG
LGEREM = (0x00019)   # MSGLOG|ERRLOG|EMLLOG
DOLOCK = (0x00020)   # action to lock table record(s)
ENDLCK = (0x00040)   # action to end locking table record(s)
AUTOID = (0x00080)   # action to retrieve the last auto added id
DODFLT = (0x00100)   # action to set empty values to default ones
SNDEML = (0x00200)   # action to send email now
RETMSG = (0x00400)   # action to return the message back
FRCLOG = (0x00800)   # force logging message
SEPLIN = (0x01000)   # add a separating line for email/STDOUT/STDERR
BRKLIN = (0x02000)   # add a line break for email/STDOUT/STDERR
EMLTOP = (0x04000)   # prepend message to email buffer
RCDMSG = (0x00814)   # make sure to record logging message
MISLOG = (0x00811)   # cannot access logfile
EMLSUM = (0x08000)   # record as email summary
EMEROL = (0x10000)   # record error as email only
EMLALL = (0x1D208)   # all email acts
DOSUDO = (0x20000)   # add 'sudo -u PGLOG['RDAUSER']' 
NOTLOG = (0x40000)   # do not log any thing
OVRIDE = (0x80000)   # do override existing file or record
NOWAIT = (0x100000)  # do not wait on globus task to finish
ADDTBL = (0x200000)  # action to add a new table if it does not exist
SKPTRC = (0x400000)  # action to skip tracing when log errors
UCNAME = (0x800000)  # action to change query field names to upper case
UCLWEX = (0x800015)  # UCNAME|MSGLOG|WARNLG|EXITLG

SUCCESS = 1   # Successful function call
FINISH  = 2   # go through a function, including time out
FAILURE = 0   # Unsuccessful function call

PGLOG = {   # more defined in untaint_suid() with environment variables
   'EMLADDR' : '',
   'CCDADDR' : '',
   'SEPLINE' : "===========================================================\n",
   'TWOGBS'  : 2147483648,
   'ONEGBS'  : 1073741824,
   'MINSIZE' : 100,        # minimal file size in bytes to be valid
   'LOGMASK' : (0xFFFFFF),  # log mask to turn off certain log action bits
   'BCKGRND' : 0,          # background process flag -b
   'ERRCNT'  : 0,          # record number of errors for email
   'ERRMSG'  : '',         # record error message for email
   'SUMMSG'  : '',         # record summary message for email
   'EMLMSG'  : '',         # record detail message for email
   'PRGMSG'  : '',         # record progressing message for email, replaced each time
   'GMTZ'    : 0,          # 0 - use local time, 1 - use greenwich mean time
   'NOLEAP'  : 0,          # 1 - skip 29 of Feburary while add days to date
   'GMTDIFF' : 6,          # gmt is 6 hours ahead of us
   'CURUID'  : None,       # the login name who executes the program
   'SETUID'  : '',         # the login name for suid if it is different to the CURUID
   'FILEMODE': 0o664,      # default 8-base file mode
   'EXECMODE': 0o775,      # default 8-base executable file mode or directory mode
   'ARCHHOST': "hpss",     # change to hpss from mss
   'ARCHROOT': "/FS/DECS",      # root path for segregated tape on hpss
   'BACKROOT': "/DRDATA/DECS",  # backup path for desaster recovering tape on hpss
   'OLDAROOT': "/FS/DSS",       # old root path on hpss
   'OLDBROOT': "/DRDATA/DSS",   # old backup tape on hpss 
   'RDAUSER' : "rdadata",  # common rda user name
   'RDAEMAIL' : "zji",     # specialist to receipt email intead of common rda user name
   'SUDORDA' : 0,          # 1 to allow sudo to PGLOG['RDAUSER']
   'HOSTNAME' : '',        # current host name the process in running on
   'OBJCTSTR' : "object",
   'BACKUPNM' : "quasar",
   'DRDATANM' : "drdata",
   'GPFSNAME' : "glade",
   'SLMNAME' : "SLURM",
   'PBSNAME' : "PBS",
   'DSIDCHRS' : "d",
   'DOSHELL' : False,
   'NEWDSID' : True,
   'BCHHOSTS' : "PBS",
   'HOSTTYPE' : 'dav',    # default HOSTTYPE
   'EMLMAX' : 256,       # up limit of email line count 
   'PGBATCH' : '',       # current batch service name, SLURM or PBS
   'PGBINDIR' : '',
   'SLMTIME' : 604800,   # max runtime for SLURM bath job, (7x24x60x60 seconds)
   'PBSTIME' : 86400,    # max runtime for SLURM bath job, (7x24x60x60 seconds)
   'MSSGRP'  : None,     # set if set to different HPSS group
   'RDAGRP'  : "decs",
   'DSCHECK' : None,     # carry some cached dscheck information
   'PGDBBUF' : None,      # reference to a connected database object
   'HPSSLMT' : 10,       # up limit of HPSS streams 
   'NOQUIT'  : 0,        # do not quit if this flag is set for daemons
   'DBRETRY' : 2,        # db retry count after error
   'TIMEOUT' : 15,       # default timeout (in seconds) for tosystem()
   'CMDTIME' : 120,      # default command time (in seconds) for pgsystem() to record end time
   'SYSERR'  : None,     # cache the error message generated inside pgsystem()
   'ERR2STD' : [],       # if non-empty reference to array of strings, change stderr to stdout if match
   'STD2ERR' : [],       # if non-empty reference to array of strings, change stdout to stderr if match
   'MISSFILE': "No such file or directory",
   'GITHUB' : "https://github.com"  # github server
}

HOSTTYPES = {
   'rda' : 'dsg_mach',
   'casper' : 'dav',
   'crhtc' : 'dav',
   'cron' : 'dav',
   'cheyenne' : 'ch',
   'chadmin'  : 'ch'
}

CPID = {
   'PID' : "",
   'CTM' : int(time.time()),
   'CMD' : "",
   'CPID' : "",
}

BCHCMDS = {'PBS' : 'qsub', 'SLURM' : 'sbatch'}

# global dists to cashe information
COMMANDS = {}
SLMHOSTS = []
SLMSTATS = {}
PBSHOSTS = []
PBSSTATS = {}

#
# get time string in format YYMMDDHHNNSS for given ctime; or current time if ctime is 0
#
def current_datetime(ctime = 0):

   if PGLOG['GMTZ']:
      dt = time.gmtime(ctime) if ctime else time.gmtime()
   else:
      dt = time.localtime(ctime) if ctime else time.localtime()

   return "{:02}{:02}{:02}{:02}{:02}{:02}".format(dt[0], dt[1], dt[2], dt[3], dt[4], dt[5])

#
# get an environment variable and untaint it
#
def get_environment(name, default = None, logact = 0):
 
   env = os.getenv(name, default)
   if env is None and logact:
      pglog(name + ": Environment variable is not defined", logact)

   return env
   
#
# cache the msg string to global email entries for later call of send_email()  
#
def set_email(msg, logact = 0):

   if logact and msg:
      if logact&EMLTOP:
         if PGLOG['PRGMSG']:
            msg = PGLOG['PRGMSG'] + "\n" + msg
            PGLOG['PRGMSG'] = ""
         if PGLOG['ERRCNT'] == 0:
            if not re.search(r'\n$', msg): msg += "!\n" 
         else:
            if PGLOG['ERRCNT'] == 1:
               msg += " with 1 Error:\n"
            else:
               msg += " with {} Errors:\n".format(PGLOG['ERRCNT'])
            msg +=  break_long_string(PGLOG['ERRMSG'], 512, None, PGLOG['EMLMAX']/2, None, 50, 25)
            PGLOG['ERRCNT'] = 0
            PGLOG['ERRMSG'] = ''

         if PGLOG['SUMMSG']:
            msg += PGLOG['SEPLINE']
            if PGLOG['SUMMSG']: msg += "Summary:\n"
            msg += break_long_string(PGLOG['SUMMSG'], 512, None, PGLOG['EMLMAX']/2, None, 50, 25)

         if PGLOG['EMLMSG']:
            msg += PGLOG['SEPLINE']
            if PGLOG['SUMMSG']: msg += "Detail Information:\n"

         PGLOG['EMLMSG'] = msg + break_long_string(PGLOG['EMLMSG'], 512, None, PGLOG['EMLMAX'], None, 50, 40)
         PGLOG['SUMMSG'] = ""   # in case not
      else:
         if logact&ERRLOG:      # record error for email summary
            PGLOG['ERRCNT'] += 1
            PGLOG['ERRMSG'] += "{}. {}".format(PGLOG['ERRCNT'], msg)
         elif logact&EMLSUM:
            if PGLOG['SUMMSG']:
               if logact&BRKLIN: PGLOG['SUMMSG'] += "\n"
               if logact&SEPLIN: PGLOG['SUMMSG'] += PGLOG['SEPLINE']
            PGLOG['SUMMSG'] += msg    # append

         if logact&EMLLOG:
            if PGLOG['EMLMSG']:
               if logact&BRKLIN: PGLOG['EMLMSG'] += "\n"
               if logact&SEPLIN: PGLOG['EMLMSG'] += PGLOG['SEPLINE']
            PGLOG['EMLMSG'] += msg    # append
   elif msg is None:
      PGLOG['EMLMSG'] = ""

#
# retrieve the cached email message
#
def get_email():
   return PGLOG['EMLMSG']

#
#  send a customized email with all entries included
#
def send_customized_email(logmsg, emlmsg, logact = 0):

   entries = {
      'fr' : ["From",    1, None],
      'to' : ["To",      1, None],
      'cc' : ["Cc",      0, None],
      'sb' : ["Subject", 1, None]
   }

   if logmsg:
      logmsg += ': '
   else:
      logmsg = ''
   for ekey in entries:
      entry = entries[ekey][0]
      if re.search(r'(^|\n){}:\s*\n'.format(entry), emlmsg, re.I):
         ms = None
      else:
         ms = re.search(r'(^|\n){}:\s*(.+)\n'.format(entry), emlmsg, re.I)
      if ms:
         entries[ekey][2] = ms.group(2)
      elif logact and entries[ekey][1]:
         return pglog("{}Missing Entry '{}' for sending email".format(logmsg, entry), logact|ERRLOG)

   logmsg += "Email " + entries['to'][2]
   if entries['cc'][2]: logmsg += " Cc'd " + entries['cc'][2]
   logmsg += " Subject: " + entries['sb'][2]

   if pgsystem(PGLOG['EMLSEND'], logact, 4, emlmsg):
      log_email(emlmsg)
      if logact: pglog(logmsg, logact&(~EXITLG))
      return SUCCESS
   else:
      errmsg = "Error sending email: " + logmsg
      return pglog(logmsg, (logact|ERRLOG)&~EXITLG)

#
#  send an email, if empty msg; send email message saved in PGLOG['EMLMSG'] instead
#
def send_email(subject = None, receiver = None, msg = None, sender = None, logact = 0):

   if not msg:
      if PGLOG['EMLMSG']:
         msg = PGLOG['EMLMSG']
         PGLOG['EMLMSG'] = ''
      else:
         return ''

   docc = 0
   if not sender:
      sender = PGLOG['CURUID']
      if sender != PGLOG['RDAUSER']: docc = 1
   if sender == PGLOG['RDAUSER']: sender = PGLOG['RDAEMAIL']
   if sender.find('@') == -1: sender += "@ucar.edu"
   if not receiver:
      receiver = PGLOG['EMLADDR'] if PGLOG['EMLADDR'] else PGLOG['CURUID']
   if receiver == PGLOG['RDAUSER']: receiver = PGLOG['RDAEMAIL']
   if receiver.find('@') == -1: receiver += "@ucar.edu"

   if docc and not re.match(PGLOG['RDAUSER'], sender): add_carbon_copy(sender, 1) 

   emlmsg = "From: {}\nTo: {}\n".format(sender, receiver)
   logmsg = "Email " + receiver
   if PGLOG['CCDADDR']:
      emlmsg += "Cc: {}\n".format(PGLOG['CCDADDR'])
      logmsg += " Cc'd " + PGLOG['CCDADDR']
   if not subject: subject = "Message from {}-{}".format(PGLOG['HOSTNAME'], get_command())
   if not re.search(r'!$', subject): subject += '!'
   emlmsg += "Subject: {}\n{}\n".format(subject, msg)
   if CPID['CPID']: logmsg += " in " + CPID['CPID']
   logmsg += ", Subject: {}\n".format(subject)

   if pgsystem(PGLOG['EMLSEND'], logact, 4, emlmsg):
      log_email(emlmsg)
      if logact: pglog(logmsg, logact&~EXITLG)
      return logmsg
   else:
      errmsg = "Error sending email: " + logmsg
      pglog(logmsg, (logact|ERRLOG)&~EXITLG)
      return errmsg

#
# log email sent
#
def log_email(emlmsg):

   if not CPID['PID']: CPID['PID'] =  "{}-{}-{}".format(PGLOG['HOSTNAME'], get_command(), PGLOG['CURUID'])
   cmdstr = "{} {} at {}\n".format(CPID['PID'], break_long_string(CPID['CMD'], 40, "...", 1), current_datetime())
   fn = "{}/{}".format(PGLOG['LOGPATH'], PGLOG['EMLFILE'])
   f = open(fn, 'a')
   f.write(cmdstr + emlmsg)
   f.close()

#
# Function: cmdlog(cmdline)
# cmdline - program name and all arguments
# ctime - time (in seconds) when the command starts 
#
def cmdlog(cmdline = None, ctime = 0, logact = None):

   if logact is None: logact = MSGLOG|FRCLOG
   if not ctime: ctime = int(time.time())

   if not cmdline or re.match('(end|quit|exit|abort)', cmdline, re.I):
      cmdline = cmdline.capitalize() if cmdline else "Ends"
      cinfo = cmd_execute_time("{} {}".format(CPID['PID'], cmdline), (ctime - CPID['CTM'])) + ": "
      if CPID['CPID']: cinfo += CPID['CPID'] + " <= "
      cinfo += break_long_string(CPID['CMD'], 40, "...", 1)
      if logact: pglog(cinfo, logact)
   else:
      cinfo = current_datetime(ctime)
      if re.match(r'CPID \d+', cmdline):
         CPID['PID'] = "{}({})-{}{}".format(PGLOG['HOSTNAME'], os.getpid(), PGLOG['CURUID'], cinfo)
         if logact: pglog("{}: {}".format(CPID['PID'], cmdline), logact)
         CPID['CPID'] = cmdline
      elif CPID['PID'] and re.match(r'(starts|catches) ', cmdline):
         if logact: pglog("{}: {} at {}".format(CPID['PID'], cmdline,  cinfo), logact)
      else:
         CPID['PID'] = "{}({})-{}{}".format(PGLOG['HOSTNAME'], os.getpid(), PGLOG['CURUID'], cinfo)
         if logact: pglog("{}: {}".format(CPID['PID'], cmdline), logact)
         CPID['CMD'] = cmdline

      CPID['CTM'] = ctime

#
# Function: pglog(msg, logact) return FAILURE or log message if not exit
#   msg  -- message to log
# locact -- logging actions: MSGLOG, WARNLG, ERRLOG, EXITLG, EMLLOG, & SNDEML
#
# log and display message/error and exit program according logact value
#
def pglog(msg, logact = MSGLOG):

   retmsg = None
   logact &= PGLOG['LOGMASK']   # filtering the log actions
   if logact&RCDMSG: logact |= MSGLOG
   if PGLOG['NOQUIT']: logact &= ~EXITLG
   if logact&EMEROL:
      if logact&EMLLOG: logact &= ~EMLLOG
      if not logact&ERRLOG: logact &= ~EMEROL

   if msg: msg = msg.lstrip()  # remove leading whitespaces for logging message

   if logact&EXITLG:
      ext = "Exit 1 in {}\n".format(os.getcwd())
      if not msg:
         msg = ext
      else:
         msg = msg.rstrip()
         msg += "; " + ext
   else:
      if msg and not re.search(r'(\n|\r)$', msg): msg += "\n"
      if logact&RETMSG: retmsg = msg

   if logact&EMLALL:
      if logact&SNDEML or not msg:
         title = (msg if msg else "Message from {}-{}".format(PGLOG['HOSTNAME'], get_command()))
         msg = send_email(title.rstrip())
      elif msg:
         set_email(msg, logact)

   if not msg: return (retmsg if retmsg else FAILURE)

   if logact&EXITLG and (PGLOG['EMLMSG'] or PGLOG['SUMMSG'] or PGLOG['ERRMSG'] or PGLOG['PRGMSG']):
      if not logact&EMLALL: set_email(msg, logact)
      title = "ABORTS {}-{}".format(PGLOG['HOSTNAME'], get_command())
      set_email((("ABORTS " + CPID['PID']) if CPID['PID'] else title), EMLTOP)
      msg += send_email(title)

   if logact&LOGERR: # make sure error is always logged
      msg = break_long_string(msg)
      if logact&(ERRLOG|EXITLG):
         cmdstr = get_error_command(int(time.time()), logact)
         msg = cmdstr + msg

      if not logact&NOTLOG:
         if logact&ERRLOG:
            if not PGLOG['ERRFILE']: PGLOG['ERRFILE'] = re.sub(r'.log$', '.err', PGLOG['LOGFILE'])
            ERR = open("{}/{}".format(PGLOG['LOGPATH'], PGLOG['ERRFILE']), 'a')
            ERR.write(msg)
            if not logact&(EMLALL|SKPTRC): ERR.write(get_call_trace())
            ERR.close()
            if logact&EXITLG:
               LOG = open("{}/{}".format(PGLOG['LOGPATH'], PGLOG['LOGFILE']), "a")
               LOG.write(cmdstr)
               LOG.close()
         else:
            LOG = open("{}/{}".format(PGLOG['LOGPATH'], PGLOG['LOGFILE']), 'a')
            LOG.write(msg)
            LOG.close()

   if not PGLOG['BCKGRND'] and logact&(ERRLOG|WARNLG):
      OUT = sys.stderr if logact&(ERRLOG|EXITLG) else sys.stdout
      if logact&BRKLIN: OUT.write("\n")
      if logact&SEPLIN: OUT.write(PGLOG['SEPLINE'])
      OUT.write(msg)

   
   if logact&EXITLG:
      pgexit(1)
   else:
      return (retmsg if retmsg else FAILURE)

#
# check and disconnet database before exit
#
def pgexit(stat = 0):

   if PGLOG['PGDBBUF']: PGLOG['PGDBBUF'].close()
   sys.exit(stat)

#
# get a command string for error log dump
#
def get_error_command(ctime, logact):

   if not CPID['PID']: CPID['PID'] =  "{}-{}-{}".format(PGLOG['HOSTNAME'], get_command(), PGLOG['CURUID'])
   cmdstr = "{} {}".format((("ABORTS" if logact&ERRLOG else "QUITS") if logact&EXITLG else "ERROR"), CPID['PID'])
   cmdstr = cmd_execute_time(cmdstr, (ctime - CPID['CTM']))
   if CPID['CPID']: cmdstr += " {} <=".format(CPID['CPID'])
   cmdstr += " {} at {}\n".format(break_long_string(CPID['CMD'], 40, "...", 1), current_datetime(ctime))

   return cmdstr

#
# get call trace track
#
def get_call_trace(cut = 1):

   t = traceback.extract_stack()
   n = len(t) - cut
   str = ''
   sep = 'Trace: '
   for i in range(n):
     tc = t[i]
     str += "{}{}({}){}".format(sep, tc[0], tc[1], ("" if tc[2] == '<module>' else "{%s()}" % tc[2]))
     if i == 0: sep = '=>'

   return str + "\n" if str else ""

#
# log message, msg, for degugging processes according to the debug level 
#
def pgdbg(level, msg = None, do_trace = True):

   if not PGLOG['DBGLEVEL']: return     # no further action

   if not isinstance(level, int):
      ms = re.match(r'^(\d+)', level)
      level = int(ms.group(1)) if ms else 0

   levels = [0, 0]
   if isinstance(PGLOG['DBGLEVEL'], int):
      levels[1] = PGLOG['DBGLEVEL']
   else:
      ms = re.match(r'^(\d+)$', PGLOG['DBGLEVEL'])
      if ms:
         levels[1] = int(ms.group(1))
      else:
         ms = re.match(r'(\d*)-(\d*)', PGLOG['DBGLEVEL'])
         if ms:
            levels[0] = int(ms.group(1)) if ms.group(1) else 0
            levels[1] = int(ms.group(2)) if ms.group(2) else 9999
   
   if level > levels[1] or level < levels[0]: return   # debug level is out of range

   if 'DBGPATH' in PGLOG:
      dfile = PGLOG['DBGPATH'] + '/' + PGLOG['DBGFILE']
   else:
      dfile = PGLOG['DBGFILE']
   if not msg:
      pglog("Append debug Info (levels {}-{}) to {}".format(levels[0], levels[1], dfile), WARNLG)
      msg = "DEBUG for " + CPID['PID'] + " "
      if CPID['CPID']: msg += CPID['CPID'] + " <= "
      msg += break_long_string(CPID['CMD'], 40, "...", 1)

   # logging debug info
   DBG = open(dfile, 'a')
   DBG.write("{}:{}\n".format(level, msg))
   if do_trace: DBG.write(get_call_trace())
   DBG.close()

#
# return trimed string (strip leading and trailling spaces); remove comments led by '#' if rmcmt > 0
#
def pgtrim(line, rmcmt = 1):

   if line:
      if rmcmt:
         if re.match(r'^\s*#', line): # comment line
            line = ''
         elif rmcmt > 1:
            ms = re.search(r'^(.+)\s\s+\#', line)
            if ms: line = ms.group(1)   # remove comment and its leading whitespaces
         else:
            ms = re.search(r'^(.+)\s+\#', line)
            if ms: line = ms.group(1)   # remove comment and its leading whitespace

      line = line.strip()  # remove leading and trailing whitespaces

   return line

#
# Function: show_usage(progname: Perl program name to get file "progname.usg")
#
# show program usage in file "PGLOG['PUSGDIR']/progname.usg" on screen with unix
# system function 'pg', exit program when done.
#
def show_usage(progname, opts = None):

   usgname = PGLOG['PUSGDIR'] + '/' + progname + '.usg'
   if opts:
      # show usage for individual option of dsarch
      for opt in opts:
         if opts[opt][0] == 0:
            msg = "Mode"
         elif opts[opt][0] == 1:
            msg = "Single-Value Information"
         elif opts[opt][0] == 2:
            msg = "Multi-Value Information"
         else:
            msg = "Action"

         sys.stdout.write("\nDescription of {} Option -{}:\n".format(msg, opt))
         IN = open(usgname, 'r')
         nilcnt = begin = 0
         for line in IN:
            if begin == 0:
               rx = "  -{} or -".format(opt)
               if re.match(rx, line): begin = 1
            elif re.match(r'^\s*$', line):
               if nilcnt: break
               nilcnt = 1
            else:
               if re.match(r'\d[\.\s\d]', line): break    # section title
               if nilcnt and re.match(r'  -\w\w or -', line): break
               nilcnt = 0
            if begin: sys.stdout.write(line)
         IN.close()
   else:
      os.system("more " + usgname)
   
   pgexit(0)

#
# compare error message to patterns saved in PGLOG['ERR2STD']
# return 1 if matched; 0 otherwise
#
def err2std(line):

   for err in PGLOG['ERR2STD']:
      if line.find(err) > -1: return 1
   return 0

#
# compare message to patterns saved in PGLOG['STD2ERR']
# return 1 if matched; 0 otherwise
#
def std2err(line):

   for out in PGLOG['STD2ERR']:
      if line.find(out) > -1: return 1
   return 0

#
# Function: pgsystem(pgcmd, logact, cmdopt, instr)
#  pgcmd  - Linux system command, can be a string, "ls -l", or a list, ['ls', '-l']
# logact  - logging action option, defaults to PgLOG.LOGWRN
# cmdopt  - command control option, default to 5 (1+4)
#           0 - no command control,
#           1 - log pgcmd (include the sub command calls),
#           2 - log standard output,
#           4 - log error output
#           7 - log all (pgcmd, and standard/error outputs),
#           8 - log command with time,
#          16 - return standard output message upon success
#          32 - log error as standard output
#          64 - force returning FAILURE if called process aborts
#         128 - tries 2 times for failed command before quits
#         256 - cache standard error message
#         512 - log instr & seconds with pgcmd if cmdopt&1
#        1024 - turn on shell
# instr   - input string passing to the command via stdin if not None
# seconds - number of seconds to wait for a timeout process if > 0
#
def pgsystem(pgcmd, logact = LOGWRN, cmdopt = 5, instr = None, seconds = 0):

   ret = SUCCESS
   if not pgcmd: return ret  # empty command

   act = logact&~EXITLG
   if act&ERRLOG: 
      act &= ~ERRLOG
      act |= WARNLG

   if act&MSGLOG: act |= FRCLOG   # make sure system calls always logged
   cmdact = act if cmdopt&1 else 0
   doshell = True if cmdopt&1024 else PGLOG['DOSHELL']

   if isinstance(pgcmd, str):
      cmdstr = pgcmd
      if not doshell and re.search(r'[*?<>|;]', pgcmd): doshell = True
      execmd = pgcmd if doshell else shlex.split(pgcmd)
   else:
      cmdstr = shlex.join(pgcmd)
      execmd = cmdstr if doshell else pgcmd

   if cmdact:
      if cmdopt&8:
         cmdlog("starts '{}'".format(cmdstr), None, cmdact)
      else:
         pglog("> " + cmdstr, cmdact)
      if cmdopt&512 and (instr or seconds):
         msg = ''
         if seconds: msg = 'Timeout = {} Seconds'.format(seconds) 
         if instr: msg += ' With STDIN:\n' + instr
         if msg: pglog(msg, cmdact)
   stdlog = act if cmdopt&2 else 0
   cmdflg = cmdact|stdlog
   abort = -1 if cmdopt&64 else 0
   loops = 2 if cmdopt&128 else 1
   PGLOG['SYSERR'] = error = retbuf = outbuf = errbuf = ''
   for loop in range(1, loops+1):
      last = time.time()
      try:
         if instr:
            FD = Popen(execmd, shell=doshell, stdout=PIPE, stderr=PIPE, stdin=PIPE)
            if seconds:
               outbuf, errbuf = FD.communicate(input=instr.encode(), timeout=seconds)
            else:
               outbuf, errbuf = FD.communicate(input=instr.encode())
         else:
            FD = Popen(execmd, shell=doshell, stdout=PIPE, stderr=PIPE)
            if seconds:
               outbuf, errbuf = FD.communicate(timeout=seconds)
            else:
               outbuf, errbuf = FD.communicate()
      except TimeoutError as e:
         errbuf = str(e)
         FD.kill()
         ret = FAILURE
      except Exception as e:
         errbuf = str(e)
         ret = FAILURE
      else:
         ret = FAILURE if FD.returncode else SUCCESS
         if isinstance(outbuf, bytes): outbuf = str(outbuf, errors='replace')
         if isinstance(errbuf, bytes): errbuf = str(errbuf, errors='replace')

      if errbuf and cmdopt&32:
         outbuf += errbuf
         if cmdopt&256: PGLOG['SYSERR'] = errbuf
         errbuf = ''

      if outbuf:
         lines = outbuf.split('\n')
         for line in lines:
            line = strip_output_line(line.strip())
            if not line: continue
            if PGLOG['STD2ERR'] and std2err(line):
               if cmdopt&260: error += line + "\n"
               if abort == -1 and re.match('ABORTS ', line): abort = 1
            else:
               if re.match(r'^>+ ', line):
                  line = '>' + line
                  if cmdflg: pglog(line, cmdflg)
               elif stdlog:
                  pglog(line, stdlog)
               if cmdopt&16: retbuf += line + "\n"

      if errbuf:
         lines = errbuf.split('\n')
         for line in lines:
            line = strip_output_line(line.strip())
            if not line: continue
            if PGLOG['ERR2STD'] and err2std(line):
               if stdlog: pglog(line, stdlog)
               if cmdopt&16: retbuf += line + "\n"
            else:
               if cmdopt&260: error += line + "\n"
               if abort == -1 and re.match('ABORTS ', line): abort = 1

      if ret == SUCCESS and abort == 1: ret = FAILURE
      end = time.time()
      last = end - last

      if error:
         if ret == FAILURE:
            error = "Error Execute: {}\n{}".format(cmdstr, error)
         else:
            error = "Error From: {}\n{}".format(cmdstr, error)

         if loop > 1: error = "Retry "
         if cmdopt&256: PGLOG['SYSERR'] += error
         if cmdopt&4:
            errlog = (act|ERRLOG)
            if ret == FAILURE and loop >= loops: errlog |= logact
            pglog(error, errlog)

      if last > PGLOG['CMDTIME'] and not re.search(r'(^|/|\s)(dsarch|dsupdt|dsrqst|rdacp|rdasub)\s', cmdstr):
         cmdstr = "> {} Ends By {}".format(break_long_string(cmdstr, 100, "...", 1), current_datetime())
         cmd_execute_time(cmdstr, last, cmdact)

      if ret == SUCCESS or loop >= loops: break
      time.sleep(6)

   if ret == FAILURE and retbuf and cmdopt&272 == 272: 
      if PGLOG['SYSERR']: PGLOG['SYSERR'] += '\n'
      PGLOG['SYSERR'] += retbuf
      retbuf = ''

   return (retbuf if cmdopt&16 else ret)

#
# strip carrage return '\r', but keep ending newline '\n'
#
def strip_output_line(line):

   ms = re.search(r'\r([^\r]+)\r*$', line)
   if ms: return ms.group(1)

   ms = re.search(r'\s\.+\s+(\d+)%\s+', line)
   if ms and int(ms.group(1)) != 100: return None

   return line

#
# show command running time string formated by seconds_to_string_time() 
#
def cmd_execute_time(cmdstr, last, logact = None):

   msg = cmdstr

   if last >= PGLOG['CMDTIME']:   # show running for at least one minute
      msg += " ({})".format(seconds_to_string_time(last))

   if logact:
      return pglog(msg, logact)
   else:
      return msg

#
# convert given seconds to string time with units of S-Second,M-Minute,H-Hour,D-Day
#
def seconds_to_string_time(seconds, showzero = 0):

   msg = ''
   s = m = h = 0

   if seconds > 0:
      s = seconds%60                  # seconds (0-59)
      minutes = int(seconds/60)       # total minutes
      m = minutes%60                  # minutes (0-59)
      if minutes >= 60:
         hours = int(minutes/60)      # total hours
         h = hours%24                 # hours (0-23)
         if hours >= 24:
            msg += "{}D".format(int(hours/24))   # days
         if h: msg += "{}H".format(h)
      if m: msg += "{}M".format(m)
      if s: 
         msg += "%dS"%(s) if isinstance(s, int) else "{:.3f}S".format(s)
   elif showzero:
      msg = "0S"

   return msg

#
#  wrap function to call pgsystem() with a timeout control
#  return FAILURE if error eval or time out
#
def tosystem(cmd, timeout = 0, logact = LOGWRN, cmdopt = 5, instr = None):

   if not timeout: timeout = PGLOG['TIMEOUT']   # set default timeout if missed
   return pgsystem(cmd, logact, cmdopt, instr, timeout)

#
# insert breaks, default to '\n', for every length, default to 1024,
# for long string; return specified number lines if mline given
#
def break_long_string(lstr, limit = 1024, bsign = "\n", mline = 200, bchars = ' &;', minlmt = 20, eline = 0):

   length = len(lstr) if lstr else 0
   if length <= limit: return lstr

   if bsign is None: bsign = "\n"
   if bchars is None: bchars = ' &;'
   addbreak = offset = 0
   retstr = ""
   elines = []
   if eline > mline: eline = mline
   mcnt = mline - eline
   ecnt = 0

   while offset < length:
      bpos = lstr[offset:].find(bsign)
      blen = bpos if bpos > -1 else (length - offset)
      if blen == 0:
         offset += 1
         substr = "" if addbreak else bsign
         addbreak = 0
      elif blen <= limit:
         blen += 1
         substr = lstr[offset:(offset+blen)]
         offset += blen
         addbreak = 0
      else:
         substr = lstr[offset:(offset+limit)]
         bpos = limit - 1
         while bpos > minlmt:
            char = substr[bpos]
            if bchars.find(char) >= 0: break
            bpos -= 1
         if bpos > minlmt:
            bpos += 1
            substr = substr[:bpos]
            offset += bpos
         else:
            offset += limit
         addbreak = 1
         substr += bsign

      if mcnt:
         retstr += substr
         mcnt -= 1
         if mcnt == 0 and eline == 0: break
      elif eline > 0:
         elines.append(substr)
         ecnt += 1
      else:
         break

   if ecnt > 0:
      if ecnt > eline:
         retstr += "..." + bsign
         mcnt = ecnt - eline
      else:
         mcnt = 0

      while mcnt < ecnt:
         retstr += elines[mcnt]
         mcnt += 1

   return retstr

#
# join two paths by remove overlapping directories
# diff = 0: join given pathes
#        1: remove path1 from path2
#
def join_paths(path1, path2, diff = 0):
   
   if not path2: return path1
   if not path1 or not diff and re.match('/', path2): return path2

   if diff:
      ms = re.match(r'{}/(.*)'.format(path1), path2)
      if ms: return ms.group(1)

   adir1 = path1.split('/')
   adir2 = path2.split('/')
   while adir2 and not adir2[0]: adir2.pop(0)
   while adir1 and adir2 and adir2[0] == "..":
      adir2.pop(0)
      adir1.pop()
   while adir2 and adir2[0] == ".": adir2.pop(0)

   if adir1 and adir2:
      len1 = len(adir1)
      len2 = len(adir2)
      idx1 = len1-1
      idx2 = mcnt = 0
      while idx2 < len1 and idx2 < len2:
         if adir1[idx1] == adir2[idx2]:
            mcnt = 1
            break
         idx2 += 1

      if mcnt > 0:
         while mcnt <= idx2:
            if adir1[idx1-mcnt] != adir2[idx2-mcnt]: break
            mcnt += 1

         if mcnt > idx2:  # remove mcnt matching directories
            while mcnt > 0:
               adir2.pop(0)
               mcnt -= 1

   if diff:
      return '/'.join(adir2)
   else:
      return '/'.join(adir1 + adir2)

#
# validate if a command for a given BATCH host is accessable and executable
#
# Return SUCCESS if valid; FAILURE if not
#
def valid_batch_host(host, logact = 0):

   HOST = host.upper()
   return SUCCESS if HOST in BCHCMDS and valid_command(BCHCMDS[HOST], logact) else FAILURE

#
# validate if a given command is accessable and executable
#
# Return the full command path if valid; '' if not
#
def valid_command(cmd, logact = 0):

   ms = re.match(r'^(\S+)( .*)$', cmd)
   if ms:
      option = ms.group(2)
      cmd = ms.group(1)
   else:
      option = ''
   if cmd not in COMMANDS:
      buf = shutil.which(cmd)
      if buf is None:
         if logact: pglog(cmd + ": executable command not found", logact)
         buf = ''
      elif option:
         buf += option
      COMMANDS[cmd] = buf

   return COMMANDS[cmd]

#
# add carbon copies to PGLOG['CCDADDR']
#
def add_carbon_copy(cc = None, isstr = None, exclude = 0, specialist = None):


   if not cc:
      if cc is None and isstr is None: PGLOG['CCDADDR'] = ''
   else:
      emails = re.split(r'[,\s]+', cc) if isstr else cc
      for email in emails:
         if not email or email.find('/') >= 0 or email == 'N': continue
         if email == "S":
            if not specialist: continue
            email = specialist

         if email.find('@') == -1: email += "@ucar.edu"
         if exclude and exclude.find(email) > -1: continue
         if PGLOG['CCDADDR']:
            if PGLOG['CCDADDR'].find(email) > -1: continue   # email Cc'd already
            PGLOG['CCDADDR'] += ", "
         PGLOG['CCDADDR'] += email

#
# get the current host name; or batch sever name if getbatch is 1
#
def get_host(getbatch = 0):

   if getbatch and PGLOG['CURBID'] != 0:
      host = PGLOG['PGBATCH']
   elif PGLOG['HOSTNAME']:
      return PGLOG['HOSTNAME']
   else:
      host = socket.gethostname()
   
   return get_short_host(host)

#
# strip domain names and retrun the server name itself
#
def get_short_host(host):

   if not host: return ''
   ms = re.match(r'^([^\.]+)\.', host)
   if ms: host = ms.group(1)
   if PGLOG['HOSTNAME'] and (host == 'localhost' or host == PGLOG['HOSTNAME']): return PGLOG['HOSTNAME']
   HOST = host.upper()
   if HOST in BCHCMDS: return HOST

   return host

#
# get a live SLURM host name
#
def get_slurm_host():

   global SLMHOSTS

   if not SLMSTATS and PGLOG['SLMHOSTS']:
      SLMHOSTS = PGLOG['SLMHOSTS'].split(':')
      for host in SLMHOSTS:
         SLMSTATS[host] = 1

   for host in SLMHOSTS:
      if host in SLMSTATS and SLMSTATS[host]: return host

   return None

#
# get a live PBS host name
#
def get_pbs_host():

   global PBSHOSTS

   if not PBSSTATS and PGLOG['PBSHOSTS']:
      PBSHOSTS = PGLOG['PBSHOSTS'].split(':')
      for host in PBSHOSTS:
         PBSSTATS[host] = 1

   for host in PBSHOSTS:
      if host in PBSSTATS and PBSSTATS[host]: return host

   return None

#
# set host status, 0 dead & 1 live, for one or all avalaible slurm hosts 
#
def set_slurm_host(host = None, stat = 0):

   global SLMHOSTS

   if host:
      SLMSTATS[host] = stat
   else:
      if not SLMHOSTS and PGLOG['SLMHOSTS']:
         SLMHOSTS = PGLOG['SLMHOSTS'].split(':')
      for host in SLMHOSTS:
         SLMSTATS[host] = stat

#
# set host status, 0 dead & 1 live, for one or all avalaible pbs hosts 
#
def set_pbs_host(host = None, stat = 0):

   global PBSHOSTS

   if host:
      PBSSTATS[host] = stat
   else:
      if not PBSHOSTS and PGLOG['PBSHOSTS']:
         PBSHOSTS = PGLOG['PBSHOSTS'].split(':')
      for host in PBSHOSTS:
         PBSSTATS[host] = stat

#
#  reset the batch host name in case was not set properly
#
def reset_batch_host(bhost, logact = LOGWRN):

   BCHHOST = bhost.upper()

   if BCHHOST != PGLOG['PGBATCH']:
      if PGLOG['CURBID'] > 0:
         pglog("{}-{}: Batch ID is set, cannot change Batch host to {}".format(PGLOG['PGBATCH'], PGLOG['CURBID'], BCHHOST) , logact)
      else:
         ms = re.search(r'(^|:){}(:|$)'.format(BCHHOST), PGLOG['BCHHOSTS'])
         if ms:
            PGLOG['PGBATCH'] = BCHHOST
            if PGLOG['CURBID'] == 0: PGLOG['CURBID'] = -1
         elif PGLOG['PGBATCH']:
            PGLOG['PGBATCH'] = ''
            PGLOG['CURBID'] = 0

#
# return the base command name of the current process
#
def get_command(cmdstr = None):

   if not cmdstr: cmdstr = sys.argv[0]
   cmdstr = op.basename(cmdstr)
   ms = re.match(r'^(.+)\.(py|pl)$', cmdstr)
   if ms:
      return ms.group(1)
   else:
      return cmdstr

#
# wrap a given command cmd for either sudo or setuid wrapper pgstart_['username']
# to run as user asuser
#
def get_local_command(cmd, asuser = None):

   cuser = PGLOG['SETUID'] if PGLOG['SETUID'] else PGLOG['CURUID']
   if not asuser or cuser == asuser: return cmd

   if cuser == PGLOG['RDAUSER']:
      wrapper = "pgstart_" + asuser
      if valid_command(wrapper): return "{} {}".format(wrapper, cmd)
   elif PGLOG['SUDORDA'] and asuser == PGLOG['RDAUSER']:
      return "sudo -u {} {}".format(PGLOG['RDAUSER'], cmd)    # sudo as user rdadata

   return cmd

#
# wrap a given command cmd for either sudo or setuid wrapper pgstart_['username']
# to run as user asuser on a given remote host
#
def get_remote_command(cmd, host, asuser = None):

#   if host and not re.match(PGLOG['HOSTNAME'], host): cmd = "ssh {} {}".format(host, cmd)
   return get_local_command(cmd, asuser)

#
# wrap a given hpss command cmd with sudo either before his of after hsi
# to run as user asuser
#
def get_hpss_command(cmd, asuser = None, hcmd = None):

   cuser = PGLOG['SETUID'] if PGLOG['SETUID'] else PGLOG['CURUID']
   if not hcmd: hcmd = 'hsi'
   
   if asuser and cuser != asuser:
      if cuser == PGLOG['RDAUSER']:
         return "{} sudo -u {} {}".format(hcmd, asuser, cmd)      # setuid wrapper as user asuser
      elif PGLOG['SUDORDA'] and asuser == PGLOG['RDAUSER']:
         return "sudo -u {} {} {}".format(PGLOG['RDAUSER'], hcmd, cmd)    # sudo as user rdadata

   if cuser != PGLOG['RDAUSER']:
      if re.match(r'^ls ', cmd) and hcmd == 'hsi':
         return "hpss" + cmd    # use 'hpssls' instead of 'hsi ls'
      elif re.match(r'^htar -tvf', hcmd):
         hcmd.replace('htar -tvf', 'htarmember', 1)    # use 'htarmember' instead of 'htar -tvf'
      elif re.match(r'^hsi ls', hcmd):
         hcmd.replce('hsi ls', 'hpssls', 1)    # use 'hpssls' instead of 'hsi ls'

   return "{} {}".format(hcmd, cmd)

#
# wrap a given sync command for given host name with/without sudo
#
def get_sync_command(host, asuser = None):

   host = get_short_host(host)
  
   if (not (PGLOG['SETUID'] and PGLOG['SETUID'] == PGLOG['RDAUSER']) and 
      (not asuser or asuser == PGLOG['RDAUSER'])):
      return "sync" + host

   return host + "-sync"

#
# set PGLOG['SETUID'] as needed
#
def set_suid(cuid = 0):

   if not cuid: cuid = PGLOG['EUID']
   if cuid != PGLOG['EUID'] or cuid != PGLOG['RUID']:
      os.setreuid(cuid, cuid)
      PGLOG['SETUID'] = pwd.getpwuid(cuid).pw_name
      if not (PGLOG['SETUID'] == PGLOG['RDAUSER'] or cuid == PGLOG['RUID']):
         set_specialist_environments(PGLOG['SETUID'])
         PGLOG['CURUID'] == PGLOG['SETUID']      # set CURUID to a specific specialist

#
# set comman pglog
#
def set_common_pglog():

   PGLOG['CURDIR'] = os.getcwd()

   # set current user id
   PGLOG['RUID'] = os.getuid()
   PGLOG['EUID'] = os.geteuid()
   PGLOG['CURUID'] = pwd.getpwuid(PGLOG['RUID']).pw_name
   try:
      PGLOG['RDAUID'] = pwd.getpwnam(PGLOG['RDAUSER']).pw_uid
      PGLOG['RDAGID'] = grp.getgrnam(PGLOG['RDAGRP']).gr_gid
   except:
      PGLOG['RDAUID'] = 0
      PGLOG['RDAGID'] = 0
   if PGLOG['CURUID'] == PGLOG['RDAUSER']: PGLOG['SETUID'] = PGLOG['RDAUSER']

   PGLOG['HOSTNAME'] = get_host()
   for htype in HOSTTYPES:
      ms = re.match(r'^{}(-|\d|$)'.format(htype), PGLOG['HOSTNAME'])
      if ms:
         PGLOG['HOSTTYPE'] = HOSTTYPES[htype]
         break
   PGLOG['DEFDSID'] = 'd000000' if PGLOG['NEWDSID'] else 'ds000.0'
   PGLOG['NOTAROOT'] = '|'.join([PGLOG['OLDAROOT'], PGLOG['OLDBROOT'], PGLOG['BACKROOT']])
   PGLOG['NOTBROOT'] = '|'.join([PGLOG['OLDAROOT'], PGLOG['OLDBROOT'], PGLOG['ARCHROOT']])
   PGLOG['ALLROOTS'] = '|'.join([PGLOG['OLDAROOT'], PGLOG['OLDBROOT'], PGLOG['ARCHROOT'], PGLOG['BACKROOT']])
   SETPGLOG("USRHOME", "/glade/u/home")
   SETPGLOG("DSSHOME", "/glade/u/home/rdadata")
   SETPGLOG("ADDPATH", "")
   SETPGLOG("ADDLIB",  "")
   SETPGLOG("OTHPATH", "")
   SETPGLOG("PSQLHOME", "/usr/pgsql-15")
   SETPGLOG("DSGHOSTS", "")
   SETPGLOG("DSIDCHRS", "d")

   if not os.getenv('HOME'): os.environ['HOME'] = "{}/{}".format(PGLOG['USRHOME'], PGLOG['CURUID'])
   SETPGLOG("HOMEBIN", os.environ.get('HOME') + "/bin")

   if 'SLURM_JOBID' in os.environ:
      PGLOG['CURBID'] = int(os.getenv('SLURM_JOBID'))
      PGLOG['PGBATCH'] = PGLOG['SLMNAME']
   elif 'PBS_JOBID' in os.environ:
      sbid = os.getenv('PBS_JOBID')
      ms = re.match(r'^(\d+)', sbid)
      PGLOG['CURBID'] = int(ms.group(1)) if ms else -1
      PGLOG['PGBATCH'] = PGLOG['PBSNAME']
   else:
      PGLOG['CURBID'] = 0
      PGLOG['PGBATCH'] = ''

   pgpath = PGLOG['HOMEBIN']
   PGLOG['LOCHOME'] = "/ncar/rda/setuid"
   if not op.isdir(PGLOG['LOCHOME']): PGLOG['LOCHOME'] = "/usr/local/decs"
   pgpath += ":{}/bin".format(PGLOG['LOCHOME'])
   locpath = "{}/bin/{}".format(PGLOG['DSSHOME'], PGLOG['HOSTTYPE'])
   if op.isdir(locpath): pgpath += ":" + locpath
   pgpath = add_local_path("{}/bin".format(PGLOG['DSSHOME']), pgpath, 1)
   if PGLOG['PSQLHOME']:
      locpath = PGLOG['PSQLHOME'] + "/bin"
      if op.isdir(locpath): pgpath += ":" + locpath
   pgpath = add_local_path(os.getenv('PATH'), pgpath, 1)
   if PGLOG['HOSTTYPE'] == 'dav': pgpath = add_local_path('/glade/u/apps/opt/qstat-cache/bin:/opt/pbs/bin', pgpath, 1)
   if 'OTHPATH' in PGLOG and PGLOG['OTHPATH']:
      pgpath = add_local_path(PGLOG['OTHPATH'], pgpath, 1)
   if PGLOG['ADDPATH']: pgpath = add_local_path(PGLOG['ADDPATH'], pgpath, 1)
   pgpath = add_local_path("/bin:/usr/bin:/usr/local/bin:/usr/sbin", pgpath, 1)

   os.environ['PATH'] = pgpath
   os.environ['SHELL'] = '/bin/sh'
   # set PGLOG values with environments and defaults
   SETPGLOG("DSSDBHM", PGLOG['DSSHOME']+"/dssdb")       # dssdb home dir
   SETPGLOG("LOGPATH", PGLOG['DSSDBHM']+"/log")         # path to log file
   SETPGLOG("LOGFILE", "pgdss.log")                     # log file name
   SETPGLOG("EMLFILE", "pgemail.log")                   # email log file name
   SETPGLOG("ERRFILE", '')                              # error file name
   SETPGLOG("EMLSEND", "/usr/lib/sendmail -t")          # send email command
   SETPGLOG("DBGLEVEL", '')                             # debug level
   SETPGLOG("DBGPATH", PGLOG['DSSDBHM']+"/log")         # path to debug log file
   SETPGLOG("OBJCTBKT", "rda-data")                     # default Bucket on Object Store
   SETPGLOG("BACKUPEP", "rda-quasar")                   # default Globus Endpoint on Quasar
   SETPGLOG("DRDATAEP", "rda-quasar-drdata")            # DRDATA Globus Endpoint on Quasar
   SETPGLOG("DBGFILE", "pgdss.dbg")                     # debug file name
   SETPGLOG("CNFPATH", PGLOG['DSSHOME']+"/config")      # path to configuration files
   SETPGLOG("PUSGDIR", PGLOG['DSSDBHM']+"/prog_usage")  # path to program usage files
   SETPGLOG("DSSURL",  "https://rda.ucar.edu")          # current dss web URL
   SETPGLOG("RQSTURL", "/datasets/request")              # request URL path
   SETPGLOG("WEBSERVERS", "rda-web-prod01.ucar.edu:rda-web-test01.ucar.edu")                 # webserver names for Web server
   PGLOG['WEBHOSTS'] = PGLOG['WEBSERVERS'].split(':') if PGLOG['WEBSERVERS'] else []
   SETPGLOG("DBMODULE", '')
   SETPGLOG("LOCDATA", "/data")

   # set dss web homedir
   SETPGLOG("DSSWEB",  PGLOG['LOCDATA']+"/web")
   SETPGLOG("DSWHOME", PGLOG['DSSWEB']+"/datasets")     # datast web root path
   PGLOG['HOMEROOTS'] = "{}|{}".format(PGLOG['DSSHOME'], PGLOG['DSWHOME'])
   SETPGLOG("DSSDATA", "/glade/campaign/collections/rda")                     # dss data root path
   SETPGLOG("DSDHOME", PGLOG['DSSDATA']+"/data")        # dataset data root path
   SETPGLOG("DECSHOME", PGLOG['DSSDATA']+"/decsdata")   # dataset decsdata root path
   SETPGLOG("DSHHOME", PGLOG['DECSHOME']+"/helpfiles")  # dataset help root path
   SETPGLOG("UPDTWKP", PGLOG['DSSDATA']+"/work")        # dsupdt work root path
   SETPGLOG("TRANSFER", PGLOG['DSSDATA']+"/transfer")   # dss transfer partition
   SETPGLOG("RQSTHOME", PGLOG['TRANSFER']+"/dsrqst")    # dsrqst home
   SETPGLOG("DSAHOME",  "")                    # dataset data alternate root path
   SETPGLOG("RQSTALTH", "")                   # alternate dsrqst path
   SETPGLOG("GPFSHOST", "")                   # empty if writable to glade
   SETPGLOG("PSQLHOST", "rda-db.ucar.edu")                   # host name for postgresql server
   SETPGLOG("SLMHOSTS", "cheyenne:casper")                   # host names for SLURM server
   SETPGLOG("PBSHOSTS", "cheyenne:casper")                   # host names for PBS server
   SETPGLOG("CHKHOSTS", "")                   # host names for dscheck daemon
   SETPGLOG("PVIEWHOST", "rda-pgdb-02.ucar.edu")                   # host name for view only postgresql server
   SETPGLOG("FTPUPLD",  PGLOG['TRANSFER']+"/rossby")    # ftp upload path
   PGLOG['GPFSROOTS'] = "{}|{}|{}".format(PGLOG['DSDHOME'], PGLOG['UPDTWKP'], PGLOG['RQSTHOME'])
 
   if 'ECCODES_DEFINITION_PATH' not in os.environ:
      os.environ['ECCODES_DEFINITION_PATH'] = "/usr/local/share/eccodes/definitions"
   os.environ['history'] = '0'

   # set tmp dir
   SETPGLOG("TMPPATH", "/glade/campaign/collections/rda/work/ptmp")
   if not PGLOG['TMPPATH']: PGLOG['TMPPATH'] = "/data/ptmp"

   SETPGLOG("TMPDIR", '')
   if not PGLOG['TMPDIR']:
      PGLOG['TMPDIR'] = "/glade/campaign/collections/rda/scratch/" + PGLOG['CURUID']
      os.environ['TMPDIR'] = PGLOG['TMPDIR']

   # empty diretory for HOST-sync
   PGLOG['TMPSYNC'] = PGLOG['DSSDBHM'] + "/tmp/.syncdir" 
   if 'DSSHOME' in PGLOG and PGLOG['DSSHOME'] and not op.exists(PGLOG['TMPSYNC']):
      pgsystem("mkdir " + PGLOG['TMPSYNC'], 0, LGWNEX, 4)
      pgsystem("chmod 775 " + PGLOG['TMPSYNC'], LOGWRN, 4)

   os.umask(2)

#
# append or prepend locpath to pgpath
#
def add_local_path(locpath, pgpath, append = 0):

   if not locpath:
      return pgpath
   elif not pgpath:
      return locpath

   paths = locpath.split(':')

   for path in paths:
      if re.match(r'^\./*$', path): continue
      path = path.rstrip('\\')
      ms = re.search(r'(^|:){}(:|$)'.format(path), pgpath)
      if ms: continue
      if append:
         pgpath += ":" + path
      else:
         pgpath = path + ":" + pgpath

   return pgpath

#
# set PGLOG value; return a string or an array reference if sep is not emty
#
def SETPGLOG(name, value = ''):

   oval = PGLOG[name] if name in PGLOG else ''
   nval = get_environment(name, ('' if re.match('PG', value) else value))
   PGLOG[name] = nval if nval else oval

#
# set specialist home and return the default shell
#
def set_specialist_home(specialist):
   
   if specialist == PGLOG['CURUID']: return   # no need reset
   if 'MAIL' in os.environ and re.search(PGLOG['CURUID'], os.environ['MAIL']):
      os.environ['MAIL'] = re.sub(PGLOG['CURUID'], specialist, os.environ['MAIL'])

   home = "{}/{}".format(PGLOG['USRHOME'], specialist)
   shell = "tcsh"
   buf = pgsystem("grep ^{}: /etc/passwd".format(specialist), LOGWRN, 20)
   if buf:
      lines = buf.split('\n')
      for line in lines:
         ms = re.search(r':(/.+):(/.+)', line)
         if ms:
            home = ms.group(1)
            shell = op.basename(ms.group(2))
            break

   if home != os.environ['HOME'] and op.exists(home):
      os.environ['HOME'] = home

   return shell

#
#  set environments for a specified specialist
#
def set_specialist_environments(specialist):

   shell = set_specialist_home(specialist)
   resource = os.environ['HOME'] + "/.tcshrc"
   checkif = 0   # 0 outside of if; 1 start if, 2 check envs, -1 checked already
   missthen = 0
   try:
      rf = open(resource, 'r')
   except:
      return   # skip if cannot open

   nline = rf.readline()
   while nline:
      line = pgtrim(nline)
      nline = rf.readline()
      if not line: continue
      if checkif == 0:
         ms = re.match(r'^if(\s|\()', line)
         if ms: checkif = 1   # start if
      elif missthen:
         missthen = 0
         if re.match(r'^then$', line): continue   # then on next line
         checkif = 0   # end of inline if
      elif re.match(r'^endif', line):
         checkif = 0   # end of if
         continue
      elif checkif == -1:   # skip the line
         continue
      elif checkif == 2 and re.match(r'^else', line):
         checkif = -1   # done check envs in if
         continue

      if checkif == 1:
         if line == 'else':
            checkif = 2
            continue
         elif re.search(r'if\W', line):
            if(re.search(r'host.*!', line, re.I) and not re.search(PGLOG['HOSTNAME'], line) or
               re.search(r'host.*=', line, re.I) and re.search(PGLOG['HOSTNAME'], line)):
               checkif = 2
            if re.search(r'\sthen$', line):
               continue
            else:
               missthen = 1
               if checkif == 1: continue
         else:
            continue

      ms = re.match(r'^setenv\s+(.*)', line)
      if ms: one_specialist_environment(ms.group(1))

   rf.close()

   SETPGLOG("HOMEBIN", PGLOG['PGBINDIR'])
   os.environ['PATH'] = add_local_path(PGLOG['HOMEBIN'], os.environ['PATH'], 0)

#
# set one environment for specialist
#
def one_specialist_environment(line):

   ms = re.match(r'^(\w+)[=\s]+(.+)$', line)
   if not ms: return
   (var, val) = ms.groups()
   if re.match(r'^(PATH|SHELL|IFS|CDPATH|)$', var): return
   if val.find('$') > -1: val = replace_environments(val)
   ms = re.match(r'^(\"|\')(.*)(\"|\')$', val)
   if ms: val = ms.group(2)   # remove quotes
   os.environ[var] = val

#
# get and repalce environment variables in ginve string; defaults to the values in PGLOG
#
def replace_environments(envstr, default = '', logact = 0):

   ishash = isinstance(default, dict)
   ms = re.search(r'(^|.)\$({*)(\w+)(}*)', envstr)
   if ms:
      lead = ms.group(1)
      name = ms.group(3)
      rep = ms.group(2) + name + ms.group(4)
      env = get_environment(name, (PGLOG[name] if name in PGLOG else (default[name] if ishash else default)), logact)
      pre = (lead if (env or lead != ":") else '')
      envstr = re.sub(r'{}\${}'.format(lead, rep), (pre+env), envstr)

   return envstr

#
# validate if the current host is a valid host to process
#
def check_process_host(hosts, chost = None, mflag = None, pinfo = None, logact = None):

   ret = 1
   error = ''
   if not mflag: mflag = 'G'
   if not chost: chost = get_host(1)
 
   if mflag == 'M':    # exact match
      if not hosts or hosts != chost:
         ret = 0
         if pinfo: error = "not matched exactly"      
   elif mflag == 'I':   # inclusive match
      if not hosts or hosts.find('!') == 0 or hosts.find(chost) < 0:
         ret = 0
         if pinfo: error = "not matched inclusively"
   elif hosts:
      if hosts.find(chost) >= 0:
         if hosts.find('!') == 0:
            ret = 0
            if pinfo: error = "matched exclusively" 
      elif hosts.find('!') != 0:
         ret = 0
         if pinfo: error = "not matched"

   if error:
      if logact is None: logact = LOGERR
      pglog("{}: CANNOT be processed on {} for hosthame {}".format(pinfo, chost, error), logact)

   return ret

#
# convert special characters
#
def convert_chars(name, default = 'X'):

   if not name: return default
   if re.match(r'^[a-zA-Z0-9]+$', name): return name  # no need convert

   z = ord('z')
   newchrs = ochrs = ''
   for i in range(len(name)):
      ch = name[i]
      if re.match(r'^[a-zA-Z0-9]$', ch):
         newchrs += ch
      elif (ch == ' ' or ch == '_') and newchrs:
         newchrs += '_'
      elif ord(ch) > z and ochrs != None:
         if not ochrs:
            ochrs = None
            with open(PGLOG['DSSHOME'] + "/lib/ExtChrs.txt", "r") as CHR:
               ochrs = CHR.readline()
               nchrs = CHR.readline()
            if ochrs is None: continue
         idx = ochrs.find(ch)
         if idx >= 0: newchrs += nchrs[idx]

   if newchrs:
      return newchrs
   else:
      return default

#
#  Retrieve host and process id
#
def current_process_info(realpid = 0):
   
   if realpid or PGLOG['CURBID'] < 1:
      return [PGLOG['HOSTNAME'], os.getpid()]
   else:
      return [PGLOG['PGBATCH'], PGLOG['CURBID']]

#
# convert given @ARGV to string. quote the entries with spaces
#
def argv_to_string(argv = None, quote = 1, action = None):

   argstr = ''
   if argv is None: argv = sys.argv[1:]
   for arg in argv:
      if argstr:  argstr += ' '
      ms = re.search(r'([<>\|\s])', arg)
      if ms:
         if action:
            pglog("{}: Cannot {} for special character '{}' in argument value".format(arg, action, ms.group(1)), LGEREX)
         if quote:
            if re.search(r"\'", arg):
               arg = "\"{}\"".format(arg)
            else:
               arg = "'{}'".format(arg)
      argstr += arg

   return argstr

#
# convert an integer to non-10 based string  
#
def int2base(x, base):

   if x == 0: return '0'
   negative = 0
   if x < 0:
      negative = 1
      x = -x

   dgts = []
   while x:
      dgts.append(str(int(x%base)))
      x = int(x/base)
   if negative: dgts.append('-')
   dgts.reverse()

   return ''.join(dgts)
 
#
# convert a non-10 based string to an integer 
#
def base2int(x, base):

   if not isinstance(x, int): x = int(x)
   if x == 0: return 0

   negative = 0
   if x < 0:
      negative = 1
      x = -x

   num = 0
   fact = 1
   while x:
      num += (x%10)*fact
      fact *= base
      x = int(x/10)
   if negative: num = -num

   return num

#
# convert integer to ordinal string
# 
def int2order(num):

   ordstr = ['th', 'st', 'nd', 'rd']
   snum = str(num)
   num %= 100
   if num > 19: num %= 10
   if num > 3: num = 0

   return snum + ordstr[num]

#
# Always call this function to initialize global variables for all applications
#
set_common_pglog()
