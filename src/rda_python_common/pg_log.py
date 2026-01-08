###############################################################################
#     Title: pg_log.py  -- Module for logging messages.
#    Author: Zaihua Ji,  zji@ucar.edu
#      Date: 03/02/2016
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-11-20 convert to class PgLOG
#   Purpose: Python library module to log message and also do other things
#             according to the value of logact, like display the error
#             message on screen and exit script
#    Github: https://github.com/NCAR/rda-python-common.git
###############################################################################
import sys
import os
import re
import pwd
import grp
import shlex
import smtplib
from email.message import EmailMessage
from subprocess import Popen, PIPE
from os import path as op
import time
import socket
import shutil
import traceback
from unidecode import unidecode

class PgLOG:

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
   DOSUDO = (0x20000)   # add 'sudo -u self.PGLOG['GDEXUSER']'
   NOTLOG = (0x40000)   # do not log any thing
   OVRIDE = (0x80000)   # do override existing file or record
   NOWAIT = (0x100000)  # do not wait on globus task to finish
   ADDTBL = (0x200000)  # action to add a new table if it does not exist
   SKPTRC = (0x400000)  # action to skip tracing when log errors
   UCNAME = (0x800000)  # action to change query field names to upper case
   UCLWEX = (0x800015)  # UCNAME|MSGLOG|WARNLG|EXITLG
   PFSIZE = (0x1000000)  # total file size under a path
   SUCCESS = 1   # Successful function call
   FINISH  = 2   # go through a function, including time out
   FAILURE = 0   # Unsuccessful function call

   def __init__(self):
      self.PGLOG = {
         # more defined in untaint_suid() with environment variables
         'EMLADDR': '',
         'CCDADDR': '',
         'SEPLINE': "===========================================================\n",
         'TWOGBS': 2147483648,
         'ONEGBS': 1073741824,
         'MINSIZE': 100,       # minimal file size in bytes to be valid
         'LOGMASK': (0xFFFFFF),  # log mask to turn off certain log action bits
         'BCKGRND': 0,         # background process flag -b
         'ERRCNT': 0,          # record number of errors for email
         'ERRMSG': '',         # record error message for email
         'SUMMSG': '',         # record summary message for email
         'EMLMSG': '',         # record detail message for email
         'PRGMSG': '',         # record progressing message for email, replaced each time
         'GMTZ': 0,            # 0 - use local time, 1 - use greenwich mean time
         'NOLEAP': 0,          # 1 - skip 29 of Feburary while add days to date
         'GMTDIFF': 6,         # gmt is 6 hours ahead of us
         'CURUID': None,       # the login name who executes the program
         'SETUID': '',         # the login name for suid if it is different to the CURUID
         'FILEMODE': 0o664,    # default 8-base file mode
         'EXECMODE': 0o775,    # default 8-base executable file mode or directory mode
         'GDEXUSER': "gdexdata",  # common gdex user name
         'GDEXEMAIL': "zji",    # specialist to receipt email intead of common gdex user name
         'SUDOGDEX': 0,         # 1 to allow sudo to self.PGLOG['GDEXUSER']
         'HOSTNAME': '',        # current host name the process in running on
         'OBJCTSTR': "object",
         'BACKUPNM': "quasar",
         'DRDATANM': "drdata",
         'GPFSNAME': "glade",
         'PBSNAME': "PBS",
         'DSIDCHRS': "d",
         'DOSHELL': False,
         'NEWDSID': True,
         'PUSGDIR': None,
         'BCHHOSTS': "PBS",
         'HOSTTYPE': 'dav',   # default HOSTTYPE
         'EMLMAX': 256,       # up limit of email line count
         'PGBATCH': '',       # current batch service name, PBS
         'PGBINDIR': '',
         'PBSTIME': 86400,    # max runtime for PBS bath job, (24x60x60 seconds)
         'MSSGRP': None,      # set if set to different HPSS group
         'GDEXGRP': "decs",
         'EMLSEND': None,     # path to sendmail, None if not exists
         'DSCHECK': None,     # carry some cached dscheck information
         'PGDBBUF': None,     # reference to a connected database object
         'NOQUIT': 0,         # do not quit if this flag is set for daemons
         'DBRETRY': 2,        # db retry count after error
         'TIMEOUT': 15,       # default timeout (in seconds) for tosystem()
         'CMDTIME': 120,      # default command time (in seconds) for pgsystem() to record end time
         'SYSERR': None,      # cache the error message generated inside pgsystem()
         'ERR2STD': [],       # if non-empty reference to array of strings, change stderr to stdout if match
         'STD2ERR': [],       # if non-empty reference to array of strings, change stdout to stderr if match
         'MISSFILE': "No such file or directory",
         'GITHUB': "https://github.com" , # github server
         'EMLSRVR': "ndir.ucar.edu",   # UCAR email server and port
         'EMLPORT': 25
      }
      self.PGLOG['RDAUSER'] = self.PGLOG['GDEXUSER']
      self.PGLOG['RDAGRP'] = self.PGLOG['GDEXGRP']
      self.PGLOG['RDAEMAIL'] = self.PGLOG['GDEXEMAIL']
      self.PGLOG['SUDORDA'] = self.PGLOG['SUDOGDEX']
      self.HOSTTYPES = {
         'rda': 'dsg_mach',
         'crlogin': 'dav',
         'casper': 'dav',
         'crhtc': 'dav',
         'cron': 'dav',
      }
      self.CPID = {
         'PID': "",
         'CTM': int(time.time()),
         'CMD': "",
         'CPID': "",
      }
      self.BCHCMDS = {'PBS': 'qsub'}
      # global dists to cashe information
      self.COMMANDS = {}
      self.PBSHOSTS = []
      self.PBSSTATS = {}
      # set additional common PGLOG values
      self.set_common_pglog()

   # get time string in format YYMMDDHHNNSS for given ctime; or current time if ctime is 0
   def current_datetime(self, ctime = 0):
      if self.PGLOG['GMTZ']:
         dt = time.gmtime(ctime) if ctime else time.gmtime()
      else:
         dt = time.localtime(ctime) if ctime else time.localtime()
      return "{:02}{:02}{:02}{:02}{:02}{:02}".format(dt[0], dt[1], dt[2], dt[3], dt[4], dt[5])

   # get an environment variable and untaint it
   def get_environment(self, name, default = None, logact = 0):
      env = os.getenv(name, default)
      if env is None and logact:
         self.pglog(name + ": Environment variable is not defined", logact)
      return env

   # cache the msg string to global email entries for later call of send_email()
   def set_email(self,  msg, logact = 0):
      if logact and msg:
         if logact&self.EMLTOP:
            if self.PGLOG['PRGMSG']:
               msg = self.PGLOG['PRGMSG'] + "\n" + msg
               self.PGLOG['PRGMSG'] = ""
            if self.PGLOG['ERRCNT'] == 0:
               if not re.search(r'\n$', msg): msg += "!\n"
            else:
               if self.PGLOG['ERRCNT'] == 1:
                  msg += " with 1 Error:\n"
               else:
                  msg += " with {} Errors:\n".format(self.PGLOG['ERRCNT'])
               msg +=  self.break_long_string(self.PGLOG['ERRMSG'], 512, None, self.PGLOG['EMLMAX']/2, None, 50, 25)
               self.PGLOG['ERRCNT'] = 0
               self.PGLOG['ERRMSG'] = ''
            if self.PGLOG['SUMMSG']:
               msg += self.PGLOG['SEPLINE']
               if self.PGLOG['SUMMSG']: msg += "Summary:\n"
               msg += self.break_long_string(self.PGLOG['SUMMSG'], 512, None, self.PGLOG['EMLMAX']/2, None, 50, 25)
            if self.PGLOG['EMLMSG']:
               msg += self.PGLOG['SEPLINE']
               if self.PGLOG['SUMMSG']: msg += "Detail Information:\n"
            self.PGLOG['EMLMSG'] = msg + self.break_long_string(self.PGLOG['EMLMSG'], 512, None, self.PGLOG['EMLMAX'], None, 50, 40)
            self.PGLOG['SUMMSG'] = ""   # in case not
         else:
            if logact&self.ERRLOG:      # record error for email summary
               self.PGLOG['ERRCNT'] += 1
               if logact&self.BRKLIN: self.PGLOG['ERRMSG'] += "\n"
               self.PGLOG['ERRMSG'] += "{}. {}".format(self.PGLOG['ERRCNT'], msg)
            elif logact&self.EMLSUM:
               if self.PGLOG['SUMMSG']:
                  if logact&self.BRKLIN: self.PGLOG['SUMMSG'] += "\n"
                  if logact&self.SEPLIN: self.PGLOG['SUMMSG'] += self.PGLOG['SEPLINE']
               self.PGLOG['SUMMSG'] += msg    # append
            if logact&self.EMLLOG:
               if self.PGLOG['EMLMSG']:
                  if logact&self.BRKLIN: self.PGLOG['EMLMSG'] += "\n"
                  if logact&self.SEPLIN: self.PGLOG['EMLMSG'] += self.PGLOG['SEPLINE']
               self.PGLOG['EMLMSG'] += msg    # append
      elif msg is None:
         self.PGLOG['EMLMSG'] = ""

   # retrieve the cached email message
   def get_email(self):
      return self.PGLOG['EMLMSG']

   #  send a customized email with all entries included
   def send_customized_email(self, logmsg, emlmsg, logact = None):
      if logact is None: logact = self.LOGWRN
      entries = {
         'fr': ["From",    1, None],
         'to': ["To",      1, None],
         'cc': ["Cc",      0, ''],
         'sb': ["Subject", 1, None]
      }
      if logmsg:
         logmsg += ': '
      else:
         logmsg = ''
      msg = emlmsg
      for ekey in entries:
         entry = entries[ekey][0]
         ms = re.search(r'(^|\n)({}: *(.*)\n)'.format(entry), emlmsg, re.I)
         if ms:
            vals = ms.groups()
            msg = msg.replace(vals[1], '')
            if vals[2]: entries[ekey][2] = vals[2]
         elif entries[ekey][1]:
            return self.pglog("{}Missing Entry '{}' for sending email".format(logmsg, entry), logact|self.ERRLOG)
      ret = self.send_python_email(entries['sb'][2], entries['to'][2], msg, entries['fr'][2], entries['cc'][2], logact)
      if ret == self.SUCCESS or not self.PGLOG['EMLSEND']: return ret   
      # try commandline sendmail
      ret = self.pgsystem(self.PGLOG['EMLSEND'], logact, 4, emlmsg)
      logmsg += "Email " + entries['to'][2]
      if entries['cc'][2]: logmsg += " Cc'd " + entries['cc'][2]
      logmsg += " Subject: " + entries['sb'][2]
      if ret:
         self.log_email(emlmsg)
         self.pglog(logmsg, logact&(~self.EXITLG))
      else:
         errmsg = "Error sending email: " + logmsg
         self.pglog(errmsg, (logact|self.ERRLOG)&~self.EXITLG)
      return ret

   #  send an email; if empty msg send email message saved in self.PGLOG['EMLMSG'] instead
   def send_email(self, subject = None, receiver = None, msg = None, sender = None, logact = None):
      if logact is None: logact = self.LOGWRN
      return self.send_python_email(subject, receiver, msg, sender, None, logact)

   #  send an email via python module smtplib; if empty msg send email message saved
   #  in self.PGLOG['EMLMSG'] instead. pass cc = '' for skipping 'Cc: '
   def send_python_email(self, subject = None, receiver = None, msg = None, sender = None, cc = None, logact = None):
      if logact is None: logact = self.LOGWRN
      if not msg:
         if self.PGLOG['EMLMSG']:
            msg = self.PGLOG['EMLMSG']
            self.PGLOG['EMLMSG'] = ''
         else:
            return ''
      docc = False if cc else True
      if not sender:
         sender = self.PGLOG['CURUID']
         if sender != self.PGLOG['GDEXUSER']: docc = False
      if sender == self.PGLOG['GDEXUSER']: sender = self.PGLOG['GDEXEMAIL']
      if sender.find('@') == -1: sender += "@ucar.edu"
      if not receiver:
         receiver = self.PGLOG['EMLADDR'] if self.PGLOG['EMLADDR'] else self.PGLOG['CURUID']
      if receiver == self.PGLOG['GDEXUSER']: receiver = self.PGLOG['GDEXEMAIL']
      if receiver.find('@') == -1: receiver += "@ucar.edu"
      if docc and not re.match(self.PGLOG['GDEXUSER'], sender): self.add_carbon_copy(sender, 1)
      emlmsg = EmailMessage()
      emlmsg.set_content(msg)
      emlmsg['From'] = sender
      emlmsg['To'] = receiver
      logmsg = "Email " + receiver
      if cc == None: cc = self.PGLOG['CCDADDR']
      if cc:
         emlmsg['Cc'] = cc
         logmsg += " Cc'd " + cc
      if not subject: subject = "Message from {}-{}".format(self.PGLOG['HOSTNAME'], self.get_command())
      # if not re.search(r'!$', subject): subject += '!'
      emlmsg['Subject'] = subject
      if self.CPID['CPID']: logmsg += " in " + self.CPID['CPID']
      logmsg += ", Subject: {}\n".format(subject)
      try:
         eml = smtplib.SMTP(self.PGLOG['EMLSRVR'], self.PGLOG['EMLPORT'])
         eml.send_message(emlmsg)
      except smtplib.SMTPException as err:
         errmsg = f"Error sending email:\n{err}\n{logmsg}"
         return self.pglog(errmsg, (logact|self.ERRLOG)&~self.EXITLG)
      finally:
         eml.quit()
         self.log_email(str(emlmsg))
         self.pglog(logmsg, logact&~self.EXITLG)
         return self.SUCCESS

   # log email sent
   def log_email(self, emlmsg):
      if not self.CPID['PID']: self.CPID['PID'] =  "{}-{}-{}".format(self.PGLOG['HOSTNAME'], self.get_command(), self.PGLOG['CURUID'])
      cmdstr = "{} {} at {}\n".format(self.CPID['PID'], self.break_long_string(self.CPID['CMD'], 40, "...", 1), self.current_datetime())
      fn = "{}/{}".format(self.PGLOG['LOGPATH'], self.PGLOG['EMLFILE'])
      try:
         f = open(fn, 'a')
         f.write(cmdstr + emlmsg)
         f.close()
      except FileNotFoundError as e:
          print(e)
   
   # Function: cmdlog(cmdline)
   # cmdline - program name and all arguments
   # ctime - time (in seconds) when the command starts
   def cmdlog(self, cmdline = None, ctime = 0, logact = None):
      if logact is None: logact = self.MSGLOG|self.FRCLOG
      if not ctime: ctime = int(time.time())
      if not cmdline or re.match('(end|quit|exit|abort)', cmdline, re.I):
         cmdline = cmdline.capitalize() if cmdline else "Ends"
         cinfo = self.cmd_execute_time("{} {}".format(self.CPID['PID'], cmdline), (ctime - self.CPID['CTM'])) + ": "
         if self.CPID['CPID']: cinfo += self.CPID['CPID'] + " <= "
         cinfo += self.break_long_string(self.CPID['CMD'], 40, "...", 1)
         if logact: self.pglog(cinfo, logact)
      else:
         cinfo = self.current_datetime(ctime)
         if re.match(r'CPID \d+', cmdline):
            self.CPID['PID'] = "{}({})-{}{}".format(self.PGLOG['HOSTNAME'], os.getpid(), self.PGLOG['CURUID'], cinfo)
            if logact: self.pglog("{}: {}".format(self.CPID['PID'], cmdline), logact)
            self.CPID['CPID'] = cmdline
         elif self.CPID['PID'] and re.match(r'(starts|catches) ', cmdline):
            if logact: self.pglog("{}: {} at {}".format(self.CPID['PID'], cmdline,  cinfo), logact)
         else:
            self.CPID['PID'] = "{}({})-{}{}".format(self.PGLOG['HOSTNAME'], os.getpid(), self.PGLOG['CURUID'], cinfo)
            if logact: self.pglog("{}: {}".format(self.CPID['PID'], cmdline), logact)
            self.CPID['CMD'] = cmdline
         self.CPID['CTM'] = ctime

   # Function: self.pglog(msg, logact) return self.FAILURE or log message if not exit
   #   msg  -- message to log
   # locact -- logging actions: MSGLOG, WARNLG, ERRLOG, EXITLG, EMLLOG, & SNDEML
   # log and display message/error and exit program according logact value
   def pglog(self, msg, logact = None):
      if logact is None: logact = self.MSGLOG  
      retmsg = None
      logact &= self.PGLOG['LOGMASK']   # filtering the log actions
      if logact&self.RCDMSG: logact |= self.MSGLOG
      if self.PGLOG['NOQUIT']: logact &= ~self.EXITLG
      if logact&self.EMEROL:
         if logact&self.EMLLOG: logact &= ~self.EMLLOG
         if not logact&self.ERRLOG: logact &= ~self.EMEROL
      msg = msg.lstrip() if msg else ''  # remove leading whitespaces for logging message
      if logact&self.EXITLG:
         ext = "Exit 1 in {}\n".format(os.getcwd())
         if msg: msg = msg.rstrip() + "; "
         msg += ext
      else:
         if msg and not re.search(r'(\n|\r)$', msg): msg += "\n"
         if logact&self.RETMSG: retmsg = msg
      if logact&self.EMLALL:
         if logact&self.SNDEML or not msg:
            title = (msg if msg else "Message from {}-{}".format(self.PGLOG['HOSTNAME'], self.get_command()))
            msg = title
            self.send_email(title.rstrip())
         elif msg:
            self.set_email(msg, logact)
      if not msg: return (retmsg if retmsg else self.FAILURE)
      if logact&self.EXITLG and (self.PGLOG['EMLMSG'] or self.PGLOG['SUMMSG'] or self.PGLOG['ERRMSG'] or self.PGLOG['PRGMSG']):
         if not logact&self.EMLALL: self.set_email(msg, logact)
         title = "ABORTS {}-{}".format(self.PGLOG['HOSTNAME'], self.get_command())
         self.set_email((("ABORTS " + self.CPID['PID']) if self.CPID['PID'] else title), self.EMLTOP)
         msg = title + '\n' + msg
         self.send_email(title)   
      if logact&self.LOGERR: # make sure error is always logged
         msg = self.break_long_string(msg)
         if logact&(self.ERRLOG|self.EXITLG):
            cmdstr = self.get_error_command(int(time.time()), logact)
            msg = cmdstr + msg
         if not logact&self.NOTLOG:
            if logact&self.ERRLOG:
               if not self.PGLOG['ERRFILE']: self.PGLOG['ERRFILE'] = re.sub(r'.log$', '.err', self.PGLOG['LOGFILE'])
               self.write_message(msg, f"{self.PGLOG['LOGPATH']}/{self.PGLOG['ERRFILE']}", logact)
               if logact&self.EXITLG:
                  self.write_message(cmdstr, f"{self.PGLOG['LOGPATH']}/{self.PGLOG['LOGFILE']}", logact)
            else:
               self.write_message(msg, f"{self.PGLOG['LOGPATH']}/{self.PGLOG['LOGFILE']}", logact)
      if not self.PGLOG['BCKGRND'] and logact&(self.ERRLOG|self.WARNLG):
         self.write_message(msg, None, logact)
   
      if logact&self.EXITLG:
         self.pgexit(1)
      else:
         return (retmsg if retmsg else self.FAILURE)

   # write a log message
   def write_message(self, msg, file, logact):   
      doclose = False
      errlog = logact&self.ERRLOG
      if file:
         try:
             OUT = open(file, 'a')
             doclose = True
         except FileNotFoundError:
            OUT = sys.stderr if logact&(self.ERRLOG|self.EXITLG) else sys.stdout
            OUT.write(f"Log File not found: {file}")
      else:
         OUT = sys.stderr if logact&(self.ERRLOG|self.EXITLG) else sys.stdout
         if logact&self.BRKLIN: OUT.write("\n")
         if logact&self.SEPLIN: OUT.write(self.PGLOG['SEPLINE'])
      OUT.write(msg)
      if errlog and file and not logact&(self.EMLALL|self.SKPTRC): OUT.write(self.get_call_trace())
      if doclose: OUT.close()

   # check and disconnet database before exit
   def pgexit(self, stat = 0):
      if self.PGLOG['PGDBBUF']: self.PGLOG['PGDBBUF'].close()
      sys.exit(stat)

   # get a command string for error log dump
   def get_error_command(self, ctime, logact):
      if not self.CPID['PID']: self.CPID['PID'] =  "{}-{}-{}".format(self.PGLOG['HOSTNAME'], self.get_command(), self.PGLOG['CURUID'])
      cmdstr = "{} {}".format((("ABORTS" if logact&self.ERRLOG else "QUITS") if logact&self.EXITLG else "ERROR"), self.CPID['PID'])
      cmdstr = self.cmd_execute_time(cmdstr, (ctime - self.CPID['CTM']))
      if self.CPID['CPID']: cmdstr += " {} <=".format(self.CPID['CPID'])
      cmdstr += " {} at {}\n".format(self.break_long_string(self.CPID['CMD'], 40, "...", 1), self.current_datetime(ctime))
      return cmdstr

   # get call trace track
   @staticmethod
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

   # get caller file name
   @staticmethod
   def get_caller_file(cidx = 0):
      return traceback.extract_stack()[cidx][0]

   # log message, msg, for degugging processes according to the debug level
   def pgdbg(self, level, msg = None, do_trace = True):
      if not self.PGLOG['DBGLEVEL']: return     # no further action
      if not isinstance(level, int):
         ms = re.match(r'^(\d+)', level)
         level = int(ms.group(1)) if ms else 0
      levels = [0, 0]
      if isinstance(self.PGLOG['DBGLEVEL'], int):
         levels[1] = self.PGLOG['DBGLEVEL']
      else:
         ms = re.match(r'^(\d+)$', self.PGLOG['DBGLEVEL'])
         if ms:
            levels[1] = int(ms.group(1))
         else:
            ms = re.match(r'(\d*)-(\d*)', self.PGLOG['DBGLEVEL'])
            if ms:
               levels[0] = int(ms.group(1)) if ms.group(1) else 0
               levels[1] = int(ms.group(2)) if ms.group(2) else 9999
      if level > levels[1] or level < levels[0]: return   # debug level is out of range
      if 'DBGPATH' in self.PGLOG:
         dfile = self.PGLOG['DBGPATH'] + '/' + self.PGLOG['DBGFILE']
      else:
         dfile = self.PGLOG['DBGFILE']
      if not msg:
         self.pglog("Append debug Info (levels {}-{}) to {}".format(levels[0], levels[1], dfile), self.WARNLG)
         msg = "DEBUG for " + self.CPID['PID'] + " "
         if self.CPID['CPID']: msg += self.CPID['CPID'] + " <= "
         msg += self.break_long_string(self.CPID['CMD'], 40, "...", 1)
      # logging debug info
      DBG = open(dfile, 'a')
      DBG.write("{}:{}\n".format(level, msg))
      if do_trace: DBG.write(self.get_call_trace())
      DBG.close()

   # return trimed string (strip leading and trailling spaces); remove comments led by '#' if rmcmt > 0
   @staticmethod
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

   # set self.PGLOG['PUSGDIR'] from the program file with full path
   def set_help_path(self, progfile):
      self.PGLOG['PUSGDIR'] = op.dirname(op.abspath(progfile))

   # Function: show_usage(progname: Perl program name to get file "progname.usg")
   # show program usage in file "self.PGLOG['PUSGDIR']/progname.usg" on screen with unix
   # system function 'pg', exit program when done.
   def show_usage(self, progname, opts = None):
      if self.PGLOG['PUSGDIR'] is None: self.set_help_path(self.get_caller_file(1))
      usgname = self.join_paths(self.PGLOG['PUSGDIR'], progname + '.usg')
      if opts:   # show usage for individual option of dsarch
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
      self.pgexit(0)

   # compare error message to patterns saved in self.PGLOG['ERR2STD']
   # return 1 if matched; 0 otherwise
   def err2std(self, line):
      for err in self.PGLOG['ERR2STD']:
         if line.find(err) > -1: return 1
      return 0

   # compare message to patterns saved in self.PGLOG['STD2ERR']
   # return 1 if matched; 0 otherwise
   def std2err(self, line):
      for out in self.PGLOG['STD2ERR']:
         if line.find(out) > -1: return 1
      return 0

   # Function: pgsystem(pgcmd, logact, cmdopt, instr)
   #  pgcmd  - Linux system command, can be a string, "ls -l", or a list, ['ls', '-l']
   # logact  - logging action option, defaults to self.LOGWRN
   # cmdopt  - command control option, default to 5 (1+4)
   #           0 - no command control,
   #           1 - log pgcmd (include the sub command calls),
   #           2 - log standard output,
   #           4 - log error output
   #           7 - log all (pgcmd, and standard/error outputs),
   #           8 - log command with time,
   #          16 - return standard output message upon success
   #          32 - log error as standard output
   #          64 - force returning self.FAILURE if called process aborts
   #         128 - tries 2 times for failed command before quits
   #         256 - cache standard error message
   #         512 - log instr & seconds with pgcmd if cmdopt&1
   #        1024 - turn on shell
   # instr   - input string passing to the command via stdin if not None
   # seconds - number of seconds to wait for a timeout process if > 0
   def pgsystem(self, pgcmd, logact = None, cmdopt = 5, instr = None, seconds = 0):
      if logact is None: logact = self.LOGWRN
      ret = self.SUCCESS
      if not pgcmd: return ret  # empty command
      act = logact&~self.EXITLG
      if act&self.ERRLOG:
         act &= ~self.ERRLOG
         act |= self.WARNLG
      if act&self.MSGLOG: act |= self.FRCLOG   # make sure system calls always logged
      cmdact = act if cmdopt&1 else 0
      doshell = True if cmdopt&1024 else self.PGLOG['DOSHELL']
      if isinstance(pgcmd, str):
         cmdstr = pgcmd
         if not doshell and re.search(r'[*?<>|;]', pgcmd): doshell = True
         execmd = pgcmd if doshell else shlex.split(pgcmd)
      else:
         cmdstr = shlex.join(pgcmd)
         execmd = cmdstr if doshell else pgcmd   
      if cmdact:
         if cmdopt&8:
            self.cmdlog("starts '{}'".format(cmdstr), None, cmdact)
         else:
            self.pglog("> " + cmdstr, cmdact)
         if cmdopt&512 and (instr or seconds):
            msg = ''
            if seconds: msg = 'Timeout = {} Seconds'.format(seconds)
            if instr: msg += ' With STDIN:\n' + instr
            if msg: self.pglog(msg, cmdact)
      stdlog = act if cmdopt&2 else 0
      cmdflg = cmdact|stdlog
      abort = -1 if cmdopt&64 else 0
      loops = 2 if cmdopt&128 else 1
      self.PGLOG['SYSERR'] = error = retbuf = outbuf = errbuf = ''
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
            ret = self.FAILURE
         except Exception as e:
            errbuf = str(e)
            ret = self.FAILURE
         else:
            ret = self.FAILURE if FD.returncode else self.SUCCESS
            if isinstance(outbuf, bytes): outbuf = str(outbuf, errors='replace')
            if isinstance(errbuf, bytes): errbuf = str(errbuf, errors='replace')   
         if errbuf and cmdopt&32:
            outbuf += errbuf
            if cmdopt&256: self.PGLOG['SYSERR'] = errbuf
            errbuf = ''
         if outbuf:
            lines = outbuf.split('\n')
            for line in lines:
               line = self.strip_output_line(line.strip())
               if not line: continue
               if self.PGLOG['STD2ERR'] and self.std2err(line):
                  if cmdopt&260: error += line + "\n"
                  if abort == -1 and re.match('ABORTS ', line): abort = 1
               else:
                  if re.match(r'^>+ ', line):
                     line = '>' + line
                     if cmdflg: self.pglog(line, cmdflg)
                  elif stdlog:
                     self.pglog(line, stdlog)
                  if cmdopt&16: retbuf += line + "\n"
         if errbuf:
            lines = errbuf.split('\n')
            for line in lines:
               line = self.strip_output_line(line.strip())
               if not line: continue
               if self.PGLOG['ERR2STD'] and self.err2std(line):
                  if stdlog: self.pglog(line, stdlog)
                  if cmdopt&16: retbuf += line + "\n"
               else:
                  if cmdopt&260: error += line + "\n"
                  if abort == -1 and re.match('ABORTS ', line): abort = 1
         if ret == self.SUCCESS and abort == 1: ret = self.FAILURE
         end = time.time()
         last = end - last
         if error:
            if ret == self.FAILURE:
               error = "Error Execute: {}\n{}".format(cmdstr, error)
            else:
               error = "Error From: {}\n{}".format(cmdstr, error)
            if loop > 1: error = "Retry "
            if cmdopt&256: self.PGLOG['SYSERR'] += error
            if cmdopt&4:
               errlog = (act|self.ERRLOG)
               if ret == self.FAILURE and loop >= loops: errlog |= logact
               self.pglog(error, errlog)
         if last > self.PGLOG['CMDTIME'] and not re.search(r'(^|/|\s)(dsarch|dsupdt|dsrqst)\s', cmdstr):
            cmdstr = "> {} Ends By {}".format(self.break_long_string(cmdstr, 100, "...", 1), self.current_datetime())
            self.cmd_execute_time(cmdstr, last, cmdact)
         if ret == self.SUCCESS or loop >= loops: break
         time.sleep(6)
      if ret == self.FAILURE and retbuf and cmdopt&272 == 272:
         if self.PGLOG['SYSERR']: self.PGLOG['SYSERR'] += '\n'
         self.PGLOG['SYSERR'] += retbuf
         retbuf = ''
      return (retbuf if cmdopt&16 else ret)

   # strip carrage return '\r', but keep ending newline '\n'
   @staticmethod
   def strip_output_line(line):
      ms = re.search(r'\r([^\r]+)\r*$', line)
      if ms: return ms.group(1)   
      ms = re.search(r'\s\.+\s+(\d+)%\s+', line)
      if ms and int(ms.group(1)) != 100: return None
      return line

   # show command running time string formated by seconds_to_string_time()
   def cmd_execute_time(self, cmdstr, last, logact = None):
      msg = cmdstr
      if last >= self.PGLOG['CMDTIME']:   # show running for at least one minute
         msg += " ({})".format(self.seconds_to_string_time(last))
      if logact:
         return self.pglog(msg, logact)
      else:
         return msg

   # convert given seconds to string time with units of S-Second,M-Minute,H-Hour,D-Day
   @staticmethod
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

   #  wrap function to call pgsystem() with a timeout control
   #  return self.FAILURE if error eval or time out
   def tosystem(self, cmd, timeout = 0, logact = 0, cmdopt = 5, instr = None):
      if logact is None: logact = self.LOGWRN
      if not timeout: timeout = self.PGLOG['TIMEOUT']   # set default timeout if missed
      return self.pgsystem(cmd, logact, cmdopt, instr, timeout)

   # insert breaks, default to '\n', for every length, default to 1024,
   # for long string; return specified number lines if mline given
   @staticmethod
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

   # join two paths by remove overlapping directories
   # diff = 0: join given pathes
   #        1: remove path1 from path2
   @staticmethod
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

   # validate if a command for a given BATCH host is accessable and executable
   # Return self.SUCCESS if valid; self.FAILURE if not
   def valid_batch_host(self, host, logact = 0):
      HOST = host.upper()
      return self.SUCCESS if HOST in self.BCHCMDS and self.valid_command(self.BCHCMDS[HOST], logact) else self.FAILURE

   # validate if a given command is accessable and executable
   # Return the full command path if valid; '' if not
   def valid_command(self, cmd, logact = 0):
      ms = re.match(r'^(\S+)( .*)$', cmd)
      if ms:
         option = ms.group(2)
         cmd = ms.group(1)
      else:
         option = ''
      if cmd not in self.COMMANDS:
         buf = shutil.which(cmd)
         if buf is None:
            if logact: self.pglog(cmd + ": executable command not found", logact)
            buf = ''
         elif option:
            buf += option
         self.COMMANDS[cmd] = buf
      return self.COMMANDS[cmd]

   # add carbon copies to self.PGLOG['CCDADDR']
   def add_carbon_copy(self, cc = None, isstr = None, exclude = 0, specialist = None):

      if not cc:
         if cc is None and isstr is None: self.PGLOG['CCDADDR'] = ''
      else:
         emails = re.split(r'[,\s]+', cc) if isstr else cc
         for email in emails:
            if not email or email.find('/') >= 0 or email == 'N': continue
            if email == "S":
               if not specialist: continue
               email = specialist
            if email.find('@') == -1: email += "@ucar.edu"
            if exclude and exclude.find(email) > -1: continue
            if self.PGLOG['CCDADDR']:
               if self.PGLOG['CCDADDR'].find(email) > -1: continue   # email Cc'd already
               self.PGLOG['CCDADDR'] += ", "
            self.PGLOG['CCDADDR'] += email

   # get the current host name; or batch sever name if getbatch is 1
   def get_host(self, getbatch = 0):

      if getbatch and self.PGLOG['CURBID'] != 0:
         host = self.PGLOG['PGBATCH']
      elif self.PGLOG['HOSTNAME']:
         return self.PGLOG['HOSTNAME']
      else:
         host = socket.gethostname()

      return self.get_short_host(host)

   #
   # strip domain names and retrun the server name itself
   #
   def get_short_host(self, host):

      if not host: return ''
      ms = re.match(r'^([^\.]+)\.', host)
      if ms: host = ms.group(1)
      if self.PGLOG['HOSTNAME'] and (host == 'localhost' or host == self.PGLOG['HOSTNAME']): return self.PGLOG['HOSTNAME']
      HOST = host.upper()
      if HOST in self.BCHCMDS: return HOST

      return host

   # get a live PBS host name
   def get_pbs_host(self):
      if not self.PBSSTATS and self.PGLOG['PBSHOSTS']:
         self.PBSHOSTS = self.PGLOG['PBSHOSTS'].split(':')
         for host in self.PBSHOSTS:
            self.PBSSTATS[host] = 1
      for host in self.PBSHOSTS:
         if host in self.PBSSTATS and self.PBSSTATS[host]: return host
      return None

   # set host status, 0 dead & 1 live, for one or all avalaible pbs hosts
   def set_pbs_host(self, host = None, stat = 0):
      if host:
         self.PBSSTATS[host] = stat
      else:
         if not self.PBSHOSTS and self.PGLOG['PBSHOSTS']:
            self.PBSHOSTS = self.PGLOG['PBSHOSTS'].split(':')
         for host in self.PBSHOSTS:
            self.PBSSTATS[host] = stat

   #  reset the batch host name in case was not set properly
   def reset_batch_host(self, bhost, logact = None):
      if logact is None: logact = self.LOGWRN
      bchhost = bhost.upper()
      if bchhost != self.PGLOG['PGBATCH']:
         if self.PGLOG['CURBID'] > 0:
            self.pglog("{}-{}: Batch ID is set, cannot change Batch host to {}".format(self.PGLOG['PGBATCH'], self.PGLOG['CURBID'], bchhost) , logact)
         else:
            ms = re.search(r'(^|:){}(:|$)'.format(bchhost), self.PGLOG['BCHHOSTS'])
            if ms:
               self.PGLOG['PGBATCH'] = bchhost
               if self.PGLOG['CURBID'] == 0: self.PGLOG['CURBID'] = -1
            elif self.PGLOG['PGBATCH']:
               self.PGLOG['PGBATCH'] = ''
               self.PGLOG['CURBID'] = 0

   # return the base command name of the current process
   @staticmethod
   def get_command(cmdstr = None):
      if not cmdstr: cmdstr = sys.argv[0]
      cmdstr = op.basename(cmdstr)
      ms = re.match(r'^(.+)\.(py|pl)$', cmdstr)
      if ms:
         return ms.group(1)
      else:
         return cmdstr

   # wrap a given command cmd for either sudo or setuid wrapper pgstart_['username']
   # to run as user asuser
   def get_local_command(self, cmd, asuser = None):
      cuser = self.PGLOG['SETUID'] if self.PGLOG['SETUID'] else self.PGLOG['CURUID']
      if not asuser or cuser == asuser: return cmd
      if cuser == self.PGLOG['GDEXUSER']:
         wrapper = "pgstart_" + asuser
         if self.valid_command(wrapper): return "{} {}".format(wrapper, cmd)
      elif self.PGLOG['SUDOGDEX'] and asuser == self.PGLOG['GDEXUSER']:
         return "sudo -u {} {}".format(self.PGLOG['GDEXUSER'], cmd)    # sudo as user gdexdata
      return cmd

   # wrap a given command cmd for either sudo or setuid wrapper pgstart_['username']
   # to run as user asuser on a given remote host
   def get_remote_command(self, cmd, host, asuser = None):
      return self.get_local_command(cmd, asuser)

   # wrap a given sync command for given host name with/without sudo
   def get_sync_command(self, host, asuser = None):
      host = self.get_short_host(host)
      if (not (self.PGLOG['SETUID'] and self.PGLOG['SETUID'] == self.PGLOG['GDEXUSER']) and
         (not asuser or asuser == self.PGLOG['GDEXUSER'])):
         return "sync" + host
      return host + "-sync"

   # set self.PGLOG['SETUID'] as needed
   def set_suid(self, cuid = 0):
      if not cuid: cuid = self.PGLOG['EUID']
      if cuid != self.PGLOG['EUID'] or cuid != self.PGLOG['RUID']:
         os.setreuid(cuid, cuid)
         self.PGLOG['SETUID'] = pwd.getpwuid(cuid).pw_name
         if not (self.PGLOG['SETUID'] == self.PGLOG['GDEXUSER'] or cuid == self.PGLOG['RUID']):
            self.set_specialist_environments(self.PGLOG['SETUID'])
            self.PGLOG['CURUID'] == self.PGLOG['SETUID']      # set CURUID to a specific specialist

   # set comman pglog
   def set_common_pglog(self):
      self.PGLOG['CURDIR'] = os.getcwd()   
      # set current user id
      self.PGLOG['RUID'] = os.getuid()
      self.PGLOG['EUID'] = os.geteuid()
      self.PGLOG['CURUID'] = pwd.getpwuid(self.PGLOG['RUID']).pw_name
      try:
         self.PGLOG['RDAUID'] = self.PGLOG['GDEXUID'] = pwd.getpwnam(self.PGLOG['GDEXUSER']).pw_uid
         self.PGLOG['RDAGID'] = self.PGLOG['GDEXGID'] = grp.getgrnam(self.PGLOG['GDEXGRP']).gr_gid
      except:
         self.PGLOG['RDAUID'] = self.PGLOG['GDEXUID'] = 0
         self.PGLOG['RDAGID'] = self.PGLOG['GDEXGID'] = 0
      if self.PGLOG['CURUID'] == self.PGLOG['GDEXUSER']: self.PGLOG['SETUID'] = self.PGLOG['GDEXUSER']   
      self.PGLOG['HOSTNAME'] = self.get_host()
      for htype in self.HOSTTYPES:
         ms = re.match(r'^{}(-|\d|$)'.format(htype), self.PGLOG['HOSTNAME'])
         if ms:
            self.PGLOG['HOSTTYPE'] = self.HOSTTYPES[htype]
            break
      self.PGLOG['DEFDSID'] = 'd000000' if self.PGLOG['NEWDSID'] else 'ds000.0'
      self.SETPGLOG("USRHOME", "/glade/u/home")
      self.SETPGLOG("DSSHOME", "/glade/u/home/gdexdata")
      self.SETPGLOG("GDEXHOME", "/data/local")
      self.SETPGLOG("ADDPATH", "")
      self.SETPGLOG("ADDLIB",  "")
      self.SETPGLOG("OTHPATH", "")
      self.SETPGLOG("PSQLHOME", "")
      self.SETPGLOG("DSGHOSTS", "")
      self.SETPGLOG("DSIDCHRS", "d")
      if not os.getenv('HOME'): os.environ['HOME'] = "{}/{}".format(self.PGLOG['USRHOME'], self.PGLOG['CURUID'])
      self.SETPGLOG("HOMEBIN", os.environ.get('HOME') + "/bin")
      if 'PBS_JOBID' in os.environ:
         sbid = os.getenv('PBS_JOBID')
         ms = re.match(r'^(\d+)', sbid)
         self.PGLOG['CURBID'] = int(ms.group(1)) if ms else -1
         self.PGLOG['PGBATCH'] = self.PGLOG['PBSNAME']
      else:
         self.PGLOG['CURBID'] = 0
         self.PGLOG['PGBATCH'] = ''
      pgpath = self.PGLOG['HOMEBIN']
      self.PGLOG['LOCHOME'] = "/ncar/rda/setuid"
      if not op.isdir(self.PGLOG['LOCHOME']): self.PGLOG['LOCHOME'] = "/usr/local/decs"
      pgpath += ":{}/bin".format(self.PGLOG['LOCHOME'])
      locpath = "{}/bin/{}".format(self.PGLOG['DSSHOME'], self.PGLOG['HOSTTYPE'])
      if op.isdir(locpath): pgpath += ":" + locpath
      pgpath = self.add_local_path("{}/bin".format(self.PGLOG['DSSHOME']), pgpath, 1)
      if self.PGLOG['PSQLHOME']:
         locpath = self.PGLOG['PSQLHOME'] + "/bin"
         if op.isdir(locpath): pgpath += ":" + locpath
      pgpath = self.add_local_path(os.getenv('PATH'), pgpath, 1)
      if self.PGLOG['HOSTTYPE'] == 'dav': pgpath = self.add_local_path('/glade/u/apps/opt/qstat-cache/bin:/opt/pbs/bin', pgpath, 1)
      if 'OTHPATH' in self.PGLOG and self.PGLOG['OTHPATH']:
         pgpath = self.add_local_path(self.PGLOG['OTHPATH'], pgpath, 1)
      if self.PGLOG['ADDPATH']: pgpath = self.add_local_path(self.PGLOG['ADDPATH'], pgpath, 1)
      pgpath = self.add_local_path("/bin:/usr/bin:/usr/local/bin:/usr/sbin", pgpath, 1)
      os.environ['PATH'] = pgpath
      os.environ['SHELL'] = '/bin/sh'
      # set self.PGLOG values with environments and defaults
      self.SETPGLOG("DSSDBHM", self.PGLOG['DSSHOME']+"/dssdb")       # dssdb home dir
      self.SETPGLOG("LOGPATH", self.PGLOG['DSSDBHM']+"/log")         # path to log file
      self.SETPGLOG("LOGFILE", "pgdss.log")                     # log file name
      self.SETPGLOG("EMLFILE", "pgemail.log")                   # email log file name
      self.SETPGLOG("ERRFILE", '')                              # error file name
      sm = "/usr/sbin/sendmail"
      if self.valid_command(sm): self.SETPGLOG("EMLSEND", f"{sm} -t")   # send email command
      self.SETPGLOG("DBGLEVEL", '')                             # debug level
      self.SETPGLOG("BAOTOKEN", 's.lh2t2kDjrqs3V8y2BU2zOocT')   # OpenBao token
      self.SETPGLOG("DBGPATH", self.PGLOG['DSSDBHM']+"/log")    # path to debug log file
      self.SETPGLOG("OBJCTBKT", "gdex-data")                    # default Bucket on Object Store
      self.SETPGLOG("BACKUPEP", "gdex-quasar")                  # default Globus Endpoint on Quasar
      self.SETPGLOG("DRDATAEP", "gdex-quasar-drdata")           # DRDATA Globus Endpoint on Quasar
      self.SETPGLOG("DBGFILE", "pgdss.dbg")                     # debug file name
      self.SETPGLOG("CNFPATH", self.PGLOG['DSSHOME']+"/config")      # path to configuration files
      self.SETPGLOG("DSSURL",  "https://gdex.ucar.edu")          # current dss web URL
      self.SETPGLOG("RQSTURL", "/datasets/request")              # request URL path
      self.SETPGLOG("WEBSERVERS", "")                 # webserver names for Web server
      self.PGLOG['WEBHOSTS'] = self.PGLOG['WEBSERVERS'].split(':') if self.PGLOG['WEBSERVERS'] else []
      self.SETPGLOG("DBMODULE", '')
      self.SETPGLOG("LOCDATA", "/data")
      # set dss web homedir
      self.SETPGLOG("DSSWEB",  self.PGLOG['LOCDATA']+"/web")
      self.SETPGLOG("DSWHOME", self.PGLOG['DSSWEB']+"/datasets")     # datast web root path
      self.PGLOG['HOMEROOTS'] = "{}|{}".format(self.PGLOG['DSSHOME'], self.PGLOG['DSWHOME'])
      self.SETPGLOG("DSSDATA", "/glade/campaign/collections/gdex")   # dss data root path
      self.SETPGLOG("DSDHOME", self.PGLOG['DSSDATA']+"/data")        # dataset data root path
      self.SETPGLOG("DECSHOME", self.PGLOG['DSSDATA']+"/decsdata")   # dataset decsdata root path
      self.SETPGLOG("DSHHOME", self.PGLOG['DECSHOME']+"/helpfiles")  # dataset help root path
      self.SETPGLOG("GDEXWORK", "/lustre/desc1/gdex/work")           # gdex work path
      self.SETPGLOG("UPDTWKP", self.PGLOG['GDEXWORK'])               # dsupdt work root path
      self.SETPGLOG("TRANSFER", "/lustre/desc1/gdex/transfer")       # gdex transfer path
      self.SETPGLOG("RQSTHOME", self.PGLOG['TRANSFER']+"/dsrqst")    # dsrqst home
      self.SETPGLOG("DSAHOME",  "")                     # dataset data alternate root path
      self.SETPGLOG("RQSTALTH", "")                     # alternate dsrqst path
      self.SETPGLOG("GPFSHOST", "")                     # empty if writable to glade
      self.SETPGLOG("PSQLHOST", "rda-db.ucar.edu")      # host name for postgresql server
      self.SETPGLOG("PBSHOSTS", "cron:casper:crlogin")  # host names for PBS server
      self.SETPGLOG("CHKHOSTS", "")                     # host names for dscheck daemon
      self.SETPGLOG("PVIEWHOST", "pgdb02.k8s.ucar.edu")             # host name for view only postgresql server
      self.SETPGLOG("PMISCHOST", "pgdb03.k8s.ucar.edu")             # host name for misc postgresql server
      self.SETPGLOG("FTPUPLD",  self.PGLOG['TRANSFER']+"/rossby")   # ftp upload path
      self.PGLOG['GPFSROOTS'] = "{}|{}|{}".format(self.PGLOG['DSDHOME'], self.PGLOG['UPDTWKP'], self.PGLOG['RQSTHOME'])
      if 'ECCODES_DEFINITION_PATH' not in os.environ:
         os.environ['ECCODES_DEFINITION_PATH'] = "/usr/local/share/eccodes/definitions"
      os.environ['history'] = '0'
      # set tmp dir
      self.SETPGLOG("TMPPATH", self.PGLOG['GDEXWORK'] + "/ptmp")
      if not self.PGLOG['TMPPATH']: self.PGLOG['TMPPATH'] = "/data/ptmp"
      self.SETPGLOG("TMPDIR", '')
      if not self.PGLOG['TMPDIR']:
         self.PGLOG['TMPDIR'] = "/lustre/desc1/scratch/" + self.PGLOG['CURUID']
         os.environ['TMPDIR'] = self.PGLOG['TMPDIR']
      # empty diretory for HOST-sync
      self.PGLOG['TMPSYNC'] = self.PGLOG['DSSDBHM'] + "/tmp/.syncdir"   
      os.umask(2)

   # check and return TMPSYNC path, and add it if not exists
   def get_tmpsync_path(self):
      return self.PGLOG['TMPSYNC']

   # append or prepend locpath to pgpath
   def add_local_path(self, locpath, pgpath, append = 0):
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

   # set self.PGLOG value; return a string or an array reference if sep is not emty
   def SETPGLOG(self, name, value = ''):
      oval = self.PGLOG[name] if name in self.PGLOG else ''
      nval = self.get_environment(name, ('' if re.match('PG', value) else value))
      self.PGLOG[name] = nval if nval else oval

   # set specialist home and return the default shell
   def set_specialist_home(self, specialist):
      if specialist == self.PGLOG['CURUID']: return   # no need reset
      if 'MAIL' in os.environ and re.search(self.PGLOG['CURUID'], os.environ['MAIL']):
         os.environ['MAIL'] = re.sub(self.PGLOG['CURUID'], specialist, os.environ['MAIL'])   
      home = "{}/{}".format(self.PGLOG['USRHOME'], specialist)
      shell = "tcsh"
      buf = self.pgsystem("grep ^{}: /etc/passwd".format(specialist), self.LOGWRN, 20)
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

   #  set environments for a specified specialist
   def set_specialist_environments(self, specialist):   
      shell = self.set_specialist_home(specialist)
      resource = os.environ['HOME'] + "/.tcshrc"
      checkif = 0   # 0 outside of if; 1 start if, 2 check envs, -1 checked already
      missthen = 0
      try:
         rf = open(resource, 'r')
      except:
         return   # skip if cannot open   
      nline = rf.readline()
      while nline:
         line = self.pgtrim(nline)
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
               if(re.search(r'host.*!', line, re.I) and not re.search(self.PGLOG['HOSTNAME'], line) or
                  re.search(r'host.*=', line, re.I) and re.search(self.PGLOG['HOSTNAME'], line)):
                  checkif = 2
               if re.search(r'\sthen$', line):
                  continue
               else:
                  missthen = 1
                  if checkif == 1: continue
            else:
               continue
         ms = re.match(r'^setenv\s+(.*)', line)
         if ms: self.one_specialist_environment(ms.group(1))
      rf.close()
      self.SETPGLOG("HOMEBIN", self.PGLOG['PGBINDIR'])
      os.environ['PATH'] = self.add_local_path(self.PGLOG['HOMEBIN'], os.environ['PATH'], 0)

   # set one environment for specialist
   def one_specialist_environment(self, line):
      ms = re.match(r'^(\w+)[=\s]+(.+)$', line)
      if not ms: return
      (var, val) = ms.groups()
      if re.match(r'^(PATH|SHELL|IFS|CDPATH|)$', var): return
      if val.find('$') > -1: val = self.replace_environments(val)
      ms = re.match(r'^(\"|\')(.*)(\"|\')$', val)
      if ms: val = ms.group(2)   # remove quotes
      os.environ[var] = val
   
   # get and repalce environment variables in ginve string; defaults to the values in self.PGLOG
   def replace_environments(self, envstr, default = '', logact = 0):
      ishash = isinstance(default, dict)
      ms = re.search(r'(^|.)\$({*)(\w+)(}*)', envstr)
      if ms:
         lead = ms.group(1)
         name = ms.group(3)
         rep = ms.group(2) + name + ms.group(4)
         env = self.get_environment(name, (self.PGLOG[name] if name in self.PGLOG else (default[name] if ishash else default)), logact)
         pre = (lead if (env or lead != ":") else '')
         envstr = re.sub(r'{}\${}'.format(lead, rep), (pre+env), envstr)
      return envstr

   # validate if the current host is a valid host to process
   def check_process_host(self, hosts, chost = None, mflag = None, pinfo = None, logact = None):
      ret = 1
      error = ''
      if not mflag: mflag = 'G'
      if not chost: chost = self.get_host(1)   
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
         if logact is None: logact = self.LOGERR
         self.pglog("{}: CANNOT be processed on {} for hosthame {}".format(pinfo, chost, error), logact)
      return ret

   # convert special foreign characters into ascii characters
   @staticmethod
   def convert_chars(name, default = 'X'):      
      if not name: return default
      if re.match(r'^[a-zA-Z0-9]+$', name): return name  # conversion not needed
      decoded_name = unidecode(name).strip()
      # remove any non-alphanumeric and non-underscore characters
      cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '', decoded_name)
      if cleaned_name:
         return cleaned_name
      else:
         return default

   #  Retrieve host and process id
   def current_process_info(self, realpid = 0):
      if realpid or self.PGLOG['CURBID'] < 1:
         return [self.PGLOG['HOSTNAME'], os.getpid()]
      else:
         return [self.PGLOG['PGBATCH'], self.PGLOG['CURBID']]

   # convert given @ARGV to string. quote the entries with spaces
   def argv_to_string(self, argv = None, quote = 1, action = None):
      argstr = ''
      if argv is None: argv = sys.argv[1:]
      for arg in argv:
         if argstr:  argstr += ' '
         ms = re.search(r'([<>\|\s])', arg)
         if ms:
            if action:
               self.pglog("{}: Cannot {} for special character '{}' in argument value".format(arg, action, ms.group(1)), self.LGEREX)
            if quote:
               if re.search(r"\'", arg):
                  arg = "\"{}\"".format(arg)
               else:
                  arg = "'{}'".format(arg)
         argstr += arg
      return argstr

   # convert an integer to non-10 based string
   @staticmethod
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

   # convert a non-10 based string to an integer
   @staticmethod
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

   # convert integer to ordinal string
   @staticmethod
   def int2order(num):
      ordstr = ['th', 'st', 'nd', 'rd']
      snum = str(num)
      num %= 100
      if num > 19: num %= 10
      if num > 3: num = 0   
      return snum + ordstr[num]
