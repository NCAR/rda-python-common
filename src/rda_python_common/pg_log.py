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
   """Logging and process management class for RDA Python tools.

   PgLOG provides a unified interface for logging messages to files and
   STDERR/STDOUT, sending email notifications, running system commands,
   and managing process metadata.

   Logging behavior is controlled by bitfield ``logact`` flags (e.g. MSGLOG,
   WARNLG, ERRLOG, EXITLG).  Combine flags with ``|`` to compose actions::

       pglog.pglog("something went wrong", PgLOG.LOGERR | PgLOG.EXITLG)

   Key flag groups:

   * Output destination: ``MSGLOG`` (log file), ``WARNLG`` / ``ERRLOG`` (STDERR),
     ``EMLLOG`` / ``SNDEML`` (email buffer / send immediately)
   * Control flow:       ``EXITLG`` (sys.exit after logging), ``RETMSG`` (return msg)
   * Formatting:         ``SEPLIN`` (separator line), ``BRKLIN`` (blank line)
   * Return constants:   ``SUCCESS`` (1), ``FAILURE`` (0), ``FINISH`` (2)

   Instance state is held in ``self.PGLOG`` (dict), ``self.CPID`` (process info
   dict), ``self.COMMANDS`` (command-path cache), and ``self.HOSTTYPES``.
   """

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
      """Initialize PgLOG with default configuration values.

      Populates ``self.PGLOG`` with site defaults (paths, email settings,
      user IDs, batch system info, etc.) then calls :meth:`set_common_pglog`
      to override those defaults from environment variables and detect the
      runtime environment (hostname, PBS job, PATH construction, etc.).
      """
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
         'TACCNAME': "tacc",
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
      self.OUTPUT = None

   def open_output(self, outfile=None):
      """Open the result output destination.

      Args:
         outfile (str, optional): Path to a file to write results to.  If
            ``None`` or omitted, output is directed to ``sys.stdout``.
      """
      if outfile:  # result output file
         try:
            self.OUTPUT = open(outfile, 'w')
         except Exception as e:
            self.pglog("{}: Error open file to write - {}".format(outfile, str(e)), self.PGOPT['extlog'])
      else:                             # result to STDOUT
         if self.OUTPUT and self.OUTPUT != sys.stdout:
            self.OUTPUT.close()
            self.OUTPUT = sys.stdout

   def current_datetime(self, ctime=0):
      """Return a datetime string in YYYYMMDDHHMMSS format.

      Args:
          ctime: Unix timestamp (seconds).  Uses current time when 0.

      Returns:
          14-character string ``YYYYMMDDHHMMSS`` in local or GMT time
          depending on ``self.PGLOG['GMTZ']``.
      """
      get_time = time.gmtime if self.PGLOG['GMTZ'] else time.localtime
      dt = get_time(ctime) if ctime else get_time()
      return "{:02}{:02}{:02}{:02}{:02}{:02}".format(dt[0], dt[1], dt[2], dt[3], dt[4], dt[5])

   def get_environment(self, name, default=None, logact=0):
      """Return an environment variable value, optionally logging if missing.

      Args:
          name:    Environment variable name.
          default: Value returned when the variable is unset (default None).
          logact:  Logging action flags; if non-zero and variable is missing,
                   calls :meth:`pglog` with that action.

      Returns:
          The variable's string value, or *default* if unset.
      """
      env = os.getenv(name, default)
      if env is None and logact:
         self.pglog(name + ": Environment variable is not defined", logact)
      return env

   def set_email(self, msg, logact=0):
      """Append or prepend *msg* to the internal email buffers.

      Buffers are flushed and composed by :meth:`send_email` /
      :meth:`send_python_email`.  Pass ``msg=None`` to clear ``EMLMSG``.

      Args:
          msg:    Message text to buffer.  ``None`` clears ``EMLMSG``.
          logact: Combination of ``EMLTOP`` (prepend / finalise as top-level
                  email), ``ERRLOG`` (record as numbered error), ``EMLSUM``
                  (record in summary section), ``EMLLOG`` (record in detail
                  section), ``BRKLIN`` / ``SEPLIN`` (formatting).
      """
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

   def get_email(self):
      """Return the currently buffered email message string."""
      return self.PGLOG['EMLMSG']

   def send_customized_email(self, logmsg, emlmsg, logact=None):
      """Send an email whose headers are embedded inside *emlmsg*.

      The message body must contain ``From:``, ``To:``, and ``Subject:``
      header lines.  ``Cc:`` is optional.  Headers are stripped from the
      body before sending.

      Args:
          logmsg: Prefix string for error/status log messages.
          emlmsg: Full email text with embedded ``From/To/Cc/Subject`` lines.
          logact: Logging action flags (default ``LOGWRN``).

      Returns:
          ``SUCCESS`` on success, ``FAILURE`` on error.
      """
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

   def send_email(self, subject=None, receiver=None, msg=None, sender=None, logact=None):
      """Send an email via :meth:`send_python_email`.

      If *msg* is empty, the buffered ``EMLMSG`` is used and cleared.

      Args:
          subject:  Email subject line.  Defaults to a hostname/command string.
          receiver: Recipient address.  Defaults to ``EMLADDR`` or ``CURUID``.
          msg:      Message body.  Uses buffered ``EMLMSG`` when omitted.
          sender:   Sender address.  Defaults to ``CURUID``.
          logact:   Logging action flags (default ``LOGWRN``).

      Returns:
          ``SUCCESS`` on success, ``FAILURE`` on error or no message to send.
      """
      if logact is None: logact = self.LOGWRN
      return self.send_python_email(subject, receiver, msg, sender, None, logact)

   def send_python_email(self, subject=None, receiver=None, msg=None, sender=None, cc=None, logact=None):
      """Send an email using Python's ``smtplib``.

      If *msg* is empty, uses and clears the buffered ``EMLMSG``.
      Pass ``cc=''`` explicitly to suppress the Cc header entirely.

      Args:
          subject:  Email subject.  Auto-generated from hostname/command if omitted.
          receiver: Recipient address.  Defaults to ``EMLADDR`` or ``CURUID``.
          msg:      Message body.  Uses buffered ``EMLMSG`` when omitted.
          sender:   Sender address.  Defaults to ``CURUID``.
          cc:       Carbon-copy address(es).  Uses ``CCDADDR`` when ``None``;
                    pass ``''`` to skip Cc entirely.
          logact:   Logging action flags (default ``LOGWRN``).

      Returns:
          ``SUCCESS`` on success, empty string when there is nothing to send,
          or ``FAILURE`` on SMTP error.
      """
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
      eml = None
      try:
         eml = smtplib.SMTP(self.PGLOG['EMLSRVR'], self.PGLOG['EMLPORT'])
         eml.send_message(emlmsg)
      except smtplib.SMTPException as err:
         errmsg = f"Error sending email:\n{err}\n{logmsg}"
         return self.pglog(errmsg, (logact|self.ERRLOG)&~self.EXITLG)
      finally:
         if eml is not None:
            eml.quit()
      self.log_email(str(emlmsg))
      self.pglog(logmsg, logact&~self.EXITLG)
      return self.SUCCESS

   def log_email(self, emlmsg):
      """Append a sent-email record to the email log file.

      Args:
          emlmsg: Full email message string (as returned by ``str(EmailMessage)``).
      """
      if not self.CPID['PID']:
         self.CPID['PID'] = "{}-{}-{}".format(self.PGLOG['HOSTNAME'], self.get_command(), self.PGLOG['CURUID'])
      cmdstr = "{} {} at {}\n".format(self.CPID['PID'], self.break_long_string(self.CPID['CMD'], 40, "...", 1), self.current_datetime())
      fn = "{}/{}".format(self.PGLOG['LOGPATH'], self.PGLOG['EMLFILE'])
      try:
         with open(fn, 'a') as f:
            f.write(cmdstr + emlmsg)
      except FileNotFoundError as e:
         print(e)
   
   def cmdlog(self, cmdline=None, ctime=0, logact=None):
      """Log a command start or end event and update process timing info.

      When *cmdline* is ``None`` or matches ``end|quit|exit|abort``, logs the
      elapsed execution time and clears process state.  Otherwise records the
      command in ``self.CPID`` and logs its start.

      Args:
          cmdline: Command string to log.  ``None`` or an end-keyword logs
                   the elapsed time since the matching start.
          ctime:   Unix timestamp (seconds) for the event.  Defaults to now.
          logact:  Logging action flags (default ``MSGLOG|FRCLOG``).
      """
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

   def pglog(self, msg, logact=None):
      """Log *msg* and take action based on *logact* bitfield flags.

      This is the central logging method.  It writes to the log file and/or
      STDERR/STDOUT, buffers for email, sends email immediately, or exits the
      process — all controlled by the *logact* bitmask.

      Args:
          msg:    Message text to log.  Leading whitespace is stripped.
          logact: Combination of action flags (default ``MSGLOG``):

                  * ``MSGLOG``  — write to log file
                  * ``WARNLG``  — write to STDERR as a warning
                  * ``ERRLOG``  — write to error log file and STDERR
                  * ``EXITLG``  — call ``sys.exit(1)`` after logging
                  * ``EMLLOG``  — append *msg* to email buffer
                  * ``SNDEML``  — send buffered email now
                  * ``RETMSG``  — return *msg* instead of ``FAILURE``
                  * ``FRCLOG``  — force write even if MSGLOG not set
                  * ``SEPLIN``  — prepend a separator line
                  * ``BRKLIN``  — prepend a blank line

      Returns:
          The *msg* string if ``RETMSG`` is set; otherwise ``FAILURE`` (0).
          Does not return when ``EXITLG`` is set.
      """
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
         if msg and not msg.endswith(('\n', '\r')): msg += "\n"
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

   def write_message(self, msg, file, logact):
      """Write *msg* to *file* (or STDOUT/STDERR when *file* is ``None``).

      When *file* is given but cannot be opened, falls back to STDOUT/STDERR
      with an error notice.  Appends a call-trace for error-log writes.

      Args:
          msg:    Text to write.
          file:   Absolute path to log file, or ``None`` for console output.
          logact: Logging action flags used to select output stream and
                  formatting (``ERRLOG``, ``EXITLG``, ``BRKLIN``, ``SEPLIN``).
      """
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

   def pgexit(self, stat=0):
      """Close the database connection (if open) and exit the process.

      Args:
          stat: Exit status code passed to ``sys.exit`` (default 0).
      """
      if self.PGLOG['PGDBBUF']: self.PGLOG['PGDBBUF'].close()
      if self.OUTPUT and self.OUTPUT != sys.stdout: self.OUTPUT.close()
      sys.exit(stat)

   def get_error_command(self, ctime, logact):
      """Build a one-line error/abort header string for log entries.

      Args:
          ctime:  Unix timestamp (seconds) at the time of the error.
          logact: Logging action flags used to determine the prefix word
                  (``ABORTS``, ``QUITS``, or ``ERROR``).

      Returns:
          Formatted string ending with ``\\n``.
      """
      if not self.CPID['PID']: self.CPID['PID'] =  "{}-{}-{}".format(self.PGLOG['HOSTNAME'], self.get_command(), self.PGLOG['CURUID'])
      cmdstr = "{} {}".format((("ABORTS" if logact&self.ERRLOG else "QUITS") if logact&self.EXITLG else "ERROR"), self.CPID['PID'])
      cmdstr = self.cmd_execute_time(cmdstr, (ctime - self.CPID['CTM']))
      if self.CPID['CPID']: cmdstr += " {} <=".format(self.CPID['CPID'])
      cmdstr += " {} at {}\n".format(self.break_long_string(self.CPID['CMD'], 40, "...", 1), self.current_datetime(ctime))
      return cmdstr

   @staticmethod
   def get_call_trace(cut=1):
      """Return a formatted call-stack trace string.

      Args:
          cut: Number of innermost frames to omit (default 1 to exclude
               this method itself).

      Returns:
          A ``Trace: file(line){func}=>...`` string ending with ``\\n``,
          or an empty string when the stack is empty.
      """
      t = traceback.extract_stack()
      n = len(t) - cut
      trace = ''
      sep = 'Trace: '
      for i in range(n):
         tc = t[i]
         trace += "{}{}({}){}".format(sep, tc[0], tc[1], ("" if tc[2] == '<module>' else "{%s()}" % tc[2]))
         if i == 0: sep = '=>'
      return trace + "\n" if trace else ""

   @staticmethod
   def get_caller_file(cidx=0):
      """Return the source-file path of a caller frame.

      Args:
          cidx: Index into ``traceback.extract_stack()`` (default 0 = oldest frame).

      Returns:
          Absolute path string of the caller's source file.
      """
      return traceback.extract_stack()[cidx][0]

   def pgdbg(self, level, msg=None, do_trace=True):
      """Append a debug message to the debug log file if *level* is in range.

      No action is taken when ``PGLOG['DBGLEVEL']`` is falsy.  The level
      range is specified as an integer (``0``–*N*) or a ``"min-max"`` string.

      Args:
          level:    Integer debug level for this message, or a string whose
                    leading digits are parsed as the level.
          msg:      Message text.  Omit to log a header-only entry that also
                    warns on STDERR.
          do_trace: When ``True`` (default), appends a call-stack trace.
      """
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
      with open(dfile, 'a') as DBG:
         DBG.write("{}:{}\n".format(level, msg))
         if do_trace: DBG.write(self.get_call_trace())

   @staticmethod
   def pgtrim(line, rmcmt=1):
      """Strip leading/trailing whitespace and optionally remove comments.

      Args:
          line:  Input string to trim.
          rmcmt: Comment removal mode:

                 * ``0`` — no comment removal
                 * ``1`` — remove lines starting with ``#`` and inline comments
                   preceded by two-or-more spaces (``  #``)
                 * ``2`` — remove inline comments preceded by one-or-more spaces

      Returns:
          Trimmed string, or ``''`` for comment-only lines.
      """
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

   def set_help_path(self, progfile):
      """Set ``PGLOG['PUSGDIR']`` to the directory containing *progfile*.

      Args:
          progfile: Path to the calling program (typically ``__file__``).
      """
      self.PGLOG['PUSGDIR'] = op.dirname(op.abspath(progfile))

   def show_usage(self, progname, opts=None):
      """Display usage information from ``<progname>.usg`` then exit.

      When *opts* is provided, prints only the description of each listed
      option key extracted from the usage file.  Otherwise displays the full
      file via ``more``.

      Args:
          progname: Base program name (without ``.py``).  The file
                    ``<PUSGDIR>/<progname>.usg`` is read.
          opts:     Dict mapping option letter to ``[type, ...]`` where
                    ``type`` is 0=Mode, 1=Single-Value, 2=Multi-Value, else Action.
                    Pass ``None`` to display the full usage file.
      """
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
            nilcnt = begin = 0
            with open(usgname, 'r') as IN:
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
      else:
         os.system("more " + usgname)
      self.pgexit(0)

   def err2std(self, line):
      """Return 1 if *line* matches any pattern in ``PGLOG['ERR2STD']``, else 0.

      Used to redirect stderr lines to stdout when they match known patterns.
      """
      for err in self.PGLOG['ERR2STD']:
         if line.find(err) > -1: return 1
      return 0

   def std2err(self, line):
      """Return 1 if *line* matches any pattern in ``PGLOG['STD2ERR']``, else 0.

      Used to redirect stdout lines to stderr when they match known patterns.
      """
      for out in self.PGLOG['STD2ERR']:
         if line.find(out) > -1: return 1
      return 0

   def pgsystem(self, pgcmd, logact=None, cmdopt=5, instr=None, seconds=0):
      """Run a system command and log/return its output.

      Args:
          pgcmd:   Command to execute — either a string (``"ls -l"``) or a
                   list (``['ls', '-l']``).
          logact:  Logging action flags (default ``LOGWRN``).
          cmdopt:  Bitfield controlling logging and execution behaviour:

                   * ``1``    — log the command line
                   * ``2``    — log stdout
                   * ``4``    — log stderr as errors
                   * ``8``    — log command with timing (via :meth:`cmdlog`)
                   * ``16``   — return stdout string on success instead of ``SUCCESS``
                   * ``32``   — merge stderr into stdout
                   * ``64``   — return ``FAILURE`` if subprocess prints ``ABORTS``
                   * ``128``  — retry once on failure
                   * ``256``  — cache stderr in ``PGLOG['SYSERR']``
                   * ``512``  — log *instr* / *seconds* alongside command
                   * ``1024`` — force shell execution

          instr:   String fed to the command via stdin (default ``None``).
          seconds: Timeout in seconds; 0 means no timeout.

      Returns:
          Stdout string when ``cmdopt & 16``; otherwise ``SUCCESS`` or ``FAILURE``.
      """
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
            cmdpstr = self.command_path(cmdstr)
            if ret == self.FAILURE:
               error = "Error Execute: {}\n{}".format(cmdpstr, error)
            else:
               error = "Error From: {}\n{}".format(cmdpstr, error)
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

   @staticmethod
   def strip_output_line(line):
      """Strip carriage returns from a terminal output line.

      Also filters intermediate progress-bar lines (lines with a ``%``
      counter that is not 100).

      Args:
          line: A single output line (already stripped of surrounding whitespace).

      Returns:
          Cleaned string, ``None`` for suppressed progress lines, or the
          original *line* if no special characters are present.
      """
      ms = re.search(r'\r([^\r]+)\r*$', line)
      if ms: return ms.group(1)   
      ms = re.search(r'\s\.+\s+(\d+)%\s+', line)
      if ms and int(ms.group(1)) != 100: return None
      return line

   def cmd_execute_time(self, cmdstr, last, logact=None):
      """Append execution time to *cmdstr* when *last* meets the threshold.

      Args:
          cmdstr: Base command/label string.
          last:   Elapsed time in seconds.
          logact: When non-zero, passes the result to :meth:`pglog` and
                  returns its return value.  When zero/``None``, returns
                  the formatted string directly.

      Returns:
          Log return value when *logact* is set; formatted string otherwise.
      """
      msg = cmdstr
      if last >= self.PGLOG['CMDTIME']:   # show running for at least one minute
         msg += " ({})".format(self.seconds_to_string_time(last))
      if logact:
         return self.pglog(msg, logact)
      else:
         return msg

   @staticmethod
   def seconds_to_string_time(seconds, showzero=0):
      """Convert a duration in seconds to a compact human-readable string.

      Examples: ``90`` → ``"1M30S"``, ``3661`` → ``"1H1M1S"``.

      Args:
          seconds:  Duration in seconds (int or float).  Negative or zero
                    values produce an empty string unless *showzero* is set.
          showzero: When non-zero, returns ``"0S"`` for a zero-second duration.

      Returns:
          String composed of ``D`` / ``H`` / ``M`` / ``S`` components,
          with fractional seconds shown to 3 decimal places when *seconds*
          is a float.  Returns ``""`` for non-positive *seconds* unless
          *showzero* is set.
      """
      msg = ''
      if seconds > 0:
         minutes, s = divmod(seconds, 60)
         hours, m = divmod(int(minutes), 60)
         days, h = divmod(hours, 24)
         if days: msg += "{}D".format(days)
         if h: msg += "{}H".format(h)
         if m: msg += "{}M".format(int(m))
         if s:
            msg += "%dS" % s if isinstance(s, int) else "{:.3f}S".format(s)
      elif showzero:
         msg = "0S"
      return msg

   def tosystem(self, cmd, timeout=0, logact=0, cmdopt=5, instr=None):
      """Run a system command with a timeout via :meth:`pgsystem`.

      Args:
          cmd:     Command string or list (passed to :meth:`pgsystem`).
          timeout: Seconds before the command is killed.  Uses
                   ``PGLOG['TIMEOUT']`` when 0.
          logact:  Logging action flags.
          cmdopt:  Command option bitfield (see :meth:`pgsystem`).
          instr:   String passed to the command via stdin.

      Returns:
          ``SUCCESS``, ``FAILURE``, or captured stdout (when ``cmdopt & 16``).
      """
      if logact is None: logact = self.LOGWRN
      if not timeout: timeout = self.PGLOG['TIMEOUT']   # set default timeout if missed
      return self.pgsystem(cmd, logact, cmdopt, instr, timeout)

   @staticmethod
   def break_long_string(lstr, limit=1024, bsign="\n", mline=200, bchars=' &;', minlmt=20, eline=0):
      """Insert line-break markers into *lstr* and optionally truncate it.

      Lines longer than *limit* are broken at a character in *bchars* when
      possible, or hard-broken at *limit*.  The result is capped at *mline*
      lines; an optional tail of *eline* lines is preserved after the cap.

      Args:
          lstr:   Input string to wrap.
          limit:  Maximum line length before a break is inserted (default 1024).
          bsign:  Break marker inserted between segments (default ``"\\n"``).
          mline:  Maximum number of output lines/segments (default 200).
          bchars: Characters at which a soft break is preferred (default ``' &;'``).
          minlmt: Minimum position for a soft break; hard-breaks below this
                  (default 20).
          eline:  Number of trailing lines to preserve after *mline* is reached
                  (default 0).

      Returns:
          Wrapped (and possibly truncated) string.
      """
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

   @staticmethod
   def join_paths(path1, path2, diff=0):
      """Join or diff two POSIX paths, removing overlapping directory components.

      Args:
          path1: Left-hand path (base).
          path2: Right-hand path (to append or subtract).
          diff:  ``0`` — join *path1* and *path2* de-duplicating overlapping
                 tail/head components; ``1`` — remove *path1* prefix from
                 *path2* and return the remainder.

      Returns:
          Joined or relative path string.
      """
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

   def valid_batch_host(self, host, logact=0):
      """Return ``SUCCESS`` if *host* is a known batch host with an accessible submit command.

      Args:
          host:   Batch host name (case-insensitive).
          logact: Logging action flags passed to :meth:`valid_command` on failure.
      """
      HOST = host.upper()
      return self.SUCCESS if HOST in self.BCHCMDS and self.valid_command(self.BCHCMDS[HOST], logact) else self.FAILURE

   def valid_command(self, cmd, logact=0):
      """Return the full path of *cmd* if it is accessible and executable.

      Results are cached in ``self.COMMANDS``.

      Args:
          cmd:    Command name (with optional arguments, e.g. ``"rsync -a"``).
          logact: Logging action flags; when non-zero, logs an error if the
                  command is not found.

      Returns:
          Full path string (with arguments appended) on success; ``''`` if
          the command is not found.
      """
      ms = re.match(r'^(\S+)( .*)$', cmd)
      if ms:
         option = ms.group(2)
         cmd = ms.group(1)
      else:
         option = ''
      if cmd not in self.COMMANDS:
         buf = shutil.which(cmd)
         if buf is None:
            if logact: self.pglog("{}: executable command not found in\n{}".format(cmd, os.environ.get("PATH")), logact)
            buf = ''
         elif option:
            buf += option
         self.COMMANDS[cmd] = buf
      return self.COMMANDS[cmd]

   def command_path(self, cmdstr):
      """Return *cmdstr* with the command name replaced by its full path when available.

      Args:
          cmdstr: Command string (``"cmd arg1 arg2"``).

      Returns:
          String with the leading command resolved to its full path, or the
          original *cmdstr* if the command already contains a path separator
          or cannot be found via ``shutil.which``.
      """
      if not cmdstr: return ''
      ary = cmdstr.split(' ', 1)
      cmd = ary[0]
      if re.search(r'[\\/]', cmd): return cmdstr
      optstr = (' ' + ary[1]) if len(ary) > 1 else ''
      pcmd = shutil.which(cmd)
      return (pcmd+optstr) if pcmd else cmdstr

   def add_carbon_copy(self, cc=None, isstr=None, exclude=0, specialist=None):
      """Update the Cc address list in ``PGLOG['CCDADDR']``.

      Passing both *cc* and *isstr* as ``None`` clears the Cc list.

      Args:
          cc:         Address string (if *isstr*) or list of addresses.
                      ``None`` or empty clears the list.
          isstr:      When truthy, *cc* is treated as a comma/space-separated string.
          exclude:    String of addresses to skip (substring match).
          specialist: Address substituted when the sentinel value ``"S"``
                      appears in the address list.
      """
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

   def get_host(self, getbatch=0):
      """Return the short hostname of the current or batch server.

      Args:
          getbatch: When non-zero and a batch job ID is active, returns the
                    batch server name instead of the local hostname.

      Returns:
          Short hostname string (domain stripped).
      """
      if getbatch and self.PGLOG['CURBID'] != 0:
         host = self.PGLOG['PGBATCH']
      elif self.PGLOG['HOSTNAME']:
         return self.PGLOG['HOSTNAME']
      else:
         host = socket.gethostname()
      return self.get_short_host(host)

   def get_short_host(self, host):
      """Strip the domain suffix from *host* and return the short hostname.

      Args:
          host: Fully-qualified or short hostname string.

      Returns:
          Short hostname, or an uppercase batch-host token when the host
          matches a known batch system.
      """
      if not host: return ''
      ms = re.match(r'^([^\.]+)\.', host)
      if ms: host = ms.group(1)
      if self.PGLOG['HOSTNAME'] and (host == 'localhost' or host == self.PGLOG['HOSTNAME']): return self.PGLOG['HOSTNAME']
      HOST = host.upper()
      if HOST in self.BCHCMDS: return HOST
      return host

   def get_pbs_host(self):
      """Return the first live PBS host from ``self.PBSHOSTS``, or ``None``."""
      if not self.PBSSTATS and self.PGLOG['PBSHOSTS']:
         self.PBSHOSTS = self.PGLOG['PBSHOSTS'].split(':')
         for host in self.PBSHOSTS:
            self.PBSSTATS[host] = 1
      for host in self.PBSHOSTS:
         if host in self.PBSSTATS and self.PBSSTATS[host]: return host
      return None

   def set_pbs_host(self, host=None, stat=0):
      """Set the live/dead status for one or all PBS hosts.

      Args:
          host: Host name to update.  When ``None``, updates all known hosts.
          stat: ``1`` for live, ``0`` for dead (default 0).
      """
      if host:
         self.PBSSTATS[host] = stat
      else:
         if not self.PBSHOSTS and self.PGLOG['PBSHOSTS']:
            self.PBSHOSTS = self.PGLOG['PBSHOSTS'].split(':')
         for host in self.PBSHOSTS:
            self.PBSSTATS[host] = stat

   def reset_batch_host(self, bhost, logact=None):
      """Change the active batch host to *bhost* if no job is currently running.

      Args:
          bhost:  New batch host name (case-insensitive).
          logact: Logging action flags (default ``LOGWRN``).
      """
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

   @staticmethod
   def get_command(cmdstr=None):
      """Return the base command name, stripping directory and ``.py``/``.pl`` extension.

      Args:
          cmdstr: Path string.  Defaults to ``sys.argv[0]``.

      Returns:
          Base name without extension.
      """
      if not cmdstr: cmdstr = sys.argv[0]
      cmdstr = op.basename(cmdstr)
      ms = re.match(r'^(.+)\.(py|pl)$', cmdstr)
      if ms:
         return ms.group(1)
      else:
         return cmdstr

   def get_local_command(self, cmd, asuser=None):
      """Wrap *cmd* so it runs as *asuser* on the local host.

      Uses a ``pgstart_<user>`` setuid wrapper when available, or
      ``sudo -u <user>`` when ``SUDOGDEX`` is enabled.

      Args:
          cmd:    Command string to wrap.
          asuser: Target username.  Returns *cmd* unchanged when ``None`` or
                  equal to the current user.

      Returns:
          Wrapped command string, or the original *cmd* if no wrapping is needed.
      """
      cuser = self.PGLOG['SETUID'] if self.PGLOG['SETUID'] else self.PGLOG['CURUID']
      if not asuser or cuser == asuser: return cmd
      if cuser == self.PGLOG['GDEXUSER']:
         wrapper = "pgstart_" + asuser
         if self.valid_command(wrapper): return "{} {}".format(wrapper, cmd)
      elif self.PGLOG['SUDOGDEX'] and asuser == self.PGLOG['GDEXUSER']:
         return "sudo -u {} {}".format(self.PGLOG['GDEXUSER'], cmd)    # sudo as user gdexdata
      return cmd

   def get_remote_command(self, cmd, host, asuser=None):
      """Wrap *cmd* for execution as *asuser* on *host* (delegates to :meth:`get_local_command`).

      Args:
          cmd:    Command string to wrap.
          host:   Target hostname (currently unused; reserved for future SSH wrapping).
          asuser: Target username.

      Returns:
          Wrapped command string.
      """
      return self.get_local_command(cmd, asuser)

   def get_sync_command(self, host, asuser=None):
      """Return the sync command name for *host* with appropriate user context.

      Args:
          host:   Target hostname.
          asuser: User to run as; affects which sync command variant is chosen.

      Returns:
          Sync command string (e.g. ``"synccasper"`` or ``"casper-sync"``).
      """
      host = self.get_short_host(host)
      if (not (self.PGLOG['SETUID'] and self.PGLOG['SETUID'] == self.PGLOG['GDEXUSER']) and
         (not asuser or asuser == self.PGLOG['GDEXUSER'])):
         return "sync" + host
      return host + "-sync"

   def set_suid(self, cuid=0):
      """Set the real and effective UID to *cuid* and update ``SETUID``.

      Calls :meth:`set_specialist_environments` when switching to a
      non-gdex specialist user.

      Args:
          cuid: Target numeric UID.  Defaults to the current effective UID.
      """
      if not cuid: cuid = self.PGLOG['EUID']
      if cuid != self.PGLOG['EUID'] or cuid != self.PGLOG['RUID']:
         os.setreuid(cuid, cuid)
         self.PGLOG['SETUID'] = pwd.getpwuid(cuid).pw_name
         if not (self.PGLOG['SETUID'] == self.PGLOG['GDEXUSER'] or cuid == self.PGLOG['RUID']):
            self.set_specialist_environments(self.PGLOG['SETUID'])
            self.PGLOG['CURUID'] == self.PGLOG['SETUID']      # set CURUID to a specific specialist

   def set_common_pglog(self):
      """Initialise runtime ``PGLOG`` values from the environment.

      Detects the current user, hostname, PBS job state, and constructs the
      ``PATH`` environment variable.  Also sets all path-related ``PGLOG``
      keys (``LOGPATH``, ``DSSDATA``, ``TMPDIR``, etc.) by reading environment
      variables of the same name via :meth:`SETPGLOG`.

      Called automatically by :meth:`__init__`.
      """
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
      self.PGLOG['LOCHOME'] = "/ncar/gdex/setuid"
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
      self.SETPGLOG("TACCEP", "gdex-tacc")                      # default Globus Endpoint on TACC
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

   def get_tmpsync_path(self):
      """Return the path to the temporary sync directory (``PGLOG['TMPSYNC']``)."""
      return self.PGLOG['TMPSYNC']

   def add_local_path(self, locpath, pgpath, append=0):
      """Add colon-separated paths from *locpath* to *pgpath* without duplicates.

      Args:
          locpath: Colon-separated path string to merge in.
          pgpath:  Existing path string to update.
          append:  ``1`` to append, ``0`` to prepend (default 0).

      Returns:
          Updated colon-separated path string.
      """
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

   def SETPGLOG(self, name, value=''):
      """Set ``PGLOG[name]`` from the environment variable *name* or fall back to *value*.

      If the environment variable is set and non-empty it takes precedence.
      If not, the existing ``PGLOG[name]`` value is kept (if present), then
      *value* is used as the final default.  Values starting with ``PG``
      are treated as unresolved placeholders and replaced with ``''``.

      Args:
          name:  ``PGLOG`` key and environment variable name.
          value: Default value when neither the environment nor an existing
                 ``PGLOG`` entry is available.
      """
      oval = self.PGLOG[name] if name in self.PGLOG else ''
      nval = self.get_environment(name, ('' if re.match('PG', value) else value))
      self.PGLOG[name] = nval if nval else oval

   def set_specialist_home(self, specialist):
      """Set ``HOME`` for *specialist* and return their default shell.

      Reads ``/etc/passwd`` to determine the home directory and shell.
      Updates ``HOME`` in the environment when the path exists.

      Args:
          specialist: Login name of the specialist user.

      Returns:
          Shell basename string (e.g. ``"tcsh"``).
      """
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

   def set_specialist_environments(self, specialist):
      """Parse *specialist*'s ``~/.tcshrc`` and apply ``setenv`` directives.

      Respects host-conditional ``if``/``else``/``endif`` blocks so that
      only the directives matching the current hostname are applied.  Skips
      ``PATH``, ``SHELL``, ``IFS``, and ``CDPATH`` for security.

      Args:
          specialist: Login name of the specialist user.
      """
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

   def one_specialist_environment(self, line):
      """Parse and apply a single ``setenv VAR VALUE`` statement.

      Expands ``$VAR`` references in the value.  Skips protected variables
      (``PATH``, ``SHELL``, ``IFS``, ``CDPATH``).

      Args:
          line: String after the ``setenv`` keyword (``"VAR VALUE"``).
      """
      ms = re.match(r'^(\w+)[=\s]+(.+)$', line)
      if not ms: return
      (var, val) = ms.groups()
      if re.match(r'^(PATH|SHELL|IFS|CDPATH|)$', var): return
      if val.find('$') > -1: val = self.replace_environments(val)
      ms = re.match(r'^(\"|\')(.*)(\"|\')$', val)
      if ms: val = ms.group(2)   # remove quotes
      os.environ[var] = val
   
   def replace_environments(self, envstr, default='', logact=0):
      """Expand the first ``$VAR`` or ``${VAR}`` reference in *envstr*.

      Looks up the variable in the environment first, then in ``PGLOG``,
      then falls back to *default*.

      Args:
          envstr:  String containing a ``$VAR`` reference.
          default: Fallback value (string or dict).  When a dict, the variable
                   name is used as the key.
          logact:  Logging action flags passed to :meth:`get_environment`.

      Returns:
          String with the first variable reference substituted.
      """
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

   def check_process_host(self, hosts, chost=None, mflag=None, pinfo=None, logact=None):
      """Check whether the current host is permitted to process *pinfo*.

      Args:
          hosts:  Host string or pattern.  Prefix ``!`` to exclude listed hosts.
          chost:  Host to check against (default: current/batch host).
          mflag:  Match mode — ``'G'`` general, ``'M'`` exact, ``'I'`` inclusive.
          pinfo:  Process description logged on failure.
          logact: Logging action flags (default ``LOGERR``).

      Returns:
          ``1`` if processing is permitted; ``0`` otherwise.
      """
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

   @staticmethod
   def convert_chars(name, default='X'):
      """Transliterate *name* to ASCII-safe characters.

      Uses ``unidecode`` to transliterate Unicode characters, then strips any
      remaining non-alphanumeric / non-underscore characters.

      Args:
          name:    Input string to convert.
          default: Return value when *name* is empty or fully non-convertible.

      Returns:
          ASCII-safe alphanumeric/underscore string, or *default*.
      """
      if not name: return default
      if re.match(r'^[a-zA-Z0-9]+$', name): return name  # conversion not needed
      decoded_name = unidecode(name).strip()
      # remove any non-alphanumeric and non-underscore characters
      cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '', decoded_name)
      if cleaned_name:
         return cleaned_name
      else:
         return default

   def current_process_info(self, realpid=0):
      """Return ``[hostname, pid]`` for the current or batch process.

      Args:
          realpid: When non-zero, always returns the real OS PID.
                   When zero and a batch job is active, returns the batch
                   server name and job ID instead.

      Returns:
          List of ``[hostname_or_batch, pid_or_jobid]``.
      """
      if realpid or self.PGLOG['CURBID'] < 1:
         return [self.PGLOG['HOSTNAME'], os.getpid()]
      else:
         return [self.PGLOG['PGBATCH'], self.PGLOG['CURBID']]

   def argv_to_string(self, argv=None, quote=1, action=None):
      """Convert an argument list to a shell-safe string.

      Arguments containing shell special characters (``< > | whitespace``)
      are single-quoted (or double-quoted when they contain single quotes).

      Args:
          argv:   List of argument strings.  Defaults to ``sys.argv[1:]``.
          quote:  When non-zero, quotes arguments with special characters.
          action: When set, calls :meth:`pglog` with ``LGEREX`` if any
                  argument contains a special character (safety guard).

      Returns:
          Space-joined argument string.
      """
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

   @staticmethod
   def int2base(x, base):
      """Convert integer *x* to a string in the given *base*.

      Args:
          x:    Integer to convert.
          base: Target numeric base (e.g. 8 for octal, 16 for hex).

      Returns:
          String representation of *x* in *base*, with a leading ``'-'``
          for negative values.
      """
      if x == 0: return '0'
      negative = 0
      if x < 0:
         negative = 1
         x = -x
      dgts = []
      while x:
         dgts.append(str(x % base))
         x //= base
      if negative: dgts.append('-')
      dgts.reverse()
      return ''.join(dgts)

   @staticmethod
   def base2int(x, base):
      """Convert a decimal-encoded *base*-number string back to a plain integer.

      The input *x* is a decimal integer whose digits represent a number
      written in *base* (e.g. ``x=1010, base=2`` → ``10``).

      Args:
          x:    Integer or string whose digits represent a base-*base* number.
          base: Source numeric base.

      Returns:
          Decoded integer value.
      """
      if not isinstance(x, int): x = int(x)
      if x == 0: return 0
      negative = 0
      if x < 0:
         negative = 1
         x = -x
      num = 0
      fact = 1
      while x:
         num += (x % 10) * fact
         fact *= base
         x //= 10
      if negative: num = -num
      return num

   @staticmethod
   def int2order(num):
      """Return the ordinal string for *num* (e.g. ``1`` → ``"1st"``).

      Args:
          num: Non-negative integer.

      Returns:
          String with appropriate suffix: ``st``, ``nd``, ``rd``, or ``th``.
      """
      ordstr = ['th', 'st', 'nd', 'rd']
      snum = str(num)
      num %= 100
      if num > 19: num %= 10
      if num > 3: num = 0   
      return snum + ordstr[num]
