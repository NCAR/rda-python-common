###############################################################################
#     Title: pg_opt.py
#    Author: Zaihua Ji,  zji@ucar.edu
#      Date: 08/26/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-12-01 convert to class PgOPT
#   Purpose: python library module for holding global varaibles
#             functions for processing options and other global functions
#    Github: https://github.com/NCAR/rda-pyhon-common.git
###############################################################################
import os
import sys
import re
import time
from os import path as op
from .pg_file import PgFile

class PgOPT(PgFile):
   """Command-line option parsing and application configuration framework for RDA tools.

   Provides option parsing, parameter management, dataset validation, and output
   formatting for RDA tools such as dsarch, dsupdt, and dsrqst.  Manages option
   definitions (``OPTS``), parsed parameter values (``params``), command-line vs.
   input-file option tracking, and structured output.

   Attributes:
      OUTPUT: File handle for result output (stdout or a file).
      CMDOPTS (dict): Options set on the command line, keyed by short option name.
      INOPTS (dict): Options read from input files, keyed by short option name.
      ALIAS (dict): Alias mappings for option names.
      TBLHASH (dict): Table-field hash definitions used for record building/querying.
      OPTS (dict): Valid option definitions; each entry is a list whose first
         element encodes the option type (0=mode, 1=single-value, 2=multi-value,
         >=4=action).
      PGOPT (dict): Global operational settings (current action bits, limits,
         program names, log-action masks, etc.).
      params (dict): Parsed parameter values keyed by short option name.
      WTYPE (dict): Web-type code-to-name mapping.
      HTYPE (dict): Help-type code-to-name mapping.
      HPATH (dict): Help-type code-to-path mapping.
      MTYPE (dict): Media-type code-to-name mapping.
      STYPE (dict): Storage-type code-to-name mapping.
      BTYPE (dict): Backup-type code-to-name mapping.
   """

   def __init__(self):
      """Initialize PgOPT and its parent PgFile, setting up all class attributes."""
      super().__init__()  # initialize parent class
      self.OUTPUT = None
      self.CMDOPTS = {}
      self.INOPTS = {}
      # global variables are used by all applications and this package.
      # they need be initialized in application specified packages
      self.ALIAS = {}
      self.TBLHASH = {}
      ###############################################################################
      # valid options the first hash value: 0 means mode option, 1 means single-value
      # option, 2 means multiple-value option, and >=4 means action option the second
      # hash values are long option names, either hash keys (considered as short
      # option names) or the associated long names can be used.  All options, except for
      # multi-line value ones, can be specified on command line, while single-value and
      # multi-value options, except option -IM for input files, can also given in input
      # files long value option names are used in output files all letters of option
      # names are case insensitive.
      #
      # The third hash value define bit flags,
      # For Action Options:
      # -1 - VSN card actions
      # >0 - set actions
      #
      # For Mode Options:
      # 1 - mode for archiving actions
      # 2 - mode for set actions
      #
      # For Single-Value Info Options:
      #   1(0x001) - auto set value
      #   2(0x002) - manually set value
      #  16(0x010) - convert to integer from commandline and input files, set to 0 if empty
      #  32(0x020) - time field
      # 128(0x080) - '' allowed for single letter value
      # 256(0x100) - date field
      #
      # For Multi-Value Info Options:
      #   1(0x001) - one for multiple
      #   2(0x002) - auto-set,
      #   4(0x004) - expanded from one
      #   8(0x008) - validated
      #  16(0x010) - convert to integer from commandline and input files, set to 0 if empty
      #  32(0x020) - time field
      #  64(0x040) - text field allowing multiple lines
      # 128(0x080) - '' allowed for single letter value
      # 256(0x100) - date field
      #
      # The fourth hash values defined restriction for single letter values
      ###############################################################################
      self.OPTS = {}
      # global initial optional values
      self.PGOPT = {
         'ACTS': 0,    # carry current action bits
         'UACTS': 0,    # carry dsarch skip check UD action bits
         'CACT': '',   # current short action name
         'IFCNT': 0,    # 1 to read a single Input File at a time
         'ANAME': '',   # cache the application name if set
         'TABLE': '',   # table name the action is on
         'UID': 0,    # user.uid
         'MSET': 'SA', # Action for multiple sets
         'WIDTH': 128,  # max column width
         'TXTBIT': 64,   # text field bit (0x1000) allow multiple lines
         'PEMAX': 12,   # max count of reuqest partition errors for auto reprocesses
         'PTMAX': 24,   # max number of partitions for a single request
         'REMAX': 2,    # max count of reuqest errors for auto reprocesses
         'RSMAX': 100,  # max count of gatherxml with options -R -S
         'RCNTL': None,  # placehold for a request control record
         'dcm': "dcm",
         'sdp': "sdp",
         'rcm': "rcm",
         'scm': "scm",
         'wpg': "",
         'gatherxml': "gatherxml",
         'cosconvert': "cosconvert",
         'emllog': self.LGWNEM,
         'emlerr': self.LOGERR|self.EMEROL,
         'emerol': self.LOGWRN|self.EMEROL,
         'emlsum': self.LOGWRN|self.EMLSUM,
         'emlsep': self.LGWNEM|self.SEPLIN,
         'wrnlog': self.LOGWRN,
         'errlog': self.LOGERR,
         'extlog': self.LGEREX,
         'PTYPE': "CPRV",
         'WDTYP': "ADNU",
         'HFTYP': "DS",
         'SDTYP': "PORWUV",
         'GXTYP': "DP"
      }
      # global default parameters
      self.params = {
         'ES': "<=>",
         'AO': "<!>",
         'DV': "<:>"
      }
      self.WTYPE = {
         'A': "ARCO",
         'D': "DATA",
         'N': "NCAR",
         'U': "UNKNOWN",
      }
      self.HTYPE = {
         'D': "DOCUMENT",
         'S': "SOFTWARE",
         'U': "UNKNOWN"
      }
      self.HPATH = {
         'D': "docs",
         'S': "software",
         'U': "help"
      }
      self.MTYPE = {
         'P': "PRIMARY",
         'A': "ARCHIVING",
         'V': "VERSION",
         'W': "WORKING",
         'R': "ORIGINAL",
         'B': "BACKUP",
         'O': "OFFSITE",
         'C': "CHRONOPOLIS",
         'U': "UNKNOWN"
      }
      self.STYPE = {
         'O': "OFFLINE",
         'P': "PRIMARY",
         'R': "ORIGINAL",
         'V': "VERSION",
         'W': "WORKING",
         'U': "UNKNOWN"
      }
      self.BTYPE = {
         'B': "BACKUPONLY",
         'D': "BACKDRDATA",
      }

   def parsing_input(self, aname):
      """Process and parse command-line and input-file options for the application.

      Reads ``sys.argv``, resolves option keys, fills ``self.params``, processes
      any input files listed via ``-IF``, and validates the final state.

      Args:
         aname (str): Application name (e.g. ``'dsarch'``, ``'dsupdt'``).
      """
      self.PGLOG['LOGFILE'] = aname + ".log"
      self.PGOPT['ANAME'] = aname
      self.dssdb_dbname()
      argv = sys.argv[1:]
      if not argv: self.show_usage(aname)
      self.cmdlog("{} {}".format(aname, ' '.join(argv)))
      # process command line options to fill option values
      option = infile = None
      needhelp = 0
      helpopts = {}
      for param in argv:
         if re.match(r'^(-{0,2}help|-H)$', param, re.I):
            if option: helpopts[option] = self.OPTS[option]
            needhelp = 1
            continue
         ms = re.match(r'^-([a-zA-Z]\w*)$', param)
         if ms:     # option parameter
            param = ms.group(1)
            if option and not needhelp and option not in self.params:
               val = self.get_default_info(option)
               if val is not None:
                  self.set_option_value(option, val)
               else:
                  self.parameter_error("-" + option, "missval")
            option = self.get_option_key(param)
            if needhelp:
               helpopts[option] = self.OPTS[option]
               break
            # set mode/action options
            if self.OPTS[option][0]&3 == 0: self.set_option_value(option)
         elif option:
            ms =re.match(r"^\'(.*)\'$", param)
            if ms: param = ms.group(1)
            self.set_option_value(option, param)
         elif self.find_dataset_id(param):
            self.set_option_value('DS', param)
         else:
            option = self.get_option_key(param, 3, 1)
            if option:
               self.set_option_value(option)
               if needhelp:
                  helpopts[option] = self.OPTS[option]
                  break
            elif op.exists(param): # assume input file
               infile = param
            else:
               self.parameter_error(param)
      if needhelp: self.show_usage(aname, helpopts)
      if option and option not in self.params:
         val = self.get_default_info(option)
         if val is not None:
            self.set_option_value(option, val)
         else:
            self.parameter_error("-" + option, "missval")
      # check if only an input filename is given on command line following aname
      if infile:
         if 'IF' in self.params:
            self.parameter_error(infile)
         else:
           self.params['IF'] = [infile]
      # process given one or multiple input files to fill option values
      if 'IF' in self.params:
         self.PGOPT['IFCNT'] = 1 if self.PGOPT['CACT'] == 'AQ' else 0
         if self.OPTS['DS'][0] == 1:
            param = self.validate_infile_names(self.params['DS']) if 'DS' in self.params else 0
         else:
            param = 1
         self.get_input_info(self.params['IF'])
         if not param and 'DS' in self.params: self.validate_infile_names(self.params['DS'])
      if not self.PGOPT['ACTS']: self.parameter_error(aname, "missact")   # no action enter
      if 'DB' in self.params:
         dcnt = len(self.params['DB'])
         for i in range(dcnt):
            if i == 0:
               self.PGLOG['DBGLEVEL'] = self.params['DB'][0]
            elif i == 1:
               self.PGLOG['DBGPATH'] = self.params['DB'][1]
            elif i == 2:
               self.PGLOG['DBGFILE'] = self.params['DB'][2]
         self.pgdbg(self.PGLOG['DBGLEVEL'])
      if 'GZ' in self.params: self.PGLOG['GMTZ'] = self.diffgmthour()
      if 'BG' in self.params: self.PGLOG['BCKGRND'] = 1

   def get_default_info(self, opt):
      """Check and retrieve the default value for an info option.

      Args:
         opt (str): Short option key to look up in ``self.OPTS``.

      Returns:
         The default value (int, str, or ``None``) if one is defined for the
         option; ``None`` otherwise.
      """
      olist = self.OPTS[opt]
      if olist[0]&3 and len(olist) > 3:
         odval = olist[3]
         if not odval or isinstance(odval, int):
            return odval
         else:
            return odval[0]   # return the first char of a default string
      return None

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
      else:                               # result to STDOUT
         self.OUTPUT = sys.stdout

   def validate_infile_names(self, dsid):
      """Validate all input file names against the expected dataset ID.

      Args:
         dsid (str): Expected dataset ID that every input file name must encode.

      Returns:
         int: Count of validated files on success, or ``self.FAILURE`` on the
         first validation error.
      """
      i = 0
      for infile in self.params['IF']:
         if not self.validate_one_infile(infile, dsid): return self.FAILURE
         i += 1
         if self.PGOPT['IFCNT'] and i >= self.PGOPT['IFCNT']: break
      return i

   def validate_one_infile(self, infile, dsid):
      """Validate a single input file name against the expected dataset ID.

      Args:
         infile (str): Path to the input file whose name is validated.
         dsid (str): Expected dataset ID to match against the file name.

      Returns:
         int: ``self.SUCCESS`` if the file encodes the correct dataset ID;
         logs a fatal error and does not return otherwise.
      """
      ndsid = self.find_dataset_id(infile)
      if ndsid is None:
         return self.pglog("{}: No dsid identified in Input file name {}!".format(dsid, infile), self.PGOPT['extlog'])
      fdsid = self.format_dataset_id(ndsid)
      if fdsid != dsid:
         return self.pglog("{}: Different dsid {} found in Input file name {}!".format(dsid, fdsid, infile), self.PGOPT['extlog'])
      return self.SUCCESS

   def get_input_info(self, infiles, table=None):
      """Gather input option values from a list of input files.

      Args:
         infiles (list[str]): Paths to input files to process.
         table (str, optional): Table name to scope option gathering; ``None``
            to gather all options.

      Returns:
         int: Total count of options read across all processed files.
      """
      i = 0
      for file in infiles:
         i += self.process_infile(file, table)
         if not self.PGOPT['IFCNT'] and self.PGOPT['CACT'] == 'AQ': self.PGOPT['IFCNT'] = 1
         if self.PGOPT['IFCNT']: break
      return i

   def read_one_infile(self, infile):
      """Validate and read option values from a single input file.

      Clears existing input-file option values, processes the file, and
      validates that its embedded dataset ID matches the current ``params['DS']``.

      Args:
         infile (str): Path to the input file to read.

      Returns:
         str: The resolved dataset ID after processing the file.
      """
      dsid = self.params['DS']
      del self.params['DS']
      if self.OPTS['DS'][2]&2: self.OPTS['DS'][2] &= ~2
      if 'DS' in self.CMDOPTS: del self.CMDOPTS['DS']
      self.clean_input_values()
      self.process_infile(infile)
      if 'DS' in self.params: dsid = self.params['DS']
      if dsid: self.validate_one_infile(infile, dsid)
      return dsid

   def process_infile(self, infile, table=None):
      """Gather input option values from one input file.

      Parses single-value assignments, action/mode options, and multi-value
      column blocks from the file.  Logs progress for large files.

      Args:
         infile (str): Path to the input file.
         table (str, optional): Table name to look for inside the file.  When
            given, only the section delimited by ``[table]`` is read.

      Returns:
         int: ``1`` if at least one option was set, ``0`` if nothing was read
         (only possible when *table* is specified).
      """
      if not op.exists(infile): self.pglog(infile + ": Input file not exists", self.PGOPT['extlog'])
      if table:
         self.pglog("Gather '{}' information from input file '{}'..." .format(table, infile), self.PGOPT['wrnlog'])
      else:
         self.pglog("Gather information from input file '{}'...".format(infile), self.PGOPT['wrnlog'])
      try:
         fd = open(infile, 'r')
      except Exception as e:
         self.pglog("{}: Error Open input file - {}!".format(infile, str(e)), self.PGOPT['extlog'])
      else:
         lines = fd.readlines()
         fd.close()
      opt = None
      columns = []
      chktbl = 1 if table else -1
      mpes = r'^(\w+)\s*{}\s*(.*)$'.format(self.params['ES'])
      mpao = r'^(\w+)\s*{}'.format(self.params['AO'])
      # column count, column index, value count, value index, line index, option-set count, end divider flag
      colcnt = colidx = valcnt = validx = linidx = setcnt = enddiv = 0
      for line in lines:
         linidx += 1
         if linidx%50000 == 0:
            self.pglog("{}: {} lines read".format(infile, linidx), self.PGOPT['wrnlog'])
         if 'NT' not in self.params: line = self.pgtrim(line, 2)
         if not line:
            if opt: self.set_option_value(opt, '', 1, linidx, line, infile)
            continue   # skip empty lines
         if chktbl > 0:
            if re.match(r'^\[{}\]$'.format(table), line, re.I): # found entry for table
               chktbl = 0
               self.clean_input_values()   # clean previously saved input values
            continue
         else:
            ms = re.match(r'^\[(\w+)\]$', line)
            if ms:
               if chktbl == 0: break     # stop at next sub-title
               if not self.PGOPT['MSET']:
                  self.input_error(linidx, line, infile, ms.group(1) + ": Cannt process sub-title")
               elif self.PGOPT['CACT'] != self.PGOPT['MSET']:
                  self.input_error(linidx, line, infile, "Use Action -{} to Set multiple sub-titles".format(self.PGOPT['MSET']))
               break   # stop getting info if no table given or a different table
         if colcnt == 0:    # check single value and action lines first
            ms = re.match(mpes, line)
            if ms:   # one value assignment
               key = ms.group(1).strip()
               val = ms.group(2)
               if val and 'NT' not in self.params: val = val.strip()
               opt = self.get_option_key(key, 1, 0, linidx, line, infile, table)
               self.set_option_value(opt, val, 0, linidx, line, infile)
               if not self.OPTS[opt][2]&self.PGOPT['TXTBIT']: opt = None
               setcnt += 1
               continue
            ms = re.match(mpao, line)
            if ms:    # set mode or action option
               key = self.get_option_key(ms.group(1).strip(), 4, 0, linidx, line, infile, table)
               self.set_option_value(key, '', 0, linidx, line, infile)
               setcnt += 1
               continue
         # check mutiple value assignment for one or more multi-value options
         values = line.split(self.params['DV'])
         valcnt = len(values)
         if colcnt == 0:
            while colcnt < valcnt:
               key = values[colcnt].strip()
               if not key: break
               opt = self.get_option_key(key, 2, 1, linidx, line, infile, table)
               if not opt: break
               columns.append(opt)
               if opt in self.params: del self.params[opt]
               colcnt += 1
            if colcnt < valcnt:
               if colcnt == (valcnt-1):
                  enddiv = 1
               else:
                  self.input_error(linidx, line, infile, "Multi-value Option Name missed for column {}".format(colcnt+1))
            opt = None
            continue
         elif valcnt == 1:
            if re.match(mpes, line):
               self.input_error(linidx, line, infile, "Cannot set single value option after Multi-value Options")
            elif re.match(mpao, line):
               self.input_error(linidx, line, infile, "Cannot set acttion/mode option after Multi-value Options")
         if opt:  # add to multipe line value
            val = values.pop(0)
            valcnt -= 1
            if val and 'NT' not in self.params: val = val.strip()
            self.set_option_value(opt, val, 1, linidx, line, infile)
            setcnt += 1
            if valcnt == 0: continue   # continue to check multiple line value
            colidx += 1
            opt = None
         reduced = 0
         valcnt += colidx
         if valcnt > colcnt:
            if enddiv:
               val = values.pop()
               if not val.strip():
                  valcnt -= 1
                  reduced = 1
            if valcnt > colcnt:
               self.input_error(linidx, line, infile, "Too many values({}) provided for {} columns".format(valcnt+colidx, colcnt))
         if values:
            for val in values:
               opt = columns[colidx]
               colidx += 1
               if val and 'NT' not in self.params: val = val.strip()
               self.set_option_value(opt, val, 0, linidx, line, infile)
               setcnt += 1
            colidx += (reduced-enddiv)
         if colidx == colcnt:
            colidx = 0   # done with gathering values of a multi-value line
            opt = None
         elif opt and not self.OPTS[opt][2]&self.PGOPT['TXTBIT']:
            colidx += 1
            opt = None
      if setcnt > 0:
         if colidx:
            if colidx < colcnt:
               self.input_error(linidx, '', infile, "{} of {} values missed".format(colcnt-colidx, colcnt))
            elif enddiv:
               self.input_error(linidx, '', infile, "Miss end divider '{}'".format(self.params['DV']))
         return 1   # read something
      else:
         if table: self.pglog("No option information found for '{}'".format(table), self.WARNLG)
         return 0  # read nothing

   def clean_input_values(self):
      """Remove all input-file option values from ``self.params``.

      Iterates over ``self.INOPTS`` and deletes the corresponding keys from
      ``self.params``, then clears ``self.INOPTS``.
      """
      # clean previously saved input values if any
      for opt in self.INOPTS:
         del self.params[opt]
      self.INOPTS = {}

   def build_record(self, flds, pgrec, tname, idx=0):
      """Build a database record dict for adding or updating a table row.

      Args:
         flds (list[str]): Field keys to include in the record.
         pgrec (dict | None): Existing database record to compare against; used
            to skip unchanged or auto-set fields.
         tname (str): Table name whose ``TBLHASH`` entry maps fields to options.
         idx (int, optional): Row index for multi-value options.  Defaults to 0.

      Returns:
         dict: Mapping of database field names to new (or changed) values.
      """
      record = {}
      if not flds: return record
      hash = self.TBLHASH[tname]
      for key in flds:
         if key not in hash: continue
         opt = hash[key][0]
         field = hash[key][3] if len(hash[key]) == 4 else hash[key][1]
         ms = re.search(r'\.(.+)$', field)
         if ms: field = ms.group(1)
         if opt in self.params:
            if self.OPTS[opt][0] == 1:
               val = self.params[opt]
            else:
               if self.OPTS[opt][2]&2 and pgrec and field in pgrec and pgrec[field]: continue
               val = self.params[opt][idx]
            sval = pgrec[field] if pgrec and field in pgrec else None
            if sval is None:
               if val == '': val = None
            elif isinstance(sval, int):
               if isinstance(val, str): val = (int(val) if val else None)   # change '' to None for int
            if self.pgcmp(sval, val, 1): record[field] = val    # record new or changed value
      return record

   def set_uid(self, aname):
      """Set ``self.PGOPT['UID']`` from the database for the current login user.

      Also configures email addresses, validates the dataset and its owner, and
      opens the output destination.

      Args:
         aname (str): Application name used in log messages.
      """
      self.set_email_logact()
      if 'LN' not in self.params:
         self.params['LN'] = self.PGLOG['CURUID']
      elif self.params['LN'] != self.PGLOG['CURUID']:
         self.params['MD'] = 1  # make sure this set if running as another user
         if 'NE' not in self.params: self.PGLOG['EMLADDR'] = self.params['LN']
         if 'DM' in self.params and re.match(r'^(start|begin)$', self.params['DM'], re.I):
            msg = "'{}' must start Daemon '{} -{}' as '{}'".format(self.PGLOG['CURUID'], aname, self.PGOPT['CACT'], self.params['LN'])
         else:
            msg = "'{}' runs '{} -{}' as '{}'!".format(self.PGLOG['CURUID'], aname, self.PGOPT['CACT'], self.params['LN'])
         self.pglog(msg,  self.PGOPT['wrnlog'])
         self.set_specialist_environments(self.params['LN'])
      if 'LN' not in self.params: self.pglog("Could not get user login name", self.PGOPT['extlog'])
      self.validate_dataset()
      if self.OPTS[self.PGOPT['CACT']][2] > 0: self.validate_dsowner(aname)
      pgrec = self.pgget("dssdb.user", "uid", "logname = '{}' AND until_date IS NULL".format(self.params['LN']), self.PGOPT['extlog'])
      if not pgrec: self.pglog("Could not get user.uid for " + self.params['LN'], self.PGOPT['extlog'])
      self.PGOPT['UID'] = pgrec['uid']
      self.open_output(self.params['OF'] if 'OF' in self.params else None)

   def set_sudo_uid(self, aname, uid):
      """Set ``self.PGOPT['UID']`` to 0 for a sudo-capable user.

      Logs a warning when the current user differs from the required *uid*,
      then records ``uid`` 0 and the current login name.

      Args:
         aname (str): Application name used in log messages.
         uid (str): Expected user ID that the application must run as.
      """
      self.set_email_logact()
      if self.PGLOG['CURUID'] != uid:
         if 'DM' in self.params and re.match(r'^(start|begin)$', self.params['DM'], re.I):
            msg = "'{}': must start Daemon '{} -{}' as '{}'".format(self.PGLOG['CURUID'], aname, self.PGOPT['CACT'], uid)
         else:
            msg = "'{}': must run '{} -{}' as '{}'".format(self.PGLOG['CURUID'], aname, self.PGOPT['CACT'], uid)
         self.pglog(msg, self.PGOPT['extlog'])
      self.PGOPT['UID'] = 0
      self.params['LN'] = self.PGLOG['CURUID']

   def set_root_uid(self, aname):
      """Set ``self.PGOPT['UID']`` to 0, requiring the caller to be ``root``.

      Logs a fatal error if the current user is not ``root``.

      Args:
         aname (str): Application name used in log messages.
      """
      self.set_email_logact()
      if self.PGLOG['CURUID'] != "root":
         if 'DM' in self.params and re.match(r'^(start|begin)$', self.params['DM'], re.I):
            msg = "'{}': you must start Daemon '{} -{}' as 'root'".format(self.PGLOG['CURUID'], aname, self.PGOPT['CACT'])
         else:
            msg = "'{}': you must run '{} -{}' as 'root'".format(self.PGLOG['CURUID'], aname, self.PGOPT['CACT'])
         self.pglog(msg, self.PGOPT['extlog'])
      self.PGOPT['UID'] = 0
      self.params['LN'] = self.PGLOG['CURUID']

   def set_email_logact(self):
      """Configure email logging bits based on ``-NE`` and ``-SE`` options.

      Strips all email bits from ``self.PGLOG['LOGMASK']`` when ``-NE`` is set,
      or removes the normal-email bit when ``-SE`` is set.
      """
      if 'NE' in self.params:
         self.PGLOG['LOGMASK'] &= ~self.EMLALL   # remove all email bits
      elif 'SE' in self.params:
         self.PGLOG['LOGMASK'] &= ~self.EMLLOG    # no normal email

   def validate_dsowner(self, aname, dsid=None, logname=None, pgds=0, logact=0):
      """Validate that the current user is an authorised dataset specialist.

      Args:
         aname (str): Application name used in log messages.
         dsid (str, optional): Specific dataset ID to validate against.  When
            ``None``, uses ``self.params['DS']``.
         logname (str, optional): Login name to check.  Defaults to the current
            user.
         pgds (int, optional): Non-zero to allow a DSS group member to proceed
            even without explicit dataset ownership.  Defaults to 0.
         logact (int, optional): Log action bits; defaults to ``self.PGOPT['extlog']``.

      Returns:
         int: ``1`` if valid, ``-1`` if validation could not be performed (no
         dataset in params), or logs a fatal error and exits.
      """
      if not logname: logname = (self.params['LN'] if 'LN' in self.params else self.PGLOG['CURUID'])
      if logname == self.PGLOG['GDEXUSER']: return 1
      dsids = {}
      if dsid:
         dsids[dsid] = 1
      elif 'DS' in self.params:
         if self.OPTS['DS'][0] == 2:
            for dsid in self.params['DS']:
               dsids[dsid] = 1
         else:
            dsids[self.params['DS']] = 1
      else:
         return -1
      if not pgds and 'MD' in self.params: pgds = 1
      if not logact: logact = self.PGOPT['extlog']
      for dsid in dsids:
         if not self.pgget("dsowner", "", "dsid = '{}' AND specialist = '{}'".format(dsid, logname), self.PGOPT['extlog']):
            if not self.pgget("dssgrp", "", "logname = '{}'".format(logname), self.PGOPT['extlog']):
               return self.pglog("'{}' is not DSS Specialist!".format(logname), logact)
            elif not pgds:
               return self.pglog("'{}' not listed as Specialist of '{}'\nRun '{}' with Option -MD!".format(logname, dsid, aname), logact)
      return 1

   def validate_dataset(self):
      """Validate that the dataset(s) in ``self.params['DS']`` exist in RDADB.

      Logs a fatal error for any dataset ID not found in the ``dataset`` table.
      """
      cnt = 1
      if 'DS' in self.params:
         if self.OPTS['DS'][0] == 2:
            for dsid in self.params['DS']:
               cnt = self.pgget("dataset", "", "dsid = '{}'".format(dsid), self.PGOPT['extlog'])
               if cnt == 0: break
         else:
            dsid = self.params['DS']
            cnt = self.pgget("dataset", "", "dsid = '{}'".format(dsid), self.PGOPT['extlog'])
      if not cnt: self.pglog(dsid + " not exists in RDADB!", self.PGOPT['extlog'])

   def validate_groups(self, parent=0):
      """Validate given group indices or group names against RDADB.

      Resolves group names (``-GN`` / ``-PN``) to indices and validates numeric
      indices.  Sets the validated bit on ``self.OPTS[gi][2]`` when done.

      Args:
         parent (int, optional): ``1`` to validate parent-group options (``PI``/``PN``);
            ``0`` for regular group options (``GI``/``GN``).  Defaults to 0.
      """
      if parent:
         gi = 'PI'
         gn = 'PN'
      else:
         gi = 'GI'
         gn = 'GN'
      if (self.OPTS[gi][2]&8): return    # already validated
      dcnd = "dsid = '{}'".format(self.params['DS'])
      if gi in self.params:
         grpcnt = len(self.params[gi])
         i = 0
         while i < grpcnt:
            gidx = self.params[gi][i]
            if not isinstance(gidx, int) and re.match(r'^(!|<|>|<>)$', gidx): break
            i += 1
         if i >= grpcnt:   # normal group index given
            for i in range(grpcnt):
               gidx = self.params[gi][i]
               gidx = int(gidx) if gidx else 0
               self.params[gi][i] = gidx
               if gidx == 0 or (i > 0 and gidx == self.params[gi][i-1]): continue
               if not self.pgget("dsgroup", '', "{} AND gindex = {}".format(dcnd, gidx), self.PGOPT['extlog']):
                  if i > 0 and parent and self.params['GI']:
                     j = 0
                     while j < i:
                        if gidx == self.params['GI'][j]: break
                        j += 1
                     if j < i: continue
                  self.pglog("Group Index {} not in RDADB for {}".format(gidx, self.params['DS']), self.PGOPT['extlog'])
         else:    # found none-equal condition sign
            pgrec = self.pgmget("dsgroup", "DISTINCT gindex", dcnd + self.get_field_condition("gindex", self.params[gi]), self.PGOPT['extlog'])
            grpcnt = (len(pgrec['gindex']) if pgrec else 0)
            if grpcnt == 0:
               self.pglog("No Group matches given Group Index condition for " + self.params['DS'], self.PGOPT['extlog'])
            self.params[gi] = pgrec['gindex']
      elif gn in self.params:
         self.params[gi] = self.group_id_to_index(self.params[gn])
      self.OPTS[gi][2] |= 8  # set validated flag

   def group_id_to_index(self, grpids):
      """Convert an array of group IDs to their corresponding group indices.

      Args:
         grpids (list[str]): Group IDs (or condition strings) to resolve.

      Returns:
         list[int] | None: Resolved group indices, or ``None`` if *grpids* is
         empty.
      """
      count = len(grpids) if grpids else 0
      if count == 0: return None
      indices = []
      dcnd = "dsid = '{}'".format(self.params['DS'])
      i = 0
      while i < count:
         gid = grpids[i]
         if gid and (re.match(r'^(!|<|>|<>)$', gid) or gid.find('%') > -1): break
         i += 1
      if i >= count:   # normal group id given
         for i in range(count):
            gid = grpids[i]
            if not gid:
               indices.append(0)
            elif i and gid == grpids[i-1]:
               indices.append(indices[i-1])
            else:
               pgrec = self.pgget("dsgroup", "gindex", "{} AND grpid = '{}'".format(dcnd, gid), self.PGOPT['extlog'])
               if not pgrec: self.pglog("Group ID {} not in RDADB for {}".format(gid, self.params['DS']), self.PGOPT['extlog'])
               indices.append(pgrec['gindex'])
         return indices
      else: # found wildcard and/or none-equal condition sign
         pgrec = self.pgmget("dsgroup", "DISTINCT gindex", dcnd + self.get_field_condition("grpid", grpids, 1), self.PGOPT['extlog'])
         count = (len(pgrec['gindex']) if pgrec else 0)
         if count == 0: self.pglog("No Group matches given Group ID condition for " + self.params['DS'], self.PGOPT['extlog'])
         return pgrec['gindex']

   def group_index_to_id(self, indices):
      """Convert an array of group indices to their corresponding group IDs.

      Args:
         indices (list[int]): Group indices (or condition strings) to resolve.

      Returns:
         list[str] | None: Resolved group IDs, or ``None`` if *indices* is
         empty.
      """
      count = len(indices) if indices else 0
      if count == 0: return None
      grpids = []
      dcnd = "dsid = '{}'".format(self.params['DS'])
      i = 0
      while i < count:
         gidx = indices[i]
         if not isinstance(gidx, int) and re.match(r'^(!|<|>|<>)$', gidx): break
         i += 1
      if i >= count:   # normal group index given
         for i in range(count):
            gidx = indices[i]
            if not gidx:
               grpids.append('')   # default value
            elif i and gidx == indices[i-1]:
               grpids.append(grpids[i-1])
            else:
               pgrec = self.pgget("dsgroup", "grpid", "{} AND gindex = {}".format(dcnd, gidx), self.PGOPT['extlog'])
               if not pgrec: self.pglog("Group Index {} not in RDADB for {}".format(gidx, self.params['DS']), self.PGOPT['extlog'])
               grpids.append(pgrec['grpid'])
         return grpids
      else:   # found none-equal condition sign
         pgrec = self.pgmget("dsgroup", "DISTINCT grpid", dcnd + self.get_field_condition("gindex", indices), self.PGOPT['extlog'])
         count = (len(pgrec['grpid']) if pgrec else 0)
         if count == 0: self.pglog("No Group matches given Group Index condition for " + self.params['DS'], self.PGOPT['extlog'])
         return pgrec['grpid']

   def append_order_fields(self, oflds, flds, tname, excludes=None):
      """Validate order fields and collect those not already in the field list.

      Args:
         oflds (str): String of field keys to check as order fields.
         flds (str | None): String of field keys already selected; used to skip
            duplicates.
         tname (str): Table name whose ``TBLHASH`` defines valid fields.
         excludes (str, optional): String of field keys to exclude.

      Returns:
         str: Concatenated order field keys that are valid and not duplicated.
      """
      orders = ''
      hash = self.TBLHASH[tname]
      for ofld in oflds:
         ufld = ofld.upper()
         if ufld not in hash or excludes and excludes.find(ufld) > -1: continue
         if flds and flds.find(ufld) > -1: continue
         orders += ofld
      return orders

   def validate_multiple_values(self, tname, count, flds=None):
      """Validate multi-value option counts for given table fields.

      Delegates to ``validate_multiple_options`` with a remove flag set
      appropriately for the ``htarfile`` table.

      Args:
         tname (str): Table name to look up in ``self.TBLHASH``.
         count (int): Expected number of values for each multi-value option.
         flds (list[str], optional): Specific field keys to validate; ``None``
            validates all fields in the table hash.
      """
      opts = []
      hash = self.TBLHASH[tname]
      if flds:
         for fld in flds:
            if fld in hash: opts.append(hash[fld][0])
      else:
         for fld in hash:
            opts.append(hash[fld][0])
      self.validate_multiple_options(count, opts, (1 if tname == 'htarfile' else 0))

   def validate_multiple_options(self, count, opts, remove=0):
      """Validate and normalise value counts for a list of multi-value options.

      Expands single-element options to *count* values when the ``one-for-many``
      flag is set, joins text fields when a single record is expected, and logs
      errors for mismatched counts.

      Args:
         count (int): Required number of values.
         opts (list[str]): Short option keys to validate.
         remove (int, optional): ``1`` to silently remove options with one value
            when *count* > 1 (used for ``htarfile``).  Defaults to 0.
      """
      for opt in opts:
         if opt not in self.params or self.OPTS[opt][0] != 2: continue # no value given or not multiple value option
         cnt = len(self.params[opt])
         if cnt == 1 and count > 1 and self.OPTS[opt][2]&1:
            val0 = self.params[opt][0]
            self.params[opt] = [val0]*count
            self.OPTS[opt][2] |= 4 # expanded
            cnt = count
         if cnt != count:
            if count == 1 and cnt > 1 and self.OPTS[opt][2]&self.PGOPT['TXTBIT']:
               self.params[opt][0] = ' '.join(self.params[opt])
            elif remove and cnt == 1 and count > 1:
               del self.params[opt]
            elif cnt < count:
               self.pglog("Multi-value Option {}({}): {} Given and {} needed".format(opt, self.OPTS[opt][1], cnt, count), self.PGOPT['extlog'])

   def get_field_keys(self, tname, include=None, exclude=None):
      """Get field keys for a RDADB table that have values set in ``self.params``.

      Args:
         tname (str): Table name to look up in ``self.TBLHASH``.
         include (str, optional): Only include fields whose key appears in this
            string.
         exclude (str, optional): Skip fields whose key appears in this string.

      Returns:
         str | None: Concatenated field keys that have param values, or ``None``
         if none found.
      """
      fields = ''
      hash = self.TBLHASH[tname]
      for fld in hash:
         if include and include.find(fld) < 0: continue
         if exclude and exclude.find(fld) > -1: continue
         opt = hash[fld][0]
         if opt in self.params: fields += fld
      return fields if fields else None

   def get_string_fields(self, flds, tname, include=None, exclude=None):
      """Build a comma-separated SQL field string for a RDADB table.

      Args:
         flds (list[str]): Field keys whose SQL expressions are included.
         tname (str): Table name to look up in ``self.TBLHASH``.
         include (str, optional): Only include fields found in this string.
         exclude (str, optional): Skip fields found in this string.

      Returns:
         str: Comma-separated SQL field expressions (e.g. ``"table.col AS alias"``).
      """
      fields = []
      hash = self.TBLHASH[tname]
      for fld in flds:
         ufld = fld.upper()   # in case
         if include and include.find(ufld) < 0: continue
         if exclude and exclude.find(ufld) > -1: continue
         if ufld not in hash:
            self.pglog("Invalid field '{}' to get from '{}'".format(fld, tname), self.PGOPT['extlog'])
         elif hash[ufld][0] not in self.OPTS:
            self.pglog("Option '{}' is not defined for field '{} - {}'".format(hash[ufld][0], ufld, hash[ufld][1]), self.PGOPT['extlog'])
         if len(hash[ufld]) == 4:
            fname = "{} {}".format(hash[ufld][3], hash[ufld][1])
         else:
            fname = hash[ufld][1]
         fields.append(fname)
      return ', '.join(fields)

   def get_max_count(self, opts):
      """Return the maximum value count across a set of multi-value options.

      Args:
         opts (list[str]): Short option keys to inspect.

      Returns:
         int: The largest ``len(self.params[opt])`` among options that are set,
         or ``0`` if none are set.
      """
      count = 0
      for opt in opts:
         if opt not in self.params: continue
         cnt = len(self.params[opt])
         if cnt > count: count = cnt
      return count

   def get_order_string(self, flds, tname, exclude=None):
      """Build an ``ORDER BY`` clause string for a RDADB query.

      Lowercase field letters signal descending order.

      Args:
         flds (str): Field key characters; lowercase means ``DESC``.
         tname (str): Table name to look up in ``self.TBLHASH``.
         exclude (str, optional): Field keys to skip.

      Returns:
         str: ``" ORDER BY ..."`` clause, or ``''`` if no valid fields remain.
      """
      orders = []
      hash = self.TBLHASH[tname]
      for fld in flds:
         if fld.islower():
            desc = " DESC"
            fld = fld.upper()
         else:
            desc = ""
         if exclude and exclude.find(fld) > -1: continue
         orders.append(hash[fld][1] + desc)
      return (" ORDER BY " + ', '.join(orders)) if orders else ''

   def get_string_titles(self, flds, hash, lens):
      """Build a delimiter-separated column title row.

      Args:
         flds (list[str]): Field keys whose titles are included.
         hash (dict): Table hash mapping field keys to option info.
         lens (list[int] | None): Column widths for padding; ``None`` to skip
            padding.

      Returns:
         str: Title row string ending with the delimiter.
      """
      titles = []
      colcnt = len(flds)
      for i in range(colcnt):
         fld = flds[i]
         if fld not in hash: continue
         opt = hash[fld][0]
         if opt not in self.OPTS: self.pglog("ERROR: Undefined option " + opt, self.PGOPT['extlog'])
         title = self.OPTS[opt][1]
         if lens:
            if len(title) > lens[i]: title = opt
            title = "{:{}}".format(title, lens[i])
         titles.append(title)
      return self.params['DV'].join(titles) + self.params['DV']

   def parameter_error(self, p, opt=None, lidx=0, line=0, infile=None):
      """Display a parameter error message and exit.

      Args:
         p (str): The parameter or option short name that caused the error.
         opt (str, optional): Error type keyword or the option short name that
            provides context (``'missval'``, ``'missact'``, ``'duplicate'``,
            ``'specified'``, ``'mixed'``, ``'delayed'``, ``'continue'``, or
            ``None``).
         lidx (int, optional): Line index in the input file (0 if not in a
            file).
         line (str | int, optional): The raw input line text.
         infile (str, optional): Path to the input file.
      """
      if not opt:
         errmsg = "value passed in without leading info option"
      elif opt == "continue":
         errmsg = "error processing input file on continue Line"
      elif opt == 'specified':
         errmsg = "option -{}/-{} is specified already".format(p, self.OPTS[p][1])
      elif opt == "mixed":
         errmsg = "single-value option mixed with multi-value option"
      elif opt == "missact":
         errmsg = "No Action Option is specified"
      elif opt == "missval":
         errmsg = "No value provided following Info Option"
      elif opt == 'duplicate':
         errmsg = "multiple actions not allowed"
      elif opt == "delayed":
         errmsg = "delayed Mode option not supported"
      elif self.OPTS[opt][0] == 0:
         errmsg = "value follows Mode Option -{}/-{}".format(opt, self.OPTS[opt][1])
      elif self.OPTS[opt][0] == 1:
         errmsg = "multiple values follow single-value Option -{}/-{}".format(opt, self.OPTS[opt][1])
      elif self.OPTS[opt][0] >= 4:
         errmsg = "value follows Action Option -{}/-{}".format(opt, self.OPTS[opt][1])
      else:
         errmsg = None
      if errmsg:
         if lidx:
            self.input_error(lidx, line, infile, "{} - {}".format(p, errmsg))
         else:
            self.pglog("ERROR: {} - {}".format(p, errmsg), self.PGOPT['extlog'])

   def input_error(self, lidx, line, infile, errmsg):
      """Log a fatal error for a problem encountered in an input file.

      Args:
         lidx (int): Line number within the input file where the error occurred.
         line (str): The raw line text that caused the error.
         infile (str): Path to the input file.
         errmsg (str): Human-readable description of the error.
      """
      self.pglog("ERROR at {}({}): {}\n  {}".format(infile, lidx, line, errmsg), self.PGOPT['extlog'])

   def action_error(self, errmsg, cact=None):
      """Log a fatal error associated with the current action and dataset.

      Args:
         errmsg (str): Human-readable description of the error.
         cact (str, optional): Short action option name; defaults to
            ``self.PGOPT['CACT']``.
      """
      msg = "ERROR"
      if self.PGOPT['ANAME']: msg += " " + self.PGOPT['ANAME']
      if not cact: cact = self.PGOPT['CACT']
      if cact: msg += " for Action {} ({})".format(cact, self.OPTS[cact][1])
      if 'DS' in self.params:
         if self.OPTS['DS'][0] == 1:
            msg += " of " + self.params['DS']
         elif self.OPTS['DS'][0] == 2 and len(self.params['DS']) == 1:
            msg += " of " + self.params['DS'][0]
      msg += ": " + errmsg
      if self.PGLOG['DSCHECK']: self.record_dscheck_error(msg, self.PGOPT['extlog'])
      self.pglog(msg, self.PGOPT['extlog'])

   def get_option_key(self, p, flag=0, skip=0, lidx=0, line=None, infile=None, table=None):
      """Resolve a parameter string to a valid short option key.

      Checks both short (two-character) and long option names, as well as
      aliases.  Logs an error for unknown or mismatched option types unless
      *skip* is set.

      Args:
         p (str): Option name to look up (with or without leading ``-``).
         flag (int, optional): Constrains the allowed option type:
            ``0`` any, ``1`` value options only, ``2`` multi-value only,
            ``3`` action options only, ``4`` mode/action only.
         skip (int, optional): ``1`` to silently return ``None`` for unknown
            options instead of logging an error.
         lidx (int, optional): Line index for input-file error reporting.
         line (str, optional): Raw line text for input-file error reporting.
         infile (str, optional): Path to the input file.
         table (str, optional): When set, marks matched options in
            ``self.INOPTS``.

      Returns:
         str | None: The short option key, or ``None`` if not found and *skip*
         is ``1``.
      """
      if p is None: p = ''
      opt = self.get_short_option(p)
      errmsg = None
      if opt:
         if flag == 1:
            if self.OPTS[opt][0]&3 == 0: errmsg = "NOT a Value Option"
         elif flag == 2:
            if self.OPTS[opt][0]&2 == 0: errmsg = "NOT a Multi-Value Option"
         elif flag == 3:
            if self.OPTS[opt][0] < 4:
               if lidx:
                  errmsg = "NOT an Action Option"
               else:
                  errmsg = "Miss leading '-' for none action option"
         elif flag == 4:
            if self.OPTS[opt][0]&3:
               errmsg = "NOT a Mode/Action Option"
         if errmsg: errmsg = "{}({}) - {}".format(opt, self.OPTS[opt][1], errmsg)
      elif not skip:
         if p:
           errmsg = "-{} - Unknown Option".format(p)
         else:
           errmsg = "'' - Empty Option Name"
      if errmsg:
         if lidx:
            self.input_error(lidx, line, infile, errmsg)
         else:
            self.pglog("ERROR: " + errmsg, self.PGOPT['extlog'])
      elif opt and (table or self.PGOPT['IFCNT'] and self.OPTS[opt][0] == 2):
         self.INOPTS[opt] = 1
      return opt

   def set_option_value(self, opt, val=None, cnl=0, lidx=0, line=None, infile=None):
      """Set a value for an option, enforcing type constraints and restrictions.

      Handles mode/action, single-value, and multi-value options.  Options
      already set on the command line are not overridden by input-file values.

      Args:
         opt (str): Short option key.
         val (str | int | None, optional): Value to set.  ``None`` is treated
            as ``''``.
         cnl (int, optional): ``1`` when the value is a continuation line
            (multi-line text field).
         lidx (int, optional): Line index for input-file error reporting.
         line (str, optional): Raw line text for input-file error reporting.
         infile (str, optional): Path to the input file.
      """
      if opt in self.CMDOPTS and lidx:   # in input file, but given on command line already
         if opt not in self.params: self.params[opt] = self.CMDOPTS[opt]
         return
      if val is None: val = ''
      if self.OPTS[opt][0]&3:
         if self.OPTS[opt][2]&16:
            if not val:
               val = 0
            elif re.match(r'^\d+$', val):
               val = int(val)
         elif val and (opt == 'DS' or opt == 'OD'):
            val = self.format_dataset_id(val)
      errmsg = None
      if not cnl and self.OPTS[opt][0]&3:
         if opt in self.params:
            if self.OPTS[opt][0] == 2:
               if self.OPTS[opt][2]&2: del self.params[opt]   # clean auto set values
            elif self.params[opt] != val and not self.OPTS[opt][2]&1:
               errmsg = "'{}', multiple values not allowed for Single-Value Option".format(val)
         if not errmsg and (not self.PGOPT['CACT'] or self.OPTS[self.PGOPT['CACT']][2]):
            dstr = self.OPTS[opt][3] if len(self.OPTS[opt]) > 3 else None
            if dstr:
               vlen = len(val)
               ms = re.match(r'^!(\w*)', dstr)
               if ms:
                  dstr = ms.group(1)
                  # Bug fix #3: was `str` (Python built-in), should be `dstr`
                  if vlen == 1 and dstr.find(val) > -1: errmsg = "{}: character must not be one of '{}'".format(val, dstr)
               elif vlen > 1 or (vlen == 0 and not self.OPTS[opt][2]&128) or (vlen == 1 and dstr.find(val) < 0):
                  errmsg = "{} single-letter value must be one of '{}'".format(val, dstr)
      if not errmsg:
         if self.OPTS[opt][0] == 2:    # multiple value option
            if opt not in self.params:
               self.params[opt] = [val]   # set the first value
               if opt == 'QF' and self.PGOPT['ACTS'] == self.OPTS['DL'][0]: self.OPTS['FS'][3] = 'ANT'
            else:
               if cnl:
                  rowidx = len(self.params[opt]) - 1
                  if self.params[opt][rowidx]:
                     if not re.match(r'^(DE|DI|DM|DW)$', opt):
                        errmsg = "Multi-line value not allowed"
                     else:
                        self.params[opt][rowidx] += "\n" + val    # multiple line value
                  else:
                     self.params[opt][rowidx] = val
               else:
                  self.params[opt].append(val)     # add next value
         elif self.OPTS[opt][0] == 1:          # single value option
            if cnl and opt in self.params:
               if val: errmsg = "Multi-line value not allowed"
            elif self.OPTS[opt][2]&2 and self.pgcmp(self.params[opt], val):
               errmsg = "{}: Single-Value Info Option has value '{}' already".format(val, self.params[opt])
            else:
               self.params[opt] = val
               self.OPTS[opt][2] |= 2
         elif val:
            if self.OPTS[opt][0] == 0 and re.match(r'^(Y|N)$', val, re.I):
               self.params[opt] = 1 if (val == 'Y' or val == 'y') else 0
            else:
               self.parameter_error(val, opt, lidx, line, infile)    # no value for flag or action options
         elif opt not in self.params:
            self.params[opt] = 1                # set flag or action option
            if self.OPTS[opt][0] > 2:
               if self.PGOPT['ACTS']: self.parameter_error(opt, "duplicate", lidx ,line, infile)   # no duplicated action options
               self.PGOPT['ACTS'] = self.OPTS[opt][0]  # add action bit
               self.PGOPT['CACT'] = opt  # add action name
               if opt == "SB": self.PGOPT['MSET'] = opt
      if errmsg:
         if lidx:
           self.input_error(lidx, line, infile, "{}({}) - {}".format(opt, self.OPTS[opt][1], errmsg))
         else:
           self.pglog("ERROR: {}({}) - {}".format(opt, self.OPTS[opt][1], errmsg), self.PGOPT['extlog'])
      if not lidx: self.CMDOPTS[opt] = self.params[opt]    # record options set on command lines

   def get_row_width(self, pgrec):
      """Calculate the display width of a single data row.

      Args:
         pgrec (dict): Record dict mapping field names to lists of values.

      Returns:
         int: Total row width in characters, including delimiter separators.
      """
      slen = len(self.params['DV'])
      width = 0
      for key in pgrec:
         wd = 0
         for val in pgrec[key]:
            if not val: continue
            if not isinstance(val, str): val = str(val)
            if key == 'note':
               vlen = val.find('\n') + 1
            else:
               vlen = 0
            if vlen < 1: vlen = len(val)
            if vlen > wd: wd = vlen    # get max width of each column
         # accumulate all column width plus length of delimiter to get row width
         if width: width += slen
         width += wd
      return width

   def get_short_option(self, p):
      """Look up the short (two-character) option key for a given parameter string.

      Searches ``self.OPTS`` first (by exact two-letter match and by long name),
      then ``self.ALIAS``.

      Args:
         p (str): Option name to look up (may be short or long).

      Returns:
         str | None: The matching short option key, or ``None`` if not found.
      """
      plen = len(p)
      if plen == 2:
         p = p.upper()
         if p in self.OPTS: return p
      for opt in self.OPTS:     # get main option first
         if not self.pgcmp(self.OPTS[opt][1], p, 1): return opt
      for opt in self.ALIAS: # then check alias option
         for key in self.ALIAS[opt]:
            if not self.pgcmp(key, p, 1): return opt
      return None

   def print_column_format(self, pgrec, flds, hash, lens, retbuf=0):
      """Print (or return) query results in column format.

      Each row of the result set is written as a delimiter-separated line.

      Args:
         pgrec (dict): Result record dict mapping field names to value lists.
         flds (list[str]): Ordered field keys to include as columns.
         hash (dict): Table hash mapping field keys to option/column info.
         lens (list[int] | None): Per-column widths for fixed-width formatting.
         retbuf (int, optional): ``1`` to return the output as a string instead
            of writing to ``self.OUTPUT``.  Defaults to 0.

      Returns:
         str | int: Accumulated output string when *retbuf* is ``1``, otherwise
         the number of rows written.
      """
      rowcnt = -1
      colcnt = len(flds)
      buf = ''
      fields = []
      flens = []
      for i in range(colcnt):
         fld = flds[i]
         if fld in hash:
            fld = hash[fld][1]
            ms = re.search(r'\.(.+)$', fld)
            if ms: fld = ms.group(1)
            if fld in pgrec:
               fields.append(fld)
               flens.append((lens[i] if lens else 0))
               if rowcnt < 0: rowcnt = len(pgrec[fld])
            else:
               self.pglog(fld + ": Unkown field name", self.PGOPT['extlog'])
      colcnt = len(fields)
      for i in range(rowcnt):
         offset = 0
         values = []
         for j in range(colcnt):
            fld = fields[j]
            idx = -1
            val = pgrec[fld][i]
            slen = flens[j]
            if val is None:
               val = ''
            elif isinstance(val, str):
               idx = val.find("\n")
               if idx > 0:
                  val = "\n" + val
                  idx = 0
            else:
               val = str(val)
            if slen:
               if idx < 0:
                  val = "{:{}}".format(val, slen)
               else:
                  val += "\n{:{}}".format(' ', offset)
               offset += slen
            values.append(val)
         line = self.params['DV'].join(values) + self.params['DV'] + "\n"
         if retbuf:
            buf += line
         else:
            self.OUTPUT.write(line)
      return buf if retbuf else rowcnt

   def print_row_format(self, pgrec, flds, hash):
      """Print query results in row format (one field per line).

      Each field is written as ``"LongName<ES>value\n"``.

      Args:
         pgrec (dict): A single database record dict.
         flds (list[str]): Ordered field keys to print.
         hash (dict): Table hash mapping field keys to option/column info.
      """
      for fld in flds:
         if fld not in hash: continue
         line = "{}{}".format(self.OPTS[hash[fld][0]][1], self.params['ES'])
         field = hash[fld][1]
         ms = re.search(r'\.(.+)$', field)
         if ms: field = ms.group(1)
         if field in pgrec:
            value = pgrec[field]
            if value is not None: line += str(value)
         self.OUTPUT.write(line + "\n")

   def compress_files(self, files, formats, count):
      """Compress or uncompress a list of files, updating format strings.

      Behaviour is controlled by the presence of ``-UZ`` in ``self.params``
      (uncompress) or its absence (compress).

      Args:
         files (list[str]): Paths to local files to process (modified in place).
         formats (list[str]): Corresponding format strings for each file.
         count (int): Number of files to process.

      Returns:
         list[str]: The (possibly updated) *files* list.
      """
      if 'UZ' in self.params:
         strcmp = 'Uncompress'
         actcmp = 0
      else:
         strcmp = 'Compress'
         actcmp = 1
      fmtcnt = len(formats)
      if not fmtcnt: return files   # just in case
      s = 's' if count > 1 else ''
      self.pglog("{}ing {} File{} for {} ...".format(strcmp, count, s, self.params['DS']), self.PGOPT['wrnlog'])
      cmpcnt = 0
      for i in range(count):
         fmt = formats[i] if(i < fmtcnt and formats[i]) else formats[0]
         (ofile, fmt) = self.compress_local_file(files[i], fmt, actcmp, self.PGOPT['extlog'])
         if ofile != files[i]:
            files[i] = ofile
            cmpcnt += 1
      self.pglog("{}/{} Files {}ed for {}".format(cmpcnt, count, strcmp, self.params['DS']) , self.PGOPT['emllog'])
      if 'ZD' in self.params: del self.params['ZD']
      if 'UZ' in self.params: del self.params['UZ']
      return files

   def get_hash_condition(self, tname, include=None, exclude=None, noand=0):
      """Build a SQL ``WHERE`` condition string from table hash and current params.

      Args:
         tname (str): Table name to look up in ``self.TBLHASH``.
         include (str, optional): Only include fields whose key appears here.
         exclude (str, optional): Skip fields whose key appears here.
         noand (int, optional): ``1`` to omit a leading ``AND`` on the first
            condition fragment.  Defaults to 0.

      Returns:
         str: Concatenated SQL condition fragments (e.g. ``" AND col = 'val'"``).
      """
      condition = ''
      hash = self.TBLHASH[tname]
      for key in hash:
         if include and include.find(key) < 0: continue
         if exclude and exclude.find(key) > -1: continue
         opt = hash[key][0]
         if opt not in self.params: continue    # no option value
         flg = hash[key][2]
         if flg < 0: # condition is ignore for this option
            self.pglog("Condition given per Option -{} (-{}) is ignored".format(opt, self.OPTS[opt][1]), self.PGOPT['errlog'])
            continue
         fld = hash[key][1]
         condition += self.get_field_condition(fld, self.params[opt], flg, noand)
         noand = 0
      return condition

   def set_default_value(self, opt, dval=None):
      """Set a default parameter value for an option if none is currently set.

      Removes the option from ``self.params`` if the current value is ``'all'``
      (case-insensitive).

      Args:
         opt (str): Short option key.
         dval (str | None, optional): Default value to apply when the option
            has no value yet.  ``None`` means no default is applied.
      """
      flag = self.OPTS[opt][0]
      if flag&3 == 0: return    # skip if not single&multiple value options
      oval = 0
      if opt in self.params:
         if flag == 1:
            oval = self.params[opt]
         else:
            count = len(self.params[opt])
            if count == 1:
               oval = self.params[opt][0]
            elif count > 1:
               return             # multiple values given already
      if oval:
         if re.match(r'^all$', oval, re.I):
            del self.params[opt]       # remove option value for all
         return                   # value given already
      if dval:
         # set default value
         if flag == 1:
            self.params[opt] = dval
         else:
            self.params[opt] = [dval]

   def cos_convert(self, locfile, cosflg, cosfile=None):
      """Add or strip a COS block for a local file using ``cosconvert``.

      Args:
         locfile (str): Path to the local file to convert.
         cosflg (str): Conversion flag passed to ``cosconvert``.
         cosfile (str, optional): Destination path for the converted file.  If
            ``None``, conversion is done in place on *locfile*.

      Returns:
         int | None: File size in bytes after conversion, or the result of a
         logged error on failure.
      """
      if cosfile:
         cmd = "cosconvert -{} {} {}".format(cosflg, cosfile, locfile)
      else:
         cmd = "cosconvert -{} {}".format(cosflg.lower(), locfile)
         cosfile = locfile
      self.pgsystem(cmd)
      info = self.check_local_file(cosfile)
      if not info:
         return self.pglog("Error - " + cmd, self.PGOPT['errlog'])   # should not happen
      else:
         return info['data_size']

   def get_option_count(self, opts):
      """Evaluate the count of values for a set of options and validate them.

      Args:
         opts (list[str]): Short option keys to inspect.

      Returns:
         int: The maximum value count across all provided options that are set
         in ``self.params``.
      """
      count = 0
      for opt in opts:
         if opt in self.params:
            cnt = len(self.params[opt])
            if cnt > count: count = cnt
      if count > 0: self.validate_multiple_options(count, opts)
      return count

   def get_all_subgroups(self, dcnd, pidx, gtype=None):
      """Gather all subgroup indices recursively for a given parent index.

      Args:
         dcnd (str): SQL condition string scoping the dataset (e.g.
            ``"dsid = 'd123456'"``).
         pidx (int): Parent group index to start from.
         gtype (str, optional): Group type filter (e.g. ``'P'`` for public
            groups only).  ``None`` includes all types.

      Returns:
         list[int]: All group indices reachable from *pidx*, including *pidx*
         itself.
      """
      gidxs = [pidx]
      gflds = "gindex"
      if gtype: gflds += ", grptype"
      grecs = self.pgmget("dsgroup", gflds, "{} and pindex = {}".format(dcnd, pidx), self.LGWNEX)
      if not grecs: return gidxs
      gcnt = len(grecs['gindex'])
      for i in range(gcnt):
         gidx = grecs['gindex'][i]
         if abs(gidx) <= abs(pidx) or gtype and grecs['grptype'][i] != gtype: continue
         subs = self.get_all_subgroups(dcnd, gidx, gtype)
         gidxs.extend(subs)
      return gidxs

   def get_data_subgroups(self, dcnd, pidx, cfld, pfcnt=0):
      """Gather public subgroup indices that directly contain data files.

      Only includes a group index when its count field (e.g. ``dwebcnt``) is
      greater than zero.  The parent *pidx* is included when it has files not
      accounted for by its children.

      Args:
         dcnd (str): SQL condition string scoping the dataset.
         pidx (int): Parent group index to start from.
         cfld (str): Count field name (e.g. ``'dwebcnt'``, ``'nwebcnt'``).
         pfcnt (int, optional): Pre-fetched file count for *pidx*; ``0`` to
            query the database.

      Returns:
         list[int] | None: Group indices with data files, or ``None`` if none
         found.
      """
      if not pfcnt:    # get file count for the parent group
         pfcnt = self.group_file_count(dcnd, pidx, cfld)
         if not pfcnt: return None
      gflds = "gindex, " + cfld
      gcnd = "{} AND pindex = {} AND {} > 0".format(dcnd, pidx, cfld)
      grecs = self.pgmget("dsgroup", gflds, gcnd, self.LGWNEX)
      if not grecs: return ([pidx] if pfcnt > 0 else None)
      gcnt = len(grecs['gindex'])
      gidxs = []
      for i in range(gcnt):
         gidx = grecs['gindex'][i]
         fcnt = grecs[cfld][i]
         if fcnt == 0 or abs(gidx) <= abs(pidx): continue
         subs = self.get_data_subgroups(dcnd, gidx, cfld, fcnt)
         if subs: gidxs.extend(subs)
         pfcnt -= fcnt
      if pfcnt > 0: gidxs.insert(0, pidx)
      return (gidxs if gidxs else None)

   def group_file_count(self, cnd, gidx, cfld):
      """Get the file count for a group from ``dsgroup`` or ``dataset``.

      Args:
         cnd (str): SQL condition string (dataset scope).
         gidx (int): Group index; ``0`` queries the ``dataset`` table instead
            of ``dsgroup``.
         cfld (str): Count field name to retrieve.

      Returns:
         int: The file count, or ``0`` if the record is not found.
      """
      if gidx:
         table = "dsgroup"
         cnd += " AND gindex = {}".format(gidx)
      else:
         table = "dataset"
      pgrec = self.pgget(table, cfld, cnd)
      return (pgrec[cfld] if pgrec else 0)

   def set_file_format(self, count):
      """Detect and set the archive format for local files in ``self.params['LF']``.

      Populates ``self.params['AF']`` with detected formats and marks the option
      as auto-set.

      Args:
         count (int): Number of files to inspect (matches the length of ``LF``).
      """
      if 'LF' in self.params:
         files = self.params['LF']
      else:
         return
      fmtcnt = 0
      fmts = [None] * count
      for i in range(count):
         fmt = self.get_file_format(files[i])
         if fmt:
            fmtcnt += 1
            fmts[i] = fmt
      if fmtcnt:
         self.params['AF'] = fmts
         self.OPTS['AF'][2] |= 2

   @staticmethod
   def get_control_frequency(frequency):
      """Parse a frequency string into a frequency list and unit character.

      Accepts formats like ``'1Y'``, ``'3M'``, ``'2W'``, ``'7D'``, ``'12H'``,
      ``'30N'``, ``'60S'``, and fractional-month forms ``'1M/2'`` through
      ``'1M/10'`` (denominators 2, 3, 5, 6, 10 only).

      Args:
         frequency (str): Frequency string to parse.

      Returns:
         tuple: ``(freq, unit)`` where *freq* is a 7-element list
         ``[years, months, days, hours, minutes, seconds, fractions]`` and
         *unit* is the uppercase unit character (``'Y'``, ``'M'``, ``'W'``,
         ``'D'``, ``'H'``, ``'N'``, or ``'S'``).  On error, returns
         ``(None, errmsg)`` where *errmsg* describes the problem.
      """
      val = nf = 0
      unit = None
      ms = re.match(r'^(\d+)([YMWDHNS])$', frequency, re.I)
      if ms:
         val = int(ms.group(1))
         unit = ms.group(2).upper()
      else:
         ms = re.match(r'^(\d+)M/(\d+)', frequency, re.I)
         if ms:
            val = int(ms.group(1))
            nf = int(ms.group(2))
            unit = 'M'
            if nf < 2 or nf > 10 or (30%nf): val = 0
      if not val:
         # Bug fix #5: normalise all error paths to return (None, errmsg)
         if nf:
            errmsg = "fraction of month frequency '{}' MUST be (2,3,5,6,10)".format(frequency)
         elif unit:
            errmsg = "frequency '{}' MUST be larger than 0".format(frequency)
         elif re.search(r'/(\d+)$', frequency):
            errmsg = "fractional frequency '{}' for month ONLY".format(frequency)
         else:
            errmsg = "invalid frequency '{}', unit must be (Y,M,W,D,H,N,S)".format(frequency)
         return (None, errmsg)
      freq = [0]*7   # initialize the frequency list
      uidx = {'Y': 0, 'D': 2, 'H': 3, 'N': 4, 'S': 5}
      if unit == 'M':
         freq[1] = val
         if nf: freq[6] = nf     # number of fractions in a month
      elif unit == 'W':
         freq[2] = 7 * val
      elif unit in uidx:
         freq[uidx[unit]] = val
      return (freq, unit)

   def valid_data_time(self, pgrec, cstr=None, logact=0):
      """Check whether a control record's data time is valid for its parent index.

      Args:
         pgrec (dict): Control record containing ``'pindex'``, ``'datatime'``,
            and ``'frequency'`` fields.
         cstr (str, optional): Context string prepended to log messages.
         logact (int, optional): Log action bits for warning messages.

      Returns:
         int: ``self.SUCCESS`` if valid or no check needed; ``self.FAILURE``
         otherwise.
      """
      if pgrec['pindex'] and pgrec['datatime']:
         (freq, unit) = self.get_control_frequency(pgrec['frequency'])
         if not freq:
            if cstr: self.pglog("{}: {}".format(cstr, unit), logact)
            return self.FAILURE
         dtime = self.adddatetime(pgrec['datatime'], freq[0], freq[1], freq[2], freq[3], freq[4], freq[5], freq[6])
         if self.pgget("dcupdt", "", "cindex = {} AND datatime < '{}'".format(pgrec['pindex'], dtime), self.PGOPT['extlog']):
            if cstr: self.pglog("{}: MUST be processed After Control Index {}".format(cstr, pgrec['pindex']), logact)
            return self.FAILURE
      return self.SUCCESS

   def publish_dataset_filelist(self, dsids):
      """Publish file lists for a collection of datasets.

      Args:
         dsids (list[str]): Dataset IDs whose file lists should be published.
      """
      for dsid in dsids:
         self.pgsystem("publish_filelist " + dsid, self.PGOPT['wrnlog'], 7)

   def get_version_index(self, dsid, logact=0):
      """Get the current active version index for a dataset.

      Args:
         dsid (str): Dataset ID to query.
         logact (int, optional): Log action bits for error messages.

      Returns:
         int: The active version index, or ``0`` if none found.
      """
      pgrec = self.pgget("dsvrsn", "vindex", "dsid = '{}' AND status = 'A'".format(dsid), logact)
      return (pgrec['vindex'] if pgrec else 0)

   @staticmethod
   def append_format_string(sformat, sfmt, chkend=0):
      """Append format components to an existing format string without duplicates.

      Args:
         sformat (str | None): Existing format string (dot-separated components).
         sfmt (str | None): Format components to append (dot-separated).
         chkend (int, optional): ``1`` to only check for a match at the end of
            *sformat*; ``0`` (default) to check anywhere.

      Returns:
         str: Updated format string with new components appended.
      """
      mp = r'(^|\.){}$' if chkend else r'(^|\.){}(\.|$)'
      if sfmt:
         if not sformat:
            sformat = sfmt
         else:
            for fmt in re.split(r'\.', sfmt):
               # Bug fix #6: escape regex metacharacters in format component
               if not re.search(mp.format(re.escape(fmt)), sformat, re.I): sformat += '.' + fmt
      return sformat

   @staticmethod
   def request_type(rtype, idx=0):
      """Return a descriptive string or attribute for a request type code.

      Args:
         rtype (str): Single-character request type code (e.g. ``'C'``, ``'S'``).
         idx (int, optional): ``0`` for the type description string (default);
            ``1`` for the associated integer attribute.

      Returns:
         str | int: The requested attribute from the internal ``RTYPE`` table.
         Falls back to type ``'U'`` (``"Data"``) for unknown codes.
      """
      RTYPE = {
         'C': ["Customized Data",                0],
         'D': ["CDP Link",                       0],
         'M': ["Delayed Mode Data",              1],
         'N': ["NCARDAP(THREDDS) Data Server",   0],
         'Q': ["Database Query",                 0],
         'R': ["Realtime Data",                  0],
         'S': ["Subset Data",                    0],
         'T': ["Subset/Format-Conversion Data",  0],
         'F': ["Format Conversion Data",         1],  # web
         'A': ["Archive Format Conversion",      1],  # web
         'P': ["Plot Chart",                     0],
         'U': ["Data",                           0]
      }
      if rtype not in RTYPE: rtype = 'U'
      return RTYPE[rtype][idx]

   def send_request_email_notice(self, pgrqst, errmsg, fcount, rstat, readyfile=None, pgpart=None):
      """Compose and send (or cache) a request email notice to the user.

      Handles error, empty-output, and success scenarios, building the email
      body from a template notice file and substituting placeholders.

      Args:
         pgrqst (dict): Database record from ``dsrqst`` with request details.
         errmsg (str | None): Error description, or ``None`` on success.
         fcount (int): Number of output files generated.
         rstat (str): Current request status character.
         readyfile (str, optional): Path to write the email buffer as a ready
            file.
         pgpart (dict, optional): Partition record when processing partitions.

      Returns:
         str: Updated request status character (e.g. ``'E'`` on email error).
      """
      pgcntl = self.PGOPT['RCNTL']
      rhome = self.params['WH'] if 'WH' in self.params and self.params['WH'] else self.PGLOG['RQSTHOME']
      if errmsg:
         if pgpart:
            if self.cache_partition_email_error(pgpart['rindex'], errmsg): return rstat
            enote = "email_part_error"
         else:
            enote = "email_error"
      elif fcount == 0:
         if pgcntl and pgcntl['empty_out'] == 'Y':
            enote = "email_empty"
         else:
            errmsg = "NO output data generated"
            if pgpart:
               if self.cache_partition_email_error(pgpart['rindex'], errmsg): return rstat
               enote = "email_part_error"
            else:
               enote = "email_error"
      elif 'EN' in self.params and self.params['EN'][0]:
         enote = self.params['EN'][0]
      elif pgrqst['enotice']:
         enote = pgrqst['enotice']
      elif pgcntl and pgcntl['enotice']:
         enote = pgcntl['enotice']
      elif pgrqst['globus_transfer'] == 'Y' and pgrqst['task_id']:
         enote = "email_notice_globus"
      else:
         enote = "email_" + ("command" if pgrqst['location'] else "notice")
      if enote[0] not in '/.': enote = "{}/notices/{}".format(rhome, enote)
      finfo = self.check_local_file(enote, 128)
      if not finfo:
         if finfo is None:
            ferror = "file not exists"
         else:
            ferror = "Error check file"
      else:
         ef = open(enote, 'r')  # open email notice file
         ferror = None
      if ferror:
         if errmsg:
            self.pglog("{}: {}\nCannot email error to {}@ucar.edu: {}".format(enote, ferror, self.PGLOG['CURUID'], errmsg),
                       (self.PGOPT['errlog'] if rstat else self.PGOPT['extlog']))
            return "E"
         else:
            errmsg = self.pglog("{}: {}\nCannot email notice to {}".format(enote, ferror, pgrqst['email']), self.PGOPT['errlog']|self.RETMSG)
            enote = rhome + "/notices/email_error"
            ef = open(enote, 'r')
            rstat = 'E'
      ebuf = ''
      ebuf += ef.read()
      ef.close()
      einfo = {}
      einfo['HOSTNAME'] = self.PGLOG['HOSTNAME']
      einfo['DSID'] = pgrqst['dsid']
      einfo['DSSURL'] = self.PGLOG['DSSURL']
      if pgrqst['location']:
         einfo['WHOME'] = pgrqst['location']
      else:
         einfo['WHOME'] = self.PGLOG['RQSTURL']
      einfo['SENDER'] = pgrqst['specialist'] + "@ucar.edu"
      einfo['RECEIVER'] = pgrqst['email']
      einfo['RTYPE'] = self.request_type(pgrqst['rqsttype'])
      self.add_carbon_copy() # clean carbon copy email in case not empty
      exclude = (einfo['SENDER'] if errmsg else einfo['RECEIVER'])
      if not errmsg and pgcntl and pgcntl['ccemail']:
         self.add_carbon_copy(pgcntl['ccemail'], 1, exclude, pgrqst['specialist'])
      if self.PGLOG['CURUID'] != pgrqst['specialist'] and self.PGLOG['CURUID'] != self.PGLOG['GDEXUSER']:
         self.add_carbon_copy(self.PGLOG['CURUID'], 1, exclude)
      if 'CC' in self.params: self.add_carbon_copy(self.params['CC'], 0, exclude)
      einfo['CCD'] = self.PGLOG['CCDADDR']
      einfo['RINDEX'] = str(pgrqst['rindex'])
      einfo['RQSTID'] = pgrqst['rqstid']
      pgrec = self.pgget("dataset", "title", "dsid = '{}'".format(pgrqst['dsid']), self.PGOPT['extlog'])
      einfo['DSTITLE'] = pgrec['title'] if pgrec and pgrec['title'] else ''
      einfo['SUBJECT'] = ''
      if errmsg:
         einfo['ERRMSG'] = self.get_error_command(int(time.time()), self.PGOPT['errlog']) + errmsg
         einfo['SUBJECT'] = "Error "
         if pgpart:
            einfo['PARTITION'] = " partition"
            einfo['PTIDX'] = "(PTIDX{})".format(pgpart['pindex'])
            einfo['SUBJECT'] += "Process Partitions of "
         else:
            einfo['PARTITION'] = einfo['PTIDX'] = ''
            einfo['SUBJECT'] += "Build "
         einfo['SUBJECT'] +=  "{} Rqst{} from {}".format(einfo['RTYPE'], pgrqst['rindex'], pgrqst['dsid'])
      else:
         if fcount == 0:
            einfo['SUBJECT'] += "NO Output:"
         else:
            einfo['SUBJECT'] += "Completed:"
            einfo['DAYS'] = str(self.PGOPT['VP'])
         pgrec = self.pgget("dssgrp", "lstname, fstname, phoneno",
                            "logname = '{}'".format(self.PGLOG['CURUID']), self.PGOPT['extlog'])
         if pgrec:
            einfo['SPECIALIST'] = "{} {}".format(pgrec['fstname'], pgrec['lstname'])
            einfo['PHONENO'] = pgrec['phoneno']
         einfo['SUBJECT'] += f" {pgrqst['dsid']} {einfo['RTYPE']} request {pgrqst['rindex']}"
      if pgrqst['note']:
         einfo['RNOTE'] = "\nRequest Detail:\n{}\n".format(pgrqst['note'])
      elif fcount > 0 and pgrqst['rinfo']:
         einfo['RNOTE'] = "\nRequest Detail:\n{}\n".format(pgrqst['rinfo'])
      else:
         einfo['RNOTE'] = ""
      if pgrqst['globus_transfer'] == 'Y' and pgrqst['task_id']:
         einfo['GLOBUS_TASK_URL'] = "https://app.globus.org/activity/" + pgrqst['task_id']
      for ekey in einfo:
         if ekey == 'CCD' and not einfo['CCD']:
            mp = r'Cc:\s*<CCD>\s*'
            rep = ''
         else:
            mp = r'<{}>'.format(ekey)
            rep = einfo[ekey]
            if rep is None:
               self.pglog("{}.{}: None ekey value for reuqest email".format(pgrqst['rindex'], ekey),
                           self.PGOPT['wrnlog']|self.FRCLOG)
               rep = ''
         ebuf = re.sub(mp, rep, ebuf)
      if self.PGLOG['DSCHECK'] and not pgpart:
         tbl = "dscheck"
         cnd = "cindex = {}".format(self.PGLOG['DSCHECK']['cindex'])
      else:
         tbl = "dsrqst"
         cnd = "rindex = {}".format(pgrqst['rindex'])
      if self.send_customized_email(f"{tbl}.{cnd}", ebuf, 0):
         if errmsg:
            self.pglog("Error Email sent to {} for {}.{}:\n{}".format(einfo['SENDER'], tbl, cnd, errmsg), self.PGOPT['errlog'])
            readyfile = None
         else:
            self.pglog("{}Email sent to {} for {}.{}\nSubset: {}".format(("Customized " if pgrqst['enotice'] else ""), einfo['RECEIVER'], tbl, cnd, einfo['SUBJECT']),
                        self.PGOPT['wrnlog']|self.FRCLOG)
      else:
         if not self.cache_customized_email(tbl, "einfo", cnd, ebuf, 0): return 'E'
         if errmsg:
            self.pglog("Error Email {} cached to {}.einfo for {}:\n{}".format(einfo['SENDER'], tbl, cnd, errmsg), self.PGOPT['errlog'])
            readyfile = None
         else:
            self.pglog("{}Email {} cached to {}.einfo for {}\nSubset: {}".format(("Customized " if pgrqst['enotice'] else ""), einfo['RECEIVER'], tbl, cnd, einfo['SUBJECT']),
                       self.PGOPT['wrnlog']|self.FRCLOG)
      if readyfile:
         rf = open(readyfile, 'w')
         rf.write(ebuf)
         rf.close()
         self.set_local_mode(readyfile, 1, self.PGLOG['FILEMODE'])
      return rstat

   def cache_partition_email_error(self, ridx, errmsg):
      """Cache a partition processing error into the existing email buffer.

      Updates ``dsrqst.einfo`` by substituting the ``<PARTERR>`` placeholder
      with the error message and a new placeholder for the next error.

      Args:
         ridx (int): Request index identifying the ``dsrqst`` row.
         errmsg (str): Error description to embed in the email buffer.

      Returns:
         int: Non-zero if the update succeeded; ``0`` if no placeholder was
         found in the existing email buffer.
      """
      pkey = "<PARTERR>"
      pgrec = self.pgget("dsrqst", 'einfo', "rindex = {}".format(ridx), self.PGOPT['extlog'])
      if not (pgrec and pgrec['einfo'] and pgrec['einfo'].find(pkey) > -1): return 0
      errmsg = self.get_error_command(int(time.time()), self.PGOPT['errlog']) + ("{}\n{}".format(errmsg, pkey))
      pgrec['einfo'] = re.sub(pkey, errmsg, pgrec['einfo'])
      return self.pgupdt("dsrqst", pgrec, "rindex = {}".format(ridx), self.PGOPT['extlog'])
