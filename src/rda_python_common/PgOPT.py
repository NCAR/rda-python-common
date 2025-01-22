#
###############################################################################
#
#     Title : PgOPT.py
#
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 08/26/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module for holding global varaibles
#             functions for processing options and other global functions
#
#    Github : https://github.com/NCAR/rda-pyhon-common.git
# 
###############################################################################
#
import os
import sys
import re
import time
from os import path as op
from . import PgLOG
from . import PgUtil
from . import PgFile
from . import PgDBI

OUTPUT = None
CMDOPTS = {}
INOPTS = {}

# global variables are used by all applications and this package.
# they need be initialized in application specified packages
ALIAS = {}
TBLHASH = {}

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
# >0 - setions
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
# The fourth hash values defined retrictions for single letter values
###############################################################################
OPTS = {}

# global initial optional values
PGOPT = {
   'ACTS'   : 0,    # carry current action bits
   'UACTS'  : 0,    # carry dsarch skip check UD action bits
   'CACT'   : '',   # current short action name
   'IFCNT'  : 0,    # 1 to read a single Input File at a time
   'ANAME'  : '',   # cache the application name if set
   'TABLE'  : '',   # table name the action is on
   'UID'    : 0,    # user.uid
   'MSET'   : 'SA', # Action for multiple sets
   'WIDTH'  : 128,  # max column width
   'TXTBIT' : 64,   # text field bit (0x1000) allow multiple lines
   'PEMAX'  : 12,   # max count of reuqest partition errors for auto reprocesses
   'REMAX'  : 2,    # max count of reuqest errors for auto reprocesses
   'RSMAX'  : 2000,  # max count of gatherxml with options -R -S
   'RCNTL'  : None,  # placehold for a request control record
   'dcm'  : "dcm",
   'sdp'  : "sdp",
   'rcm'  : "rcm",
   'scm'  : "scm",
   'wpg'  : "",
   'gatherxml'  : "gatherxml",
   'cosconvert' : "cosconvert",
   'emllog' : PgLOG.LGWNEM,
   'emlerr' : PgLOG.LOGERR|PgLOG.EMEROL,
   'emerol' : PgLOG.LOGWRN|PgLOG.EMEROL,
   'emlsum' : PgLOG.LOGWRN|PgLOG.EMLSUM,
   'emlsep' : PgLOG.LGWNEM|PgLOG.SEPLIN,
   'wrnlog' : PgLOG.LOGWRN,
   'errlog' : PgLOG.LOGERR,
   'extlog' : PgLOG.LGEREX,
   'PTYPE'  : "CPRV",
   'WDTYP'  : "ADNU",
   'HFTYP'  : "DS",
   'SDTYP'  : "PORWUV",
   'GXTYP'  : "DP"
}

# global default parameters
params = {
   'ES' : "<=>",
   'AO' : "<!>",
   'DV' : "<:>"
}

WTYPE = {
   'A' : "ARCO",
   'D' : "DATA",
   'N' : "NCAR",
   'U' : "UNKNOWN",
}

HTYPE = {
   'D' : "DOCUMENT",
   'S' : "SOFTWARE",
   'U' : "UNKNOWN"
}

HPATH = {
   'D' : "docs",
   'S' : "software",
   'U' : "help"
}

MTYPE = {
   'P' : "PRIMARY",
   'A' : "ARCHIVING",
   'V' : "VERSION",
   'W' : "WORKING",
   'R' : "ORIGINAL",
   'B' : "BACKUP",
   'O' : "OFFSITE",
   'C' : "CHRONOPOLIS",
   'U' : "UNKNOWN"
}

STYPE = {
   'O' : "OFFLINE",
   'P' : "PRIMARY",
   'R' : "ORIGINAL",
   'V' : "VERSION",
   'W' : "WORKING",
   'U' : "UNKNOWN"
}

BTYPE = {
   'B' : "BACKUPONLY",
   'D' : "BACKDRDATA",
}

#
# process and parsing input information
# aname - application name such as 'dsarch', 'dsupdt', and 'dsrqst'
#
def parsing_input(aname):

   PgLOG.PGLOG['LOGFILE'] = aname + ".log"
   PGOPT['ANAME'] = aname
   PgDBI.dssdb_dbname()
   argv = sys.argv[1:]
   if not argv: PgLOG.show_usage(aname)

   PgLOG.cmdlog("{} {}".format(aname, ' '.join(argv)))

   # process command line options to fill option values
   option = infile = None
   needhelp = 0
   helpopts = {}
   for param in argv:
      if re.match(r'^(-{0,2}help|-H)$', param, re.I):
         if option: helpopts[option] = OPTS[option]
         needhelp = 1
         continue

      ms = re.match(r'^-([a-zA-Z]\w*)$', param)
      if ms:     # option parameter
         param = ms.group(1)
         if option and not needhelp and option not in params:
            val = get_default_info(option)
            if val is not None:
               set_option_value(option, val)
            else:
               parameter_error("-" + option, "missval")
         option = get_option_key(param)
         if needhelp:
            helpopts[option] = OPTS[option]
            break

         # set mode/action options
         if OPTS[option][0]&3 == 0: set_option_value(option)

      elif option:
         ms =re.match(r"^\'(.*)\'$", param)
         if ms: param = ms.group(1)
         set_option_value(option, param)

      elif PgUtil.find_dataset_id(param):
         set_option_value('DS', param)

      else:
         option = get_option_key(param, 3, 1)
         if option:
            set_option_value(option)
            if needhelp:
               helpopts[option] = OPTS[option]
               break
         elif op.exists(param): # assume input file
            infile = param
         else:
            parameter_error(param)

   if needhelp: PgLOG.show_usage(aname, helpopts)

   if option and option not in params:
      val = get_default_info(option)
      if val is not None:
         set_option_value(option, val)
      else:
         parameter_error("-" + option, "missval")

   # check if only an input filename is given on command line following aname
   if infile:
      if 'IF' in params:
         parameter_error(infile)
      else:
        params['IF'] = [infile]

   # process given one or multiple input files to fill option values
   if 'IF' in params:
      PGOPT['IFCNT'] = 1 if PGOPT['CACT'] == 'AQ' else 0
      if OPTS['DS'][0] == 1:
         param = validate_infile_names(params['DS']) if 'DS' in params else 0
      else:
         param = 1
      get_input_info(params['IF'])
      if not param and 'DS' in params: validate_infile_names(params['DS'])

   if not PGOPT['ACTS']: parameter_error(aname, "missact")   # no action enter

   if 'DB' in params:
      dcnt = len(params['DB'])
      for i in range(dcnt):
         if i == 0:
            PgLOG.PGLOG['DBGLEVEL'] = params['DB'][0]
         elif i == 1:
            PgLOG.PGLOG['DBGPATH'] = params['DB'][1]
         elif i == 2:
            PgLOG.PGLOG['DBGFILE'] = params['DB'][2]
      PgLOG.pgdbg(PgLOG.PGLOG['DBGLEVEL'])

   if 'GZ' in params: PgLOG.PGLOG['GMTZ'] = PgUtil.diffgmthour()
   if 'BG' in params: PgLOG.PGLOG['BCKGRND'] = 1

#
# check and get default value for info option, return None if not available
#
def get_default_info(opt):

   olist = OPTS[opt]
   if olist[0]&3 and len(olist) > 3:
      odval = olist[3]
      if not odval or isinstance(odval, int):
         return odval
      else:
         return odval[0]   # return the first char of a default string

   return None

#
# set output file name handler now
#
def open_output(outfile = None):

   global OUTPUT

   if outfile:  # result output file
      try:
         OUTPUT = open(outfile, 'w')
      except Exception as e:
         PgLOG.pglog("{}: Error open file to write - {}".format(outfile, str(e)), PGOPT['extlog'])
   else:                               # result to STDOUT
      OUTPUT = sys.stdout

#
# return 1 if valid infile names; sys.exit(1) otherwise
#
def validate_infile_names(dsid):

   i = 0
   for infile in params['IF']:
      if not validate_one_infile(infile, dsid): return PgLOG.FAILURE
      i += 1
      if PGOPT['IFCNT'] and i >= PGOPT['IFCNT']: break

   return i

#
# validate an input filename against dsid
#
def validate_one_infile(infile, dsid):

   ndsid = PgUtil.find_dataset_id(infile)
   if ndsid == None:
      return PgLOG.pglog("{}: No dsid identified in Input file name {}!".format(dsid, infile), PGOPT['extlog'])

   fdsid = PgUtil.format_dataset_id(ndsid)
   if fdsid != dsid:
      return PgLOG.pglog("{}: Different dsid {} found in Input file name {}!".format(dsid, fdsid, infile), PGOPT['extlog'])

   return PgLOG.SUCCESS

#
# gather input information from input files
#
def get_input_info(infiles, table = None):

   i = 0
   for file in infiles:
      i += process_infile(file, table)
      if not PGOPT['IFCNT'] and PGOPT['CACT'] == 'AQ': PGOPT['IFCNT'] = 1
      if PGOPT['IFCNT']: break

   return i

#
# validate and get info from a single input file
#
def read_one_infile(infile):

   dsid = params['DS']
   del params['DS']
   if OPTS['DS'][2]&2: OPTS['DS'][2] &= ~2
   if 'DS' in CMDOPTS: del CMDOPTS['DS']
   clean_input_values()
   process_infile(infile)
   if 'DS' in params: dsid = params['DS']
   if dsid: validate_one_infile(infile, dsid)

   return dsid

#
# gather input option values from one input file
#
# return 0 if nothing retireved if table is not null
#
def process_infile(infile, table = None):

   if not op.exists(infile): PgLOG.pglog(infile + ": Input file not exists", PGOPT['extlog'])
   if table:
      PgLOG.pglog("Gather '{}' information from input file '{}'..." .format(table, infile), PGOPT['wrnlog'])
   else:
      PgLOG.pglog("Gather information from input file '{}'...".format(infile), PGOPT['wrnlog'])
   
   try:
      fd = open(infile, 'r')
   except Exception as e:
      PgLOG.pglog("{}: Error Open input file - {}!".format(infile, str(e)), PGOPT['extlog'])
   else:
      lines = fd.readlines()
      fd.close()

   opt = None
   columns = []
   chktbl = 1 if table else -1
   mpes = r'^(\w+)\s*{}\s*(.*)$'.format(params['ES'])
   mpao = r'^(\w+)\s*{}'.format(params['AO'])
   # column count, column index, value count, value index, line index, option-set count, end divider flag
   colcnt = colidx = valcnt = validx = linidx = setcnt = enddiv = 0
   for line in lines:
      linidx += 1
      if linidx%50000 == 0:
         PgLOG.pglog("{}: {} lines read".format(infile, linidx), PGOPT['wrnlog'])
      if 'NT' not in params: line = PgLOG.pgtrim(line, 2)
      if not line:
         if opt: set_option_value(opt, '', 1, linidx, line, infile)
         continue   # skip empty lines
      if chktbl > 0:
         if re.match(r'^\[{}\]$'.format(table), line, re.I): # found entry for table
            chktbl = 0
            clean_input_values()   # clean previously saved input values
         continue
      else:
         ms = re.match(r'^\[(\w+)\]$', line)
         if ms:
            if chktbl == 0: break     # stop at next sub-title
            if not PGOPT['MSET']:
               input_error(linidx, line, infile, ms.group(1) + ": Cannt process sub-title")
            elif PGOPT['CACT'] != PGOPT['MSET']:
               input_error(linidx, line, infile, "Use Action -{} to Set multiple sub-titles".format(PGOPT['MSET']))
            break   # stop getting info if no table given or a different table

      if colcnt == 0:    # check single value and action lines first
         ms = re.match(mpes, line)
         if ms:   # one value assignment
            key = ms.group(1).strip()
            val = ms.group(2)
            if val and 'NT' not in params: val = val.strip()
            opt = get_option_key(key, 1, 0, linidx, line, infile, table)
            set_option_value(opt, val, 0, linidx, line, infile)
            if not OPTS[opt][2]&PGOPT['TXTBIT']: opt = None
            setcnt += 1
            continue   

         ms = re.match(mpao, line)
         if ms:    # set mode or action option
            key = get_option_key(ms.group(1).strip(), 4, 0, linidx, line, infile, table)
            set_option_value(key, '', 0, linidx, line, infile)
            setcnt += 1
            continue

      # check mutiple value assignment for one or more multi-value options
      values = line.split(params['DV'])
      valcnt = len(values)
      if colcnt == 0:
         while colcnt < valcnt:
            key = values[colcnt].strip()
            if not key: break
            opt = get_option_key(key, 2, 1, linidx, line, infile, table)
            if not opt: break
            columns.append(opt)
            if opt in params: del params[opt]
            colcnt += 1
         if colcnt < valcnt:
            if colcnt == (valcnt-1):
               enddiv = 1
            else:
               input_error(linidx, line, infile, "Multi-value Option Name missed for column {}".format(colcnt+1))
         opt = None
         continue

      elif valcnt == 1:
         if re.match(mpes, line):
            input_error(linidx, line, infile, "Cannot set single value option after Multi-value Options")
         elif re.match(mpao, line):
            input_error(linidx, line, infile, "Cannot set acttion/mode option after Multi-value Options")

      if opt:  # add to multipe line value
         val = values.pop(0)
         valcnt -= 1
         if val and 'NT' not in params: val = val.strip()
         set_option_value(opt, val, 1, linidx, line, infile)
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
            input_error(linidx, line, infile, "Too many values({}) provided for {} columns".format(valcnt+colidx, colcnt))

      if values:
         for val in values:
            opt = columns[colidx]
            colidx += 1
            if val and 'NT' not in params: val = val.strip()
            set_option_value(opt, val, 0, linidx, line, infile)
            setcnt += 1
         colidx += (reduced-enddiv)

      if colidx == colcnt:
         colidx = 0   # done with gathering values of a multi-value line
         opt = None
      elif opt and not OPTS[opt][2]&PGOPT['TXTBIT']:
         colidx += 1
         opt = None

   if setcnt > 0:
      if colidx:
         if colidx < colcnt:
            input_error(linidx, '', infile, "{} of {} values missed".format(colcnt-colidx, colcnt))
         elif enddiv:
            input_error(linidx, '', infile, "Miss end divider '{}'".format(params['DV']))
      return 1   # read something
   else:
      if table: PgLOG.pglog("No option information found for '{}'".format(table), PgLOG.WARNLG)
      return 0  # read nothing

#
# clean params for input option values when set mutiple tables 
#
def clean_input_values():

   global INOPTS
   # clean previously saved input values if any
   for opt in INOPTS:
      del params[opt]
   INOPTS = {}

#
# build a hash record for add or update of a table record
#
def build_record(flds, pgrec, tname, idx = 0):

   record = {}
   if not flds: return record

   hash = TBLHASH[tname]

   for key in flds:
      if key not in hash: continue
      opt = hash[key][0]
      field = hash[key][3] if len(hash[key]) == 4 else hash[key][1]
      ms = re.search(r'\.(.+)$', field)
      if ms: field = ms.group(1)
      if opt in params:
         if OPTS[opt][0] == 1:
            val = params[opt]
         else:
            if OPTS[opt][2]&2 and pgrec and field in pgrec and pgrec[field]: continue
            val = params[opt][idx]
         sval = pgrec[field] if pgrec and field in pgrec else None
         if sval is None:
            if val == '': val = None
         elif isinstance(sval, int):
            if isinstance(val, str): val = (int(val) if val else None)   # change '' to None for int
         if PgUtil.pgcmp(sval, val, 1): record[field] = val    # record new or changed value

   return record

#
# set global variable PGOPT['UID'] with value of user.uid, fatal if unsuccessful
#
def set_uid(aname):
   
   set_email_logact()
 
   if 'LN' not in params:
      params['LN'] = PgLOG.PGLOG['CURUID']
   elif params['LN'] != PgLOG.PGLOG['CURUID']:
      params['MD'] = 1  # make sure this set if running as another user
      if 'NE' not in params: PgLOG.PGLOG['EMLADDR'] = params['LN']
      if 'DM' in params and re.match(r'^(start|begin)$', params['DM'], re.I):
         msg = "'{}' must start Daemon '{} -{}' as '{}'".format(PgLOG.PGLOG['CURUID'], aname, PGOPT['CACT'], params['LN'])
      else:
         msg = "'{}' runs '{} -{}' as '{}'!".format(PgLOG.PGLOG['CURUID'], aname, PGOPT['CACT'], params['LN'])
      PgLOG.pglog(msg,  PGOPT['wrnlog'])
      PgLOG.set_specialist_environments(params['LN'])

   if 'LN' not in params: PgLOG.pglog("Could not get user login name", PGOPT['extlog'])

   validate_dataset()
   if OPTS[PGOPT['CACT']][2] > 0: validate_dsowner(aname)
   
   pgrec = PgDBI.pgget("dssdb.user", "uid", "logname = '{}' AND until_date IS NULL".format(params['LN']), PGOPT['extlog'])
   if not pgrec: PgLOG.pglog("Could not get user.uid for " + params['LN'], PGOPT['extlog'])
   PGOPT['UID'] = pgrec['uid']

   open_output(params['OF'] if 'OF' in params else None)

#
# set global variable PGOPT['UID'] as 0 for a sudo user
#
def set_sudo_uid(aname, uid):

   set_email_logact()
 
   if PgLOG.PGLOG['CURUID'] != uid:
      if 'DM' in params and re.match(r'^(start|begin)$', params['DM'], re.I):
         msg = "'{}': must start Daemon '{} -{} as '{}'".format(PgLOG.PGLOG['CURUID'], aname, params['CACT'], uid)
      else:
         msg = "'{}': must run '{} -{}' as '{}'".format(PgLOG.PGLOG['CURUID'], aname, params['CACT'], uid) 
      PgLOG.pglog(msg, PGOPT['extlog'])

   PGOPT['UID'] = 0
   params['LN'] = PgLOG.PGLOG['CURUID']

#
# set global variable PGOPT['UID'] as 0 for root user
#
def set_root_uid(aname):

   set_email_logact()
 
   if PgLOG.PGLOG['CURUID'] != "root":
      if 'DM' in params and re.match(r'^(start|begin)$', params['DM'], re.I):
         msg = "'{}': you must start Daemon '{} -{} as 'root'".format(PgLOG.PGLOG['CURUID'], aname, params['CACT'])
      else:
         msg = "'{}': you must run '{} -{}' as 'root'".format(PgLOG.PGLOG['CURUID'], aname, params['CACT']) 
      PgLOG.pglog(msg, PGOPT['extlog'])

   PGOPT['UID'] = 0
   params['LN'] = PgLOG.PGLOG['CURUID']

#
# set email logging bits
#
def set_email_logact():

   if 'NE' in params:
      PgLOG.PGLOG['LOGMASK'] &= ~PgLOG.EMLALL   # remove all email bits
   elif 'SE' in params:
      PgLOG.PGLOG['LOGMASK'] &= ~PgLOG.EMLLOG    # no normal email

#
# validate dataset owner
#
# return: 0 or fatal if not valid, 1 if valid, -1 if can not be validated
#
def validate_dsowner(aname, dsid = None, logname = None, pgds = 0, logact = 0):

   if not logname: logname = (params['LN'] if 'LN' in params else PgLOG.PGLOG['CURUID'])
   if logname == PgLOG.PGLOG['RDAUSER']: return 1

   dsids = {}
   if dsid:
      dsids[dsid] = 1
   elif 'DS' in params:
      if OPTS['DS'][0] == 2:
         for dsid in params['DS']:
            dsids[dsid] = 1
      else:
         dsids[params['DS']] = 1
   else:
      return -1

   if not pgds and 'MD' in params: pgds = 1
   if not logact: logact = PGOPT['extlog']
   
   for dsid in dsids:
      if not PgDBI.pgget("dsowner", "", "dsid = '{}' AND specialist = '{}'".format(dsid, logname), PGOPT['extlog']):
         if not PgDBI.pgget("dssgrp", "", "logname = '{}'".format(logname), PGOPT['extlog']):
            return PgLOG.pglog("'{}' is not DSS Specialist!".format(logname), logact)
         elif not pgds:
            return PgLOG.pglog("'{}' not listed as Specialist of '{}'\nRun '{}' with Option -MD!".format(logname, dsid, aname), logact)

   return 1
  
#
# validate dataset
#
def validate_dataset():

   cnt = 1
   if 'DS' in params:
      if OPTS['DS'][0] == 2:
         for dsid in params['DS']:
            cnt = PgDBI.pgget("dataset", "", "dsid = '{}'".format(dsid), PGOPT['extlog'])
            if cnt == 0: break
      else:
         dsid = params['DS']
         cnt = PgDBI.pgget("dataset", "", "dsid = '{}'".format(dsid), PGOPT['extlog'])

   if not cnt: PgLOG.pglog(dsid + " not exists in RDADB!", PGOPT['extlog'])

#
# validate given group indices or group names
#
def validate_groups(parent = 0):

   if parent:
      gi = 'PI'
      gn = 'PN'
   else:
      gi = 'GI'
      gn = 'GN'
   if (OPTS[gi][2]&8): return    # already validated
 
   dcnd = "dsid = '{}'".format(params['DS'])
   if gi in params:
      grpcnt = len(params[gi])
      i = 0
      while i < grpcnt:
         gidx = params[gi][i]
         if not isinstance(gidx, int) and re.match(r'^(!|<|>|<>)$', gidx): break
         i += 1
      if i >= grpcnt:   # normal group index given
         for i in range(grpcnt):
            gidx = params[gi][i]
            gidx = int(gidx) if gidx else 0
            params[gi][i] = gidx
            if gidx == 0 or (i > 0 and gidx == params[gi][i-1]): continue
            if not PgDBI.pgget("dsgroup", '', "{} AND gindex = {}".format(dcnd, gidx), PGOPT['extlog']):
               if i > 0 and parent and params['GI']:
                  j = 0
                  while j < i:
                     if gidx == params['GI'][j]: break
                     j += 1
                  if j < i: continue
               PgLOG.pglog("Group Index {} not in RDADB for {}".format(gidx, params['DS']), PGOPT['extlog'])
      else:    # found none-equal condition sign
         pgrec = PgDBI.pgmget("dsgroup", "DISTINCT gindex", dcnd + PgDBI.get_field_condition("gindex", params[gi]), PGOPT['extlog'])
         grpcnt = (len(pgrec['gindex']) if pgrec else 0)
         if grpcnt == 0:
            PgLOG.pglog("No Group matches given Group Index condition for " + params['DS'], PGOPT['extlog'])

         params[gi] = pgrec['gindex']
   elif gn in params:
      params[gi] = group_id_to_index(params[gn])

   OPTS[gi][2] |= 8  # set validated flag

#
# get group index array from given group IDs
#
def group_id_to_index(grpids):

   count = len(grpids) if grpids else 0
   if count == 0: return None

   indices = []
   dcnd = "dsid = '{}'".format(params['DS'])
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
            pgrec = PgDBI.pgget("dsgroup", "gindex", "{} AND grpid = '{}'".format(dcnd, gid), PGOPT['extlog'])
            if not pgrec: PgLOG.pglog("Group ID {} not in RDADB for {}".format(gid, params['DS']), PGOPT['extlog'])
            indices.append(pgrec['gindex'])
      return indices
   else: # found wildcard and/or none-equal condition sign
      pgrec = PgDBI.pgmget("dsgroup", "DISTINCT gindex", dcnd + PgDBI.get_field_condition("grpid", grpids, 1), PGOPT['extlog'])
      count = (len(pgrec['gindex']) if pgrec else 0)
      if count == 0: PgLOG.pglog("No Group matches given Group ID condition for " + params['DS'], PGOPT['extlog'])
      return pgrec['gindex']

#
# get group ID array from given group indices
#
def group_index_to_id(indices):

   count = len(indices) if indices else 0
   if count == 0: return None

   grpids = []
   dcnd = "dsid = '{}'".format(params['DS'])
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
            pgrec = PgDBI.pgget("dsgroup", "grpid", "{} AND gindex = {}".format(dcnd, gidx), PGOPT['extlog'])
            if not pgrec: PgLOG.pglog("Group Index {} not in RDADB for {}".format(gidx, params['DS']), PGOPT['extlog'])
            grpids.append(pgrec['grpid'])
      return grpids
   else:   # found none-equal condition sign
      pgrec = PgDBI.pgmget("dsgroup", "DISTINCT grpid", dcnd + PgDBI.get_field_condition("gindex", indices), PGOPT['extlog'])
      count = (len(pgrec['grpid']) if pgrec else 0)
      if count == 0: PgLOG.pglog("No Group matches given Group Index condition for " + params['DS'], PGOPT['extlog'])
      return pgrec['grpid']

#
# validate order fields and
# get an array of order fields that are not in given fields
#
def append_order_fields(oflds, flds, tname, excludes = None):

   orders = ''
   hash = TBLHASH[tname]
   for ofld in oflds:
      ufld = ofld.upper()
      if ufld not in hash or excludes and excludes.find(ufld) > -1: continue
      if flds and flds.find(ufld) > -1: continue
      orders += ofld

   return orders

#
# validate mutiple values for given fields
#
def validate_multiple_values(tname, count, flds = None):

   opts = []
   hash = TBLHASH[tname]
   if flds:
      for fld in flds:
         if fld in hash: opts.append(hash[fld][0])
   else:
      for fld in hash:
         opts.append(hash[fld][0])

   validate_multiple_options(count, opts, (1 if tname == 'htarfile' else 0))

#
# validate multiple values for given options
#
def validate_multiple_options(count, opts, remove = 0):

   for opt in opts:
      if opt not in params or OPTS[opt][0] != 2: continue # no value given or not multiple value option
      cnt = len(params[opt])
      if cnt == 1 and count > 1 and OPTS[opt][2]&1:
         val0 = params[opt][0]
         params[opt] = [val0]*count
         OPTS[opt][2] |= 4 # expanded
         cnt = count
      if cnt != count:
         if count == 1 and cnt > 1 and OPTS[opt][2]&PGOPT['TXTBIT']:
            params[opt][0] = ' '.join(params[opt])
         elif remove and cnt == 1 and count > 1:
            del params[opt]
         elif cnt < count:
            PgLOG.pglog("Multi-value Option {}({}): {} Given and {} needed".format(opt, OPTS[opt][1], cnt, count), PGOPT['extlog'])

#
# get field keys for a RDADB table, include all if !include
#
def get_field_keys(tname, include = None, exclude = None):

   fields = ''
   hash = TBLHASH[tname]

   for fld in hash:
      if include and include.find(fld) < 0: continue
      if exclude and exclude.find(fld) > -1: continue
      opt = hash[fld][0]
      if opt in params: fields += fld

   return fields if fields else None

#
# get a string for fields of a RDADB table
#
def get_string_fields(flds, tname, include = None, exclude = None):

   fields = []
   hash = TBLHASH[tname]

   for fld in flds:
      ufld = fld.upper()   # in case
      if include and include.find(ufld) < 0: continue
      if exclude and exclude.find(ufld) > -1: continue
      if ufld not in hash:
         PgLOG.pglog("Invalid field '{}' to get from '{}'".format(fld, tname), PGOPT['extlog'])
      elif hash[ufld][0] not in OPTS:
         PgLOG.pglog("Option '{}' is not defined for field '{} - {}'".format(hash[ufld][0], ufld, hash[ufld][1]), PGOPT['extlog'])
      if len(hash[ufld]) == 4:
         fname = "{} {}".format(hash[ufld][3], hash[ufld][1])
      else:
         fname = hash[ufld][1]
      fields.append(fname)

   return ', '.join(fields) 

#
# get max count for given options
#
def get_max_count(opts):

   count = 0
   for opt in opts:
      if opt not in params: continue
      cnt = len(params[opt])
      if cnt > count: count = cnt

   return count

#
# get a string of fields of a RDADB table for sorting
#
def get_order_string(flds, tname, exclude = None):

   orders = []
   hash = TBLHASH[tname]

   for fld in flds:
      if fld.islower():
         desc = " DESC"
         fld = fld.upper()
      else:
         desc = ""
      if exclude and exclude.find(fld) > -1: continue
      orders.append(hash[fld][1] + desc)

   return (" ORDER BY " + ', '.join(orders)) if orders else ''

#
# get a string for column titles of a given table
#
def get_string_titles(flds, hash, lens):

   titles = []
   colcnt = len(flds)
   for i in range(colcnt):
      fld = flds[i]
      if fld not in hash: continue
      opt = hash[fld][0]
      if opt not in OPTS: PgLOG.pglog("ERROR: Undefined option " + opt, PGOPT['extlog'])
      title = OPTS[opt][1]
      if lens:
         if len(title) > lens[i]: title = opt
         title = "{:{}}".format(title, lens[i])
      titles.append(title)

   return params['DV'].join(titles) + params['DV']

#
# display error message and exit
#
def parameter_error(p, opt = None, lidx = 0, line = 0, infile = None):

   if not opt:
      errmsg = "value passed in without leading info option"
   elif opt == "continue":
      errmsg = "error processing input file on continue Line"
   elif opt == 'specified':
      errmsg = "option -{}/-{} is specified already".format(p, OPTS[p][1])
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
   elif OPTS[opt][0] == 0:
      errmsg = "value follows Mode Option -{}/-{}".format(opt, OPTS[opt][1])
   elif OPTS[opt][0] == 1:
      errmsg = "multiple values follow single-value Option -{}/-{}".format(opt, OPTS[opt][1])
   elif OPTS[opt][0] >= 4:
      errmsg = "value follows Action Option -{}/-{}".format(opt, OPTS[opt][1])
   else:
      errmsg = None
     
   if errmsg:
      if lidx:
         input_error(lidx, line, infile, "{} - {}".format(p, errmsg))
      else:
         PgLOG.pglog("ERROR: {} - {}".format(p, errmsg), PGOPT['extlog'])

#
# wrap function to PgLOG.pglog() for error in input files
#
def input_error(lidx, line, infile, errmsg):

   PgLOG.pglog("ERROR at {}({}): {}\n  {}".format(infile, lidx, line, errmsg), PGOPT['extlog'])

#
# wrap function to PgLOG.pglog() for error for action
#
def action_error(errmsg, cact = None):

   msg = "ERROR"
   if PGOPT['ANAME']: msg += " " + PGOPT['ANAME']
   if not cact: cact = PGOPT['CACT']
   if cact: msg += " for Action {} ({})".format(cact, OPTS[cact][1])

   if 'DS' in params:
      if OPTS['DS'][0] == 1:
         msg += " of " + params['DS']
      elif OPTS['DS'][0] == 2 and len(params['DS']) == 1:
         msg += " of " + params['DS'][0]

   msg += ": " + errmsg
   if PgLOG.PGLOG['DSCHECK']: PgDBI.record_dscheck_error(msg)
   PgLOG.pglog(msg, PGOPT['extlog'])

#
# get the valid option for given parameter by checking if the given option
# name matches either an valid option key (short name) or its long name
# flag: 1 - value key only, 2 - multi-value key only, 3 - action key only,
#       4 - mode&action key only
#
def get_option_key(p, flag = 0, skip = 0, lidx = 0, line = None, infile = None, table = None):

   if p is None: p = ''
   opt = get_short_option(p)
   errmsg = None
   if opt:
      if flag == 1:
         if OPTS[opt][0]&3 == 0: errmsg = "NOT a Value Option"
      elif flag == 2:
         if OPTS[opt][0]&2 == 0: errmsg = "NOT a Multi-Value Option"
      elif flag == 3:
         if OPTS[opt][0] < 4:
            if lidx:
               errmsg = "NOT an Action Option"
            else:
               errmsg = "Miss leading '-' for none action option"
      elif flag == 4:
         if OPTS[opt][0]&3:
            errmsg = "NOT a Mode/Action Option"
      if errmsg: errmsg = "{}({}) - {}".format(opt, OPTS[opt][1], errmsg)
   elif not skip:
      if p:
        errmsg = "-{} - Unknown Option".format(p)
      else:
        errmsg = "'' - Empty Option Name"

   if errmsg:
      if lidx:
         input_error(lidx, line, infile, errmsg)
      else:
         PgLOG.pglog("ERROR: " + errmsg, PGOPT['extlog'])
   elif opt and (table or PGOPT['IFCNT'] and OPTS[opt][0] == 2):
      INOPTS[opt] = 1

   return opt

#
# set values to given options, ignore options set in input files if the options
# already set on command line
#
def set_option_value(opt, val = None, cnl = 0, lidx = 0, line = None, infile = None):

   if opt in CMDOPTS and lidx:   # in input file, but given on command line already
      if opt not in params: params[opt] = CMDOPTS[opt]
      return

   if val is None: val = ''
   if OPTS[opt][0]&3:
      if OPTS[opt][2]&16:
         if not val:
            val = 0
         elif re.match(r'^\d+$', val):
            val = int(val)
      elif val and (opt == 'DS' or opt == 'OD'):
         val = PgUtil.format_dataset_id(val)

   errmsg = None
   if not cnl and OPTS[opt][0]&3:
      if opt in params:
         if OPTS[opt][0] == 2:
            if OPTS[opt][2]&2: del params[opt]   # clean auto set values
         elif params[opt] != val and not OPTS[opt][2]&1:
            errmsg = "'{}', multiple values not allowed for Single-Value Option".format(val)
      if not errmsg and (not PGOPT['CACT'] or OPTS[PGOPT['CACT']][2]):
         dstr = OPTS[opt][3] if len(OPTS[opt]) > 3 else None
         if dstr:
            vlen = len(val)
            ms = re.match(r'^!(\w*)', dstr)
            if ms:
               dstr = ms.group(1)
               if vlen == 1 and dstr.find(val) > -1: errmsg = "{}: character must not be one of '{}'".format(val, str)
            elif vlen > 1 or (vlen == 0 and not OPTS[opt][2]&128) or (vlen == 1 and dstr.find(val) < 0):
               errmsg = "{} single-letter value must be one of '{}'".format(val, dstr)

   if not errmsg:
      if OPTS[opt][0] == 2:    # multiple value option
         if opt not in params:
            params[opt] = [val]   # set the first value
            if opt == 'QF' and PGOPT['ACTS'] == OPTS['DL'][0]: OPTS['FS'][3] = 'ANT'
         else:
            if cnl:
               rowidx = len(params[opt]) - 1
               if params[opt][rowidx]:
                  if not re.match(r'^(DE|DI|DM|DW)$', opt):
                     errmsg = "Multi-line value not allowed"
                  else:
                     params[opt][rowidx] += "\n" + val    # multiple line value
               else:
                  params[opt][rowidx] = val
            else:
               params[opt].append(val)     # add next value
      elif OPTS[opt][0] == 1:          # single value option  
         if cnl and opt in params:
            if val: errmsg = "Multi-line value not allowed"
         elif OPTS[opt][2]&2 and PgUtil.pgcmp(params[opt], val):
            errmsg = "{}: Single-Value Info Option has value '{}' already".format(val, params[opt])
         else:
            params[opt] = val
            OPTS[opt][2] |= 2
      elif val:
         if OPTS[opt][0] == 0 and re.match(r'^(Y|N)$', val, re.I):
            params[opt] = 1 if (val == 'Y' or val == 'y') else 0
         else:
            parameter_error(val, opt, lidx, line, infile)    # no value for flag or action options
      elif opt not in params:
         params[opt] = 1                # set flag or action option
         if OPTS[opt][0] > 2:
            if PGOPT['ACTS']: parameter_error(opt, "duplicate", lidx ,line, infile)   # no duplicated action options
            PGOPT['ACTS'] = OPTS[opt][0]  # add action bit
            PGOPT['CACT'] = opt  # add action name
            if opt == "SB": PGOPT['MSET'] = opt

   if errmsg:
      if lidx:
        input_error(lidx, line, infile, "{}({}) - {}".format(opt, OPTS[opt][1], errmsg))
      else:
        PgLOG.pglog("ERROR: {}({}) - {}".format(opt, OPTS[opt][1], errmsg), PGOPT['extlog'])

   if not lidx: CMDOPTS[opt] = params[opt]    # record options set on command lines

#
# get width for a single row if in column format
#
def get_row_width(pgrec):

   slen = len(params['DV'])   
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

#
# get a short option name by searching dict OPTS and ALIAS
#
def get_short_option(p):

   plen = len(p)   
   if plen == 2:
      p = p.upper()
      if p in OPTS: return p

   for opt in OPTS:     # get main option first
      if not PgUtil.pgcmp(OPTS[opt][1], p, 1): return opt

   for opt in ALIAS: # then check alias option
      for key in ALIAS[opt]:
         if not PgUtil.pgcmp(key, p, 1): return opt

   return None

#
# print result in column format, with multiple values each row
#
def print_column_format(pgrec, flds, hash, lens, retbuf = 0):

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
            PgLOG.pglog(fld + ": Unkown field name", PGOPT['extlog'])

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
      line = params['DV'].join(values) + params['DV'] + "\n"
      if retbuf:
         buf += line
      else:
         OUTPUT.write(line)

   return buf if retbuf else rowcnt

#
# print result in row format, with single value on each row
#
def print_row_format(pgrec, flds, hash):

   for fld in flds:
      if fld not in hash: continue
      line = "{}{}".format(OPTS[hash[fld][0]][1], params['ES'])
      field = hash[fld][1]
      ms = re.search(r'\.(.+)$', field)
      if ms: field = ms.group(1)
      if field in pgrec:
         value = pgrec[field]
         if value is not None: line += str(value)
      OUTPUT.write(line + "\n")

#
# compress/uncompress given files and change the formats accordingly
#
def compress_files(files, formats, count):
   
   if 'UZ' in params:
      strcmp = 'Uncompress'
      actcmp = 0
   else:
      strcmp = 'Compress'
      actcmp = 1
   fmtcnt = len(formats)
   if not fmtcnt: return files   # just in case
   s = 's' if count > 1 else ''
   PgLOG.pglog("{}ing {} File{} for {} ...".format(strcmp, count, s, params['DS']), PGOPT['wrnlog'])
   cmpcnt = 0
   for i in range(count):
      fmt = formats[i] if(i < fmtcnt and formats[i]) else formats[0]
      (ofile, fmt) = PgFile.compress_local_file(files[i], fmt, actcmp, PGOPT['extlog'])
      if ofile != files[i]:
         files[i] = ofile
         cmpcnt += 1
   
   PgLOG.pglog("{}/{} Files {}ed for {}".format(cmpcnt, count, strcmp, params['DS']) , PGOPT['emllog'])
   
   if 'ZD' in params: del params['ZD']
   if 'UZ' in params: del params['UZ']
   
   return files

#
# get hash condition
# tname - table name to identify a table hash
# noand - 1 for not add leading 'AND'
#
def get_hash_condition(tname, include = None, exclude = None, noand = 0):
   
   condition = ''
   hash = TBLHASH[tname]

   for key in hash:
      if include and include.find(key) < 0: continue
      if exclude and exclude.find(key) > -1: continue
      opt = hash[key][0]
      if opt not in params: continue    # no option value
      flg = hash[key][2]
      if flg < 0: # condition is ignore for this option
         PgLOG.pglog("Condition given per Option -{} (-{}) is ignored".format(opt, OPTS[opt][1]), PGOPT['errlog'])
         continue

      fld = hash[key][1]
      condition += PgDBI.get_field_condition(fld, params[opt], flg, noand)
      noand = 0

   return condition

#
# set default params value for given opt empty the value if 'all' is given
#
def set_default_value(opt, dval = None):

   flag = OPTS[opt][0]
   if flag&3 == 0: return    # skip if not single&multiple value options

   oval = 0
   if opt in params:
      if flag == 1:
         oval = params[opt]
      else:
         count = len(params[opt])
         if count == 1:
            oval = params[opt][0]
         elif count > 1:
            return             # multiple values given already

   if oval:
      if re.match(r'^all$', oval, re.I):
         del params[opt]       # remove option value for all
      return                   # value given already

   if dval:
      # set default value      
      if flag == 1:
         params[opt] = dval
      else:
         params[opt] = [dval]

#
# add/strip COS block for give file name and cosflg if given/not-given cosfile
# return the file size after the convertion
#
def cos_convert(locfile, cosflg, cosfile = None):

   if cosfile:
      cmd = "cosconvert -{} {} {}".format(cosflg, cosfile, locfile)
   else:
      cmd = "cosconvert -{} {}".format(cosflg.lower(), locfile)
      cosfile = locfile

   PgLOG.pgsystem(cmd)
   info = PgFile.check_local_file(cosfile)
   if not info:
      return PgLOG.pglog("Error - " + cmd, PGOPT['errlog'])   # should not happen
   else:
      return info['data_size']

#
# evaluate count of values for given options
#
def get_option_count(opts):

   count = 0
   for opt in opts:
      if opt in params:
         cnt = len(params[opt])
         if cnt > count: count = cnt
   if count > 0: validate_multiple_options(count, opts) 

   return count

#
# gather subgroup indices recursively for given condition
#  dsid: Dataset Id
#  pidx: parent group index
# gtype: group type if not empty, P - public groups only)
#
# Return: array reference of group indices
#
def get_all_subgroups(dcnd, pidx, gtype = None):

   gidxs = [pidx]
   gflds = "gindex"
   if gtype: gflds += ", grptype"
   grecs = PgDBI.pgmget("dsgroup", gflds, "{} and pindex = {}".format(dcnd, pidx), PgLOG.LGWNEX)
   if not grecs: return gidxs

   gcnt = len(grecs['gindex'])
   for i in range(gcnt):
      gidx = grecs['gindex'][i]
      if abs(gidx) <= abs(pidx) or gtype and grecs['grptype'][i] != gtype: continue
      subs = get_all_subgroups(dcnd, gidx, gtype)
      gidxs.extend(subs)

   return gidxs

#
# gather public subgroup indices recursively for given condition. A group index is
# gathered only if there are data files right under it. The pidx is included too
# if file count of it larger then zero.
#  dsid: Dataset Id
#  pidx: parent group index
#  cfld: count field (dwebcnt, nwebcnt, savedcnt)
# pfcnt: file count for parent group index pidx 0 to skip)
#
# Return: array reference of group indices
#
def get_data_subgroups(dcnd, pidx, cfld, pfcnt = 0):

   if not pfcnt:    # get file count for the parent group
      pfcnt = group_file_count(dcnd, pidx, cfld)
      if not pfcnt: return None

   gflds = "gindex, " + cfld
   gcnd = "{} AND pindex = {} AND {} > 0".format(dcnd, pidx, cfld)
   grecs = PgDBI.pgmget("dsgroup", gflds, gcnd, PgLOG.LGWNEX)
   if not grecs: return ([pidx] if pfcnt > 0 else None)

   gcnt = len(grecs['gindex'])
   gidxs = []
   for i in range(gcnt):
      gidx = grecs['gindex'][i]
      fcnt = grecs[cfld][i]
      if fcnt == 0 or abs(gidx) <= abs(pidx): continue
      subs = get_data_subgroups(dcnd, gidx, cfld, fcnt)
      if subs: gidxs.extend(subs)
      pfcnt -= fcnt
   if pfcnt > 0: gidxs.insert(0, pidx)

   return (gidxs if gidxs else None)

#
# get group file count for given count field name
#
def group_file_count(cnd, gidx, cfld):

   if gidx:
      table = "dsgroup"
      cnd += " AND gindex = {}".format(gidx)
   else:
      table = "dataset"
   pgrec = PgDBI.pgget(table, cfld, cnd)

   return (pgrec[cfld] if pgrec else 0)

#
# set file format for actions -AM/-AW from given local files
#
def set_file_format(count):

   if 'LF' in params:
      files = params['LF']
   else:
      return
   
   fmtcnt = 0
   fmts = [None] * count
   for i in range(count):
      fmt = PgFile.get_file_format(files[i])
      if fmt:
         fmtcnt += 1
         fmts[i] = fmt

   if fmtcnt:
      params['AF'] = fmts
      OPTS['AF'][2] |= 2

#
# get frequency information
#
def get_control_frequency(frequency):

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
      if nf:
         unit = "fraction of month frequency '{}' MUST be (2,3,5,6,10)".format(frequency)
      elif unit:
         val = "frequency '{}' MUST be larger than 0".format(frequency)
      elif re.search(r'/(\d+)$', frequency):
         val = "fractional frequency '{}' for month ONLY".format(frequency)
      else:
         val = "invalid frequency '{}', unit must be (Y,M,W,D,H)".format(frequency)
      return (None, unit)

   freq = [0]*7   # initialize the frequence list
   uidx = {'Y' : 0, 'D' : 2, 'H' : 3, 'N' : 4, 'S' : 5}
   if unit == 'M':
      freq[1] = val
      if nf: freq[6] = nf     # number of fractions in a month
   elif unit == 'W':
      freq[2] = 7 * val
   elif unit in uidx:
      freq[uidx[unit]] = val

   return (freq, unit)

#
#  check if valid data time for given pindex
#
def valid_data_time(pgrec, cstr = None, logact = 0):

   if pgrec['pindex'] and pgrec['datatime']:
      (freq, unit) = get_control_frequency(pgrec['frequency'])
      if not freq:
         if cstr: PgLOG.pglog("{}: {}".format(cstr, unit), logact)
         return PgLOG.FAILURE

      dtime = PgUtil.adddatetime(pgrec['datatime'], freq[0], freq[1], freq[2], freq[3], freq[4], freq[5], freq[6])
      if PgDBI.pgget("dcupdt", "", "cindex = {} AND datatime < '{}'".format(pgrec['pindex'], dtime), PGOPT['extlog']):
         if cstr: PgLOG.pglog("{}: MUST be processed After Control Index {}".format(cstr, pgrec['pindex']), logact)
         return PgLOG.FAILURE

   return PgLOG.SUCCESS

#
# publish filelists for given datasets
# 
def publish_dataset_filelist(dsids):

   for dsid in dsids:
      PgLOG.pgsystem("publish_filelist " + dsid, PGOPT['wrnlog'], 7)

#
# get the current active version index for given dsid
#
def get_version_index(dsid, logact = 0):

   pgrec = PgDBI.pgget("dsvrsn", "vindex", "dsid = '{}' AND status = 'A'".format(dsid), logact)

   return (pgrec['vindex'] if pgrec else 0)

#
# append given format (data or archive) sfmt to format string sformat
#
def append_format_string(sformat, sfmt, chkend = 0):

   mp = r'(^|\.){}$' if chkend else r'(^|\.){}(\.|$)'
   if sfmt:
      if not sformat:
         sformat = sfmt
      else:
         for fmt in re.split(r'\.', sfmt):
            if not re.search(mp.format(fmt), sformat, re.I): sformat += '.' + fmt

   return sformat

#
# get request type string or shared info
#
def request_type(rtype, idx = 0):

   RTYPE = {
      'C' : ["Customized Data",                0],
      'D' : ["CDP Link",                       0],
      'M' : ["Delayed Mode Data",              1],
      'N' : ["NCARDAP(THREDDS) Data Server",   0],
      'Q' : ["Database Query",                 0],
      'R' : ["Realtime Data",                  0],
      'S' : ["Subset Data",                    0],
      'T' : ["Subset/Format-Conversion Data",  0],
      'F' : ["Format Conversion Data",         1],  # web
      'A' : ["Archive Format Conversion",      1],  # web
      'P' : ["Plot Chart",                     0],
      'U' : ["Data",                           0]
   }
   
   if rtype not in RTYPE: rtype = 'U'
   
   return RTYPE[rtype][idx]

#
# email notice of for user
#
def send_request_email_notice(pgrqst, errmsg, fcount, rstat, readyfile = None, pgpart = None):

   pgcntl = PGOPT['RCNTL']
   rhome = params['WH'] if 'WH' in params and params['WH'] else PgLOG.PGLOG['RQSTHOME']
   if errmsg:
      if pgpart:
         if cache_partition_email_error(pgpart['rindex'], errmsg): return rstat
         enote = "email_part_error"
      else:
         enote = "email_error"
   elif fcount == 0:
      if pgcntl and pgcntl['empty_out'] == 'Y':
         enote = "email_empty"
      else:
         errmsg = "NO output data generated"
         if pgpart:
            if cache_partition_email_error(pgpart['rindex'], errmsg): return rstat
            enote = "email_part_error"
         else:
            enote = "email_error"
   elif 'EN' in params and params['EN'][0]:
      enote = params['EN'][0]
   elif pgrqst['enotice']:
      enote = pgrqst['enotice']
   elif pgcntl and pgcntl['enotice']:
      enote = pgcntl['enotice']
   elif pgrqst['globus_transfer'] == 'Y' and pgrqst['task_id']:
      enote = "email_notice_globus"
   else:
      enote = "email_" + ("command" if pgrqst['location'] else "notice")

   if enote[0] not in '/.': enote = "{}/notices/{}".format(rhome, enote)      

   finfo = PgFile.check_local_file(enote, 128)
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
         PgLOG.pglog("{}: {}\nCannot email error to {}@ucar.edu: {}".format(enote, ferror, PgLOG.PGLOG['CURUID'], errmsg),
                     (PGOPT['errlog'] if rstat else PGOPT['extlog']))
         return "E"
      else:
         errmsg = PgLOG.pglog("{}: {}\nCannot email notice to {}".format(enote, ferror, pgrqst['email']), PGOPT['errlog']|PgLOG.RETMSG)
         enote = rhome + "/notices/email_error"
         ef = open(enote, 'r')
         rstat = 'E'

   ebuf = ''
   ebuf += ef.read()
   ef.close()

   einfo = {}
   einfo['HOSTNAME'] = PgLOG.PGLOG['HOSTNAME']
   einfo['DSID'] = pgrqst['dsid']
   einfo['DSSURL'] = PgLOG.PGLOG['DSSURL']
   if pgrqst['location']:
      einfo['WHOME'] = pgrqst['location']
   else:
      einfo['WHOME'] = PgLOG.PGLOG['RQSTURL']
   einfo['SENDER'] = pgrqst['specialist'] + "@ucar.edu"
   einfo['RECEIVER'] = pgrqst['email']
   einfo['RTYPE'] = request_type(pgrqst['rqsttype'])
   PgLOG.add_carbon_copy() # clean carbon copy email in case not empty
   exclude = (einfo['SENDER'] if errmsg else einfo['RECEIVER'])
   if not errmsg and pgcntl and pgcntl['ccemail']:
      PgLOG.add_carbon_copy(pgcntl['ccemail'], 1, exclude, pgrqst['specialist'])
   if PgLOG.PGLOG['CURUID'] != pgrqst['specialist'] and PgLOG.PGLOG['CURUID'] != PgLOG.PGLOG['RDAUSER']:
      PgLOG.add_carbon_copy(PgLOG.PGLOG['CURUID'], 1, exclude)
   if 'CC' in params: PgLOG.add_carbon_copy(params['CC'], 0, exclude)
   einfo['CCD'] = PgLOG.PGLOG['CCDADDR']
   einfo['RINDEX'] = str(pgrqst['rindex'])
   einfo['RQSTID'] = pgrqst['rqstid']
   pgrec = PgDBI.pgget("dataset", "title", "dsid = '{}'".format(pgrqst['dsid']), PGOPT['extlog'])
   einfo['DSTITLE'] = pgrec['title'] if pgrec and pgrec['title'] else ''
   einfo['SUBJECT'] = ''
   if errmsg:
      einfo['ERRMSG'] = PgLOG.get_error_command(int(time.time()), PGOPT['errlog']) + errmsg
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
         einfo['DAYS'] = str(PGOPT['VP'])
      pgrec = PgDBI.pgget("dssgrp", "lstname, fstname, phoneno",
                          "logname = '{}'".format(PgLOG.PGLOG['CURUID']), PGOPT['extlog'])
      if pgrec:
         einfo['SPECIALIST'] = "{} {}".format(pgrec['fstname'], pgrec['lstname'])
         einfo['PHONENO'] = pgrec['phoneno']
      einfo['SUBJECT'] += " {} {} request {} - {}!".format(pgrqst['dsid'], einfo['RTYPE'], pgrqst['rindex'], pgrqst['email'])

   if pgrqst['note']:
      einfo['RNOTE'] = "\nRequest Detail:\n{}\n".format(pgrqst['note'])
   elif fcount > 0 and pgrqst['rinfo']:
      einfo['RNOTE'] = "\nRequest Detail:\n{}\n".format(pgrqst['rinfo'])
   else:
      einfo['RNOTE'] = ""
   
   if pgrqst['globus_transfer'] == 'Y' and pgrqst['task_id']:
      einfo['GLOBUS_TASK_URL'] = "https://app.globus.org/activity/" + pgrqst['task_id']

   for ekey in einfo:
      ebuf = re.sub(r'<{}>'.format(ekey), einfo[ekey], ebuf)

   if PgLOG.PGLOG['DSCHECK'] and not pgpart:
      tbl = "dscheck"
      cnd = "cindex = {}".format(PgLOG.PGLOG['DSCHECK']['cindex'])
   else:
      tbl = "dsrqst"
      cnd = "rindex = {}".format(pgrqst['rindex'])

   if not PgDBI.cache_customized_email(tbl, "einfo", cnd, ebuf, 0): return 'E'
   if errmsg:
      PgLOG.pglog("Error Email {} cached to {}.einfo for {}:\n{}".format(einfo['SENDER'], tbl, cnd, errmsg),
                  PGOPT['errlog'])
   else:
      PgLOG.pglog("{}Email {} cached to {}.einfo for {}\nSubset: {}".format(("Customized " if pgrqst['enotice'] else ""), einfo['RECEIVER'], tbl, cnd, einfo['SUBJECT']),
                  PGOPT['wrnlog']|PgLOG.FRCLOG)
      if readyfile:
         rf = open(readyfile, 'w')
         rf.write(ebuf)
         rf.close()
         PgFile.set_local_mode(readyfile, 1, PgLOG.PGLOG['FILEMODE'])

   return rstat

#
#  cache partition process error to existing email buffer
#
def cache_partition_email_error(ridx, errmsg):

   pkey = "<PARTERR>"
   pgrec = PgDBI.pgget("dsrqst", 'einfo', "rindex = {}".format(ridx), PGOPT['extlog'])
   if not (pgrec and pgrec['einfo'] and pgrec['einfo'].find(pkey) > -1): return 0

   errmsg = PgLOG.get_error_command(int(time.time()), PGOPT['errlog']) + ("{}\n{}".format(errmsg, pkey))
   pgrec['einfo'] = re.sub(pkey, errmsg, pgrec['einfo'])

   return PgDBI.pgupdt("dsrqst", pgrec, "rindex = {}".format(ridx), PGOPT['extlog'])
