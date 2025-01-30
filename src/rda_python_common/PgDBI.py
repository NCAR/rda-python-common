#
###############################################################################
#
#     Title : PgDBI.py  -- PostgreSQL DataBase Interface
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 06/07/2022
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : Python library module to handle query and manipulate PostgreSQL database
#
#    Github : https://github.com/NCAR/rda-python-common.git
#
###############################################################################

import os
import re
import time
from datetime import datetime
import psycopg2 as PgSQL
from psycopg2.extras import execute_values
from psycopg2.extras import execute_batch
from os import path as op
from . import PgLOG

pgdb = None    # reference to a connected database object
curtran = 0    # 0 - no transaction, 1 - in transaction
NMISSES = []   # array of mising userno
LMISSES = []   # array of mising logname
TABLES = {}      # record table field information
SEQUENCES = {}   # record table sequence fielnames
SPECIALIST = {}  # hash array refrences to specialist info of dsids
SYSDOWN = {}
PGDBI = {}
ADDTBLS = []
PGSIGNS = ['!', '<', '>', '<>']
CHCODE = 1042

# hard coded db ports for dbnames
DBPORTS = {
  'default' : 0
}

DBPASS = {}

# hard coded db names for given schema names
DBNAMES = {
   'ivaddb' : 'ivaddb',
   'cntldb' : 'ivaddb',
   'cdmsdb' : 'ivaddb',
   'ispddb' : 'ispddb',
    'obsua' : 'upadb',
  'default' : 'rdadb',
}

# hard coded socket paths for machine_dbnames
DBSOCKS = {
  'default' : '',
}

# home path for check db on alter host
VIEWHOMES = {
  'default' : PgLOG.PGLOG['DSSDBHM']
}

# add more to the list if used for names
PGRES = ['end', 'window']

#
#  PostgreSQL specified query timestamp format
#
fmtyr = lambda fn: "extract(year from {})::int".format(fn)
fmtqt = lambda fn: "extract(quarter from {})::int".format(fn)
fmtmn = lambda fn: "extract(month from {})::int".format(fn)
fmtdt = lambda fn: "date({})".format(fn)
fmtym = lambda fn: "to_char({}, 'yyyy-mm')".format(fn)
fmthr = lambda fn: "extract(hour from {})::int".format(fn)

#
# set environments and defaults
#
def SETPGDBI(name, value):
   PGDBI[name] = PgLOG.get_environment(name, value)

SETPGDBI('DEFDB', 'rdadb')
SETPGDBI("DEFSC", 'dssdb')
SETPGDBI('DEFHOST', PgLOG.PGLOG['PSQLHOST'])
SETPGDBI("DEFPORT", 0)
SETPGDBI("DEFSOCK", '')
SETPGDBI("DBNAME", PGDBI['DEFDB'])
SETPGDBI("SCNAME", PGDBI['DEFSC'])
SETPGDBI("LNNAME", PGDBI['DEFSC'])
SETPGDBI("PWNAME", None)
SETPGDBI("DBHOST", (os.environ['DSSDBHOST'] if os.environ.get('DSSDBHOST') else PGDBI['DEFHOST']))
SETPGDBI("DBPORT", 0)
SETPGDBI("ERRLOG", PgLOG.LOGERR)   # default error logact
SETPGDBI("EXITLG", PgLOG.LGEREX)   # default exit logact
SETPGDBI("DBSOCK", '')
SETPGDBI("DATADIR", PgLOG.PGLOG['DSDHOME'])
SETPGDBI("BCKPATH", PgLOG.PGLOG['DSSDBHM'] + "/backup")
SETPGDBI("SQLPATH", PgLOG.PGLOG['DSSDBHM'] + "/sql")
SETPGDBI("VWNAME", PGDBI['DEFSC'])
SETPGDBI("VWPORT", 0)
SETPGDBI("VWSOCK", '')

PGDBI['DBSHOST'] = PgLOG.get_short_host(PGDBI['DBHOST'])
PGDBI['DEFSHOST'] = PgLOG.get_short_host(PGDBI['DEFHOST'])
PGDBI['VWHOST'] = PgLOG.PGLOG['PVIEWHOST']
PGDBI['VWSHOST'] = PgLOG.get_short_host(PGDBI['VWHOST'])
PGDBI['VWHOME'] =  (VIEWHOMES[PgLOG.PGLOG['HOSTNAME']] if PgLOG.PGLOG['HOSTNAME'] in VIEWHOMES else VIEWHOMES['default'])
PGDBI['SCPATH'] = None       # additional schema path for set search_path
PGDBI['VHSET'] = 0
PGDBI['PGSIZE'] = 1000        # number of records for page_size
PGDBI['MTRANS'] = 5000       # max number of changes in one transactions
PGDBI['MAXICNT'] = 12000000  # maximum number of records in each table

#
# create a pgddl command string with
# table name (tname), prefix (pre) and suffix (suf)
#
def get_pgddl_command(tname, pre = None, suf = None):

   ms = re.match(r'^(.+)\.(.+)$', tname)
   if ms:
      scname = ms.group(1)
      tname = ms.group(2)
   else:
      scname = PGDBI['SCNAME']
   xy = ''
   if suf: xy += ' -x ' + suf
   if pre: xy += ' -y ' + pre
   return "pgddl {} -aa -h {} -d {} -c {} -u {}{}".format(tname, PGDBI['DBHOST'], PGDBI['DBNAME'], scname, PGDBI['LNNAME'], xy)

#
# set default connection for dssdb PostgreSQL Server
#
def dssdb_dbname():
   default_scinfo(PGDBI['DEFDB'], PGDBI['DEFSC'], PgLOG.PGLOG['PSQLHOST'])

dssdb_scname = dssdb_dbname

#
# set default connection for obsua PostgreSQL Server
#
def obsua_dbname():
   default_scinfo('upadb', 'obsua', "rda-pgdb-03.ucar.edu")

obsua_scname = obsua_dbname

#
# set default connection for ivaddb PostgreSQL Server
#
def ivaddb_dbname():
   default_scinfo('ivaddb', 'ivaddb', "rda-pgdb-03.ucar.edu")

ivaddb_scname = ivaddb_dbname

#
# set default connection for ispddb PostgreSQL Server
#
def ispddb_dbname():
   default_scinfo('ispddb', 'ispddb', "rda-pgdb-03.ucar.edu")

ispddb_scname = ispddb_dbname

#
# set a default schema info with hard coded info
#
def default_dbinfo(scname = None, dbhost = None, lnname = None, pwname = None, dbport = None, socket = None):

   return default_scinfo(get_dbname(scname), scname, dbhost, lnname, pwname, dbport, socket)

#
# set default database/schema info with hard coded info
#
def default_scinfo(dbname = None, scname = None, dbhost = None, lnname = None, pwname = None, dbport = None, socket = None):

   if not dbname: dbname = PGDBI['DEFDB']
   if not scname: scname = PGDBI['DEFSC']
   if not dbhost: dbhost = PGDBI['DEFHOST']
   if dbport is None: dbport = PGDBI['DEFPORT']
   if socket is None:  socket = PGDBI['DEFSOCK']

   set_scname(dbname, scname, lnname, pwname, dbhost, dbport, socket)

#
# get the datbase sock file name of a given dbname for local connection
#
def get_dbsock(dbname):
   
   return (DBSOCKS[dbname] if dbname in DBSOCKS else DBSOCKS['default'])

#
# get the datbase port number of a given dbname for remote connection
#
def get_dbport(dbname):

   return (DBPORTS[dbname] if dbname in DBPORTS else DBPORTS['default'])

#
# get the datbase name of a given schema name for remote connection
#
def get_dbname(scname):

   if scname:
      if scname in DBNAMES: return DBNAMES[scname]
      return DBNAMES['default']
   return None

#
# set connection for viewing database information
#
def view_dbinfo(scname = None, lnname = None, pwname = None):

   return view_scinfo(get_dbname(scname), scname, lnname, pwname)

#
# set connection for viewing database/schema information
#
def view_scinfo(dbname = None, scname = None, lnname = None, pwname = None):

   if not dbname: dbname = PGDBI['DEFDB']
   if not scname: scname = PGDBI['DEFSC']

   set_scname(dbname, scname, lnname, pwname, PgLOG.PGLOG['PVIEWHOST'], PGDBI['VWPORT'])

#
# set connection for given scname
#
def set_dbname(scname = None, lnname = None, pwname = None, dbhost = None, dbport = None, socket = None):

   if not scname: scname = PGDBI['DEFSC']
   return set_scname(get_dbname(scname), scname, lnname, pwname, dbhost, dbport, socket)

#
# set connection for given database & schema names
#
def set_scname(dbname = None, scname = None, lnname = None, pwname = None, dbhost = None, dbport = None, socket = None):

   global pgdb
   changed = 0

   if dbname and dbname != PGDBI['DBNAME']:
      PGDBI['DBNAME'] = dbname
      changed = 1
   if scname and scname != PGDBI['SCNAME']:
      PGDBI['LNNAME'] = PGDBI['SCNAME'] = scname
      changed = 1
   if lnname and lnname != PGDBI['LNNAME']:
      PGDBI['LNNAME'] = lnname
      changed = 1
   if pwname != PGDBI['PWNAME']:
      PGDBI['PWNAME'] = pwname
      changed = 1
   if dbhost and dbhost != PGDBI['DBHOST']:
      PGDBI['DBHOST'] = dbhost
      PGDBI['DBSHOST'] = PgLOG.get_short_host(dbhost)
      changed = 1
   if PGDBI['DBSHOST'] == PgLOG.PGLOG['HOSTNAME']:
      if socket is None: socket = get_dbsock(dbname)
      if socket != PGDBI['DBSOCK']:
         PGDBI['DBSOCK'] = socket
         changed = 1
   else:
      if not dbport: dbport = get_dbport(dbname)
      if dbport != PGDBI['DBPORT']:
         PGDBI['DBPORT'] = dbport
         changed = 1

   if changed and pgdb is not None: pgdisconnect(1)

#
# start a database transaction and exit if fails
#
def starttran():

   global curtran
   global pgdb

   if curtran == 1: endtran()   # try to end previous transaction
   if not pgdb:
      pgconnect(0, 0, False)
   else:
      try:
         pgdb.isolation_level
      except PgSQL.OperationalError as e:
         pgconnect(0, 0, False)
      if pgdb.closed:
         pgconnect(0, 0, False)
      elif pgdb.autocommit:
         pgdb.autocommit = False
   curtran = 1

#
# end a transaction with changes committed and exit if fails
#
def endtran(autocommit = True):

   global curtran
   global pgdb
   if curtran and pgdb:
      if not pgdb.closed: pgdb.commit()
      pgdb.autocommit = autocommit
      curtran = 0 if autocommit else 1

#
# end a transaction without changes committed and exit inside if fails
#
def aborttran(autocommit = True):

   global curtran
   global pgdb
   if curtran and pgdb:
      if not pgdb.closed: pgdb.rollback()
      pgdb.autocommit = autocommit
   curtran = 0 if autocommit else 1

#
# record error message to dscheck record and clean the lock
#
def record_dscheck_error(errmsg):

   cnd = PgLOG.PGLOG['DSCHECK']['chkcnd']
   if PgLOG.PGLOG['NOQUIT']: PgLOG.PGLOG['NOQUIT'] = 0
   dflags = PgLOG.PGLOG['DSCHECK']['dflags']

   pgrec = pgget("dscheck", "mcount, tcount, lockhost, pid", cnd, PgLOG.LGEREX)
   if not pgrec: return 0
   if not pgrec['pid'] and not pgrec['lockhost']: return 0
   (chost, cpid) = PgLOG.current_process_info()
   if pgrec['pid'] != cpid or pgrec['lockhost'] != chost: return 0

   # update dscheck record only if it is still locked by the current process
   record = {}
   record['chktime'] = int(time.time())
   record['status'] = "E"
   record['pid'] = 0   # release lock
   if dflags:
      record['dflags'] = dflags
      record['mcount'] = pgrec['mcount'] + 1
   else:
      record['dflags'] = ''

   if errmsg:
      errmsg = PgLOG.break_long_string(errmsg, 512, None, 50, None, 50, 25)
      if pgrec['tcount'] > 1: errmsg = "Try {}: {}".format(pgrec['tcount'], errmsg)
      record['errmsg'] = errmsg

   return pgupdt("dscheck", record, cnd, PGDBI['ERRLOG'])

#
# local function to log query error
#
def qelog(dberror, sleep, sqlstr, vals, pgcnt, logact = PGDBI['ERRLOG']):

   retry = " Sleep {}(sec) & ".format(sleep) if sleep else " "
   if sqlstr:
      if sqlstr.find("Retry ") == 0:
         retry += "the {} ".format(PgLOG.int2order(pgcnt+1))
      elif sleep:
         retry += "the {} Retry: \n".format(PgLOG.int2order(pgcnt+1))
      elif pgcnt:
         retry = " Error the {} Retry: \n".format(PgLOG.int2order(pgcnt))
      else:
         retry = "\n"
      sqlstr = retry + sqlstr
   else:
      sqlstr = ''

   if vals: sqlstr += " with values: " + str(vals)

   if dberror: sqlstr = "{}\n{}".format(dberror, sqlstr)
   if logact&PgLOG.EXITLG and PgLOG.PGLOG['DSCHECK']: record_dscheck_error(sqlstr)
   PgLOG.pglog(sqlstr, logact)
   if sleep: time.sleep(sleep)

   return PgLOG.FAILURE    # if not exit in PgLOG.pglog()

#
# try to add a new table according the table not exist error
#
def try_add_table(dberror, logact):

   ms = re.match(r'^42P01 ERROR:  relation "(.+)" does not exist', dberror)
   if ms:
      tname = ms.group(1)
      add_new_table(tname, logact = logact)

#
# add a table for given table name
#
def add_a_table(tname, logact):

   add_new_table(tname, logact = logact)   

#
# add a new table for given table name
#
def add_new_table(tname, pre = None, suf = None, logact = 0):

   if pre:
      tbname = '{}_{}'.format(pre, tname)
   elif suf:
      tbname = '{}_{}'.format(tname, suf)
   else:
      tbname = tname
   if tbname in ADDTBLS: return

   PgLOG.pgsystem(get_pgddl_command(tname, pre, suf), logact)
   ADDTBLS.append(tbname)

#
# validate a table for given table name (tname), prefix (pre) and suffix (suf),
# and add it if not existing
#
def valid_table(tname, pre = None, suf = None, logact = 0):

   if pre:
      tbname = '{}_{}'.format(pre, tname)
   elif suf:
      tbname = '{}_{}'.format(tname, suf)
   else:
      tbname = tname
   if tbname in ADDTBLS: return tbname

   if not pgcheck(tbname, logact): PgLOG.pgsystem(get_pgddl_command(tname, pre, suf), logact)
   ADDTBLS.append(tbname)
   return tbname

#
# local function to log query error
#
def check_dberror(pgerr, pgcnt, sqlstr, ary, logact = PGDBI['ERRLOG']):

   ret = PgLOG.FAILURE

   pgcode = pgerr.pgcode
   pgerror = pgerr.pgerror
   dberror = "{} {}".format(pgcode, pgerror) if pgcode and pgerror else str(pgerr)
   if pgcnt < PgLOG.PGLOG['DBRETRY']:
      if not pgcode:
         if PGDBI['DBNAME'] == PGDBI['DEFDB'] and PGDBI['DBSHOST'] != PGDBI['DEFSHOST']:
            default_dbinfo()
            qelog(dberror, 0, "Retry Connecting to {} on {}".format(PGDBI['DBNAME'], PGDBI['DBHOST']), ary, pgcnt, PgLOG.MSGLOG)
         else:
            qelog(dberror, 5+5*pgcnt, "Retry Connecting", ary, pgcnt, PgLOG.LOGWRN)
         return PgLOG.SUCCESS
      elif re.match(r'^(08|57)', pgcode):
         qelog(dberror, 0, "Retry Connecting", ary, pgcnt, PgLOG.LOGWRN)
         pgconnect(1, pgcnt + 1)
         return (PgLOG.FAILURE if not pgdb else PgLOG.SUCCESS)
      elif re.match(r'^55', pgcode):  #  try to lock again
         qelog(dberror, 10, "Retry Locking", ary, pgcnt, PgLOG.LOGWRN)
         return PgLOG.SUCCESS
      elif pgcode == '25P02':   #  try to add table
         qelog(dberror, 0, "Rollback transaction", ary, pgcnt, PgLOG.LOGWRN)
         pgdb.rollback()
         return PgLOG.SUCCESS
      elif pgcode == '42P01' and logact&PgLOG.ADDTBL:   #  try to add table
         qelog(dberror, 0, "Retry after adding a table", ary, pgcnt, PgLOG.LOGWRN)
         try_add_table(dberror, logact)
         return PgLOG.SUCCESS

   if logact&PgLOG.DOLOCK and pgcode and re.match(r'^55\w\w\w$', pgcode): 
      logact &= ~PgLOG.EXITLG   # no exit for lock error
   return qelog(dberror, 0, sqlstr, ary, pgcnt, logact)

#
# return hash reference to postgresql batch mode command and output file name
#
def pgbatch(sqlfile, foreground = 0):

#   if(PGDBI['VWHOST'] and PGDBI['VWHOME'] and
#      PGDBI['DBSHOST'] == PGDBI['VWSHOST'] and PGDBI['SCNAME'] == PGDBI['VWNAME']):
#      slave = "/{}/{}.slave".format(PGDBI['VWHOME'], PGDBI['VWHOST'])
#      if not op.exists(slave): default_scname()

   dbhost = 'localhost' if PGDBI['DBSHOST'] == PgLOG.PGLOG['HOSTNAME'] else PGDBI['DBHOST'] 
   options = "-h {} -p {}".format(dbhost, PGDBI['DBPORT'])
   pwname = get_pgpass_password()
   os.environ['PGPASSWORD'] = pwname
   options += " -U {} {}".format(PGDBI['LNNAME'], PGDBI['DBNAME'])

   if not sqlfile: return options

   if foreground:
      batch = "psql {} < {} |".format(options, sqlfile)
   else:
      batch['out'] = sqlfile
      if re.search(r'\.sql$', batch['out']):
         batch['out'] = re.sub(r'\.sql$', '.out', batch['out'])
      else:
         batch['out'] += ".out"

      batch['cmd'] = "psql {} < {} > {} 2>&1".format(options, sqlfile , batch['out'])

   return batch

#
# start a connection to dssdb database and return a DBI object; None if error
# force connect if connect > 0
#
def pgconnect(reconnect = 0, pgcnt = 0, autocommit = True):

   global pgdb

   if pgdb:
      if reconnect and not pgdb.closed: return pgdb    # no need reconnect
   elif reconnect:
      reconnect = 0   # initial connection

   while True:
      config = {'database' : PGDBI['DBNAME'],
                    'user' : PGDBI['LNNAME']}
      if PGDBI['DBSHOST'] == PgLOG.PGLOG['HOSTNAME']:
         config['host'] = 'localhost'
      else:
         config['host'] = PGDBI['DBHOST'] if PGDBI['DBHOST'] else PGDBI['DEFHOST']
         if not PGDBI['DBPORT']: PGDBI['DBPORT'] = get_dbport(PGDBI['DBNAME'])
      if PGDBI['DBPORT']: config['port'] = PGDBI['DBPORT'] 
      config['password'] = '***'
      sqlstr = "psycopg2.connect(**{})".format(config)
      config['password'] = get_pgpass_password()
      if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, sqlstr)
      try:
         PgLOG.PGLOG['PGDBBUF'] = pgdb = PgSQL.connect(**config)
         if reconnect: PgLOG.pglog("{} Reconnected at {}".format(sqlstr, PgLOG.current_datetime()), PgLOG.MSGLOG|PgLOG.FRCLOG)
         if autocommit: pgdb.autocommit = autocommit
         return pgdb
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, None, PGDBI['EXITLG']): return PgLOG.FAILURE 
         pgcnt += 1

#
# return a PostgreSQL cursor upon success
#
def pgcursor():

   global pgdb
   pgcur = None

   if not pgdb:
      pgconnect()
      if not pgdb: return PgLOG.FAILURE

   pgcnt = 0
   while True:
      try:
         pgcur = pgdb.cursor()
         spath = "SET search_path = '{}'".format(PGDBI['SCNAME'])
         if PGDBI['SCPATH'] and PGDBI['SCPATH'] != PGDBI['SCNAME']:
            spath += ", '{}'".format(PGDBI['SCPATH'])
         pgcur.execute(spath)
      except PgSQL.Error as pgerr:
         if pgcnt == 0 and pgdb.closed:
            pgconnect(1)
         elif not check_dberror(pgerr, pgcnt, '', None, PGDBI['EXITLG']):
            return PgLOG.FAILURE 
      else:
         break
      pgcnt += 1

   return pgcur

#
# disconnect to dssdb database
#
def pgdisconnect(stopit = 1):

   global pgdb
   if pgdb:
      if stopit: pgdb.close()
      PgLOG.PGLOG['PGDBBUF'] = pgdb = None

#
# gather table field default information as hash array with field names as keys
# and default values as values
# the whole table information is cached to a hash array with table names as keys
#
def pgtable(tablename, logact = PGDBI['ERRLOG']):

   if tablename in TABLES: return TABLES[tablename].copy()  # cached already
   intms = r'^(smallint||bigint|integer)$'
   fields = "column_name col, data_type typ, is_nullable nil, column_default def"
   condition = table_condition(tablename)
   pgcnt = 0
   while True:
      pgrecs = pgmget('information_schema.columns', fields, condition, logact)
      cnt = len(pgrecs['col']) if pgrecs else 0
      if cnt: break
      if pgcnt == 0 and logact&PgLOG.ADDTBL:
         add_new_table(tablename, logact = logact)
      else:
         return PgLOG.pglog(tablename + ": Table not exists", logact)
      pgcnt += 1

   pgdefs = {}
   for i in range(cnt):
      name = pgrecs['col'][i]
      isint = re.match(intms, pgrecs['typ'][i])
      dflt = pgrecs['def'][i]
      if dflt != None:
         if re.match(r'^nextval\(', dflt):
            dflt = 0
         else:
            dflt = check_default_value(dflt, isint)
      elif pgrecs['nil'][i] == 'YES':
         dflt = None
      elif isint:
         dflt = 0
      else:
         dflt = ''
      pgdefs[name] = dflt

   TABLES[tablename] = pgdefs.copy()
   return pgdefs

#
# get sequence field name for given table name
#
def pgsequence(tablename, logact = PGDBI['ERRLOG']):

   if tablename in SEQUENCES: return SEQUENCES[tablename]  # cached already
   condition = table_condition(tablename) + " AND column_default LIKE 'nextval(%'"
   pgrec = pgget('information_schema.columns', 'column_name', condition, logact)
   seqname = pgrec['column_name'] if pgrec else None
   SEQUENCES[tablename] = seqname
   
   return seqname

#
# check default value for integer & string
#
def check_default_value(dflt, isint):

   if isint:
      ms = re.match(r"^'{0,1}(\d+)", dflt)
      if ms: dflt = int(ms.group(1))
   elif dflt[0] == "'":
      ms = re.match(r"^(.+)::", dflt)
      if ms: dflt = ms.group(1)
   elif dflt != 'NULL':
      dflt = "'{}'".format(dflt)
   return dflt

#
# local fucntion: insert prepare pgadd()/pgmadd() for given table and field names
# according to options of multiple place holds and returning sequence id  
#
def prepare_insert(tablename, fields, multi = True, getid = None):

   strfld = pgnames(fields, '.', ',')
   if multi:
      strplc = "(" + ','.join(['%s']*len(fields)) + ")"
   else:
      strplc = '%s'
   sqlstr = "INSERT INTO {} ({}) VALUES {}".format(tablename, strfld, strplc)
   if getid: sqlstr += " RETURNING " + getid

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, sqlstr)

   return sqlstr

#
# local fucntion: prepare default value for single record
#
def prepare_default(tablename, record, logact = 0):

   table = pgtable(tablename, logact)

   for fld in record:
      val = record[fld]
      if val is None:
         vlen = 0
      elif isinstance(val, str):
         vlen = len(val)
      else:
         vlen = 1
      if vlen == 0: record[fld] = table[fld]

#
# local fucntion: prepare default value for multiple records
#
def prepare_defaults(tablename, records, logact = 0):

   table = pgtable(tablename, logact)

   for fld in records:
      vals = records[fld]
      vcnt = len(vals)
      for i in range(vcnt):
         if vals[i] is None:
            vlen = 0
         elif isinstance(vals[i], str):
            vlen = len(vals[i])
         else:
            vlen = 1
         if vlen == 0: records[fld][i] = table[fld]

#
# insert one record into tablename
# tablename: add record for one table name each call
#    record: hash reference with keys as field names and hash values as field values
# return PgLOG.SUCCESS or PgLOG.FAILURE
#
def pgadd(tablename, record, logact = PGDBI['ERRLOG'], getid = None):

   global curtran
   if not record: return PgLOG.pglog("Nothing adds to " + tablename, logact)
   if logact&PgLOG.DODFLT: prepare_default(tablename, record, logact)
   if logact&PgLOG.AUTOID and not getid: getid = pgsequence(tablename, logact) 
   sqlstr = prepare_insert(tablename, list(record), True, getid)
   values = tuple(record.values())

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "Insert: " + str(values))

   ret = acnt = pgcnt = 0
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      try:
         pgcur.execute(sqlstr, values)
         acnt = 1
         if getid:
            ret = pgcur.fetchone()[0]
         else:
            ret = PgLOG.SUCCESS
         pgcur.close()
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, values, logact): return PgLOG.FAILURE
      else:
         break
      pgcnt += 1

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "pgadd: 1 record added to " + tablename + ", return " + str(ret))
   if(logact&PgLOG.ENDLCK):
      endtran()
   elif curtran:
      curtran += acnt
      if curtran > PGDBI['MTRANS']: starttran()

   return ret

#
# insert multiple records into tablename
# tablename: add records for one table name each call
#   records: dict with field names as keys and each value is a list of field values
#  return PgLOG.SUCCESS or PgLOG.FAILURE
#
def pgmadd(tablename, records, logact = PGDBI['ERRLOG'], getid = None):

   global curtran
   if not records: return PgLOG.pglog("Nothing to insert to table " + tablename, logact)
   if logact&PgLOG.DODFLT: prepare_defaults(tablename, records, logact)
   if logact&PgLOG.AUTOID and not getid: getid = pgsequence(tablename, logact)
   multi = True if getid else False
   sqlstr = prepare_insert(tablename, list(records), multi, getid)

   v = records.values()
   values = list(zip(*v))
   cntrow = len(values)
   ids = [] if getid else None

   if PgLOG.PGLOG['DBGLEVEL']:
      for row in values: PgLOG.pgdbg(1000, "Insert: " + str(row))

   count = pgcnt = 0
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE

      if getid:
         while count < cntrow:
            record = values[count]
            try:
               pgcur.execute(sqlstr, record)
               ids.append(pgcur.fetchone()[0])
               count += 1
            except PgSQL.Error as pgerr:
               if not check_dberror(pgerr, pgcnt, sqlstr, record, logact): return PgLOG.FAILURE
               break
      else:
         try:
            execute_values(pgcur, sqlstr, values, page_size=PGDBI['PGSIZE'])
            count = cntrow
         except PgSQL.Error as pgerr:
            if not check_dberror(pgerr, pgcnt, sqlstr, values[0], logact): return PgLOG.FAILURE
      if count >= cntrow: break
      pgcnt += 1

   pgcur.close()
   if(PgLOG.PGLOG['DBGLEVEL']): PgLOG.pgdbg(1000, "pgmadd: {} of {} record(s) added to {}".format(count, cntrow, tablename))

   if(logact&PgLOG.ENDLCK):
      endtran()
   elif curtran:
      curtran += count
      if curtran > PGDBI['MTRANS']: starttran()

   return (ids if ids else count)

#
# local function: select prepare for pgget() and pgmget()
#
def prepare_select(tablenames, fields = None, condition = None, cndflds = None, logact = 0):

   sqlstr = ''   
   if tablenames:
      if fields:
         sqlstr = "SELECT " + fields
      else:
         sqlstr = "SELECT count(*) cntrec"

      sqlstr += " FROM " + tablenames
      if condition:
         if re.match(r'^\s*(ORDER|GROUP|HAVING|OFFSET|LIMIT)\s', condition, re.I):
            sqlstr += " " + condition      # no where clause, append directly
         else:
            sqlstr += " WHERE " + condition
      elif cndflds:
         sep = 'WHERE'
         for fld in cndflds:
            sqlstr += " {} {}=%s".format(sep, fld)
            sep = 'AND'
      if logact&PgLOG.DOLOCK:
         starttran()
         sqlstr += " FOR UPDATE"
   elif fields:
      sqlstr = "SELECT " + fields
   elif condition:
      sqlstr = condition

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, sqlstr)

   return sqlstr

#
# tablenames: comma deliminated string of one or more tables and more than one table for joining,
#     fields: comma deliminated string of one or more field names,
#  condition: querry conditions for where clause
# return a dict reference with keys as field names upon success 
#
def pgget(tablenames, fields, condition = None, logact = 0):

   if not logact: logact = PGDBI['ERRLOG']
   if fields and condition and not re.search(r'limit 1$', condition, re.I): condition += " LIMIT 1"
   sqlstr = prepare_select(tablenames, fields, condition, None, logact)
   if fields and not re.search(r'(^|\s)limit 1($|\s)', sqlstr, re.I): sqlstr += " LIMIT 1"
   ucname = True if logact&PgLOG.UCNAME else False
   pgcnt = 0
   record = {}
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      try:
         pgcur.execute(sqlstr)
         vals = pgcur.fetchone()
         if vals:
            colcnt = len(pgcur.description)
            for i in range(colcnt):
               col = pgcur.description[i]
               colname = col[0].upper() if ucname else col[0]
               val = vals[i]
               if col[1] == CHCODE and val and val[-1] == ' ': val = val.rstrip()
               record[colname] = val
         pgcur.close()
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, None, logact): return PgLOG.FAILURE 
      else:
         break
      pgcnt += 1

   if record and tablenames and not fields:
      if PgLOG.PGLOG['DBGLEVEL']:
         PgLOG.pgdbg(1000, "pgget: {} record(s) found from {}".format(record['cntrec'], tablenames))
      return record['cntrec']
   elif PgLOG.PGLOG['DBGLEVEL']:
      cnt = 1 if record else 0
      PgLOG.pgdbg(1000, "pgget: {} record retrieved from {}".format(cnt, tablenames))

   return record

#
# tablenames: comma deliminated string of one or more tables and more than one table for joining,
#     fields: comma deliminated string of one or more field names,
#  condition: querry conditions for where clause
# return a dict reference with keys as field names upon success, values for each field name
#        are in a list. All lists are the same length with missing values set to None
#
def pgmget(tablenames, fields, condition = None, logact = PGDBI['ERRLOG']):

   sqlstr = prepare_select(tablenames, fields, condition, None, logact)
   ucname = True if logact&PgLOG.UCNAME else False
   count = pgcnt = 0
   records = {}
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      try:
         pgcur.execute(sqlstr)
         rowvals = pgcur.fetchall()
         if rowvals:
            colcnt = len(pgcur.description)
            count = len(rowvals)
            colvals = list(zip(*rowvals))
            for i in range(colcnt):
               col = pgcur.description[i]
               colname = col[0].upper() if ucname else col[0]
               vals = list(colvals[i])
               if col[1] == CHCODE:
                  for j in range(count):
                     if vals[j] and vals[j][-1] == ' ': vals[j] = vals[j].rstrip()
               records[colname] = vals
         pgcur.close()
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, None, logact): return PgLOG.FAILURE 
      else:
         break
      pgcnt += 1

   if PgLOG.PGLOG['DBGLEVEL']:
      PgLOG.pgdbg(1000, "pgmget: {} record(s) retrieved from {}".format(count, tablenames))

   return records

#
# tablenames: comma deliminated string of one or more tables
#     fields: comma deliminated string of one or more field names,
#    cnddict: condition dict with field names : values
# return a dict(field names : values) upon success 
#
# retrieve one records from tablenames condition dict
#
def pghget(tablenames, fields, cnddict, logact = PGDBI['ERRLOG']):

   if not tablenames: return PgLOG.pglog("Miss Table name to query", logact)
   if not fields: return PgLOG.pglog("Nothing to query " + tablenames, logact)
   if not cnddict: return PgLOG.pglog("Miss condition dict values to query " + tablenames, logact)
   sqlstr = prepare_select(tablenames, fields, None, list(cnddict), logact)
   if fields and not re.search(r'limit 1$', sqlstr, re.I): sqlstr += " LIMIT 1"
   ucname = True if logact&PgLOG.UCNAME else False

   values = tuple(cnddict.values())
   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "Query from {} for {}".format(tablenames, values))

   pgcnt = 0
   record = {}
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      try:
         pgcur.execute(sqlstr, values)
         vals = pgcur.fetchone()
         if vals:
            colcnt = len(pgcur.description)
            for i in range(colcnt):
               col = pgcur.description[i]
               colname = col[0].upper() if ucname else col[0]
               val = vals[i]
               if col[1] == CHCODE and val and val[-1] == ' ': val = val.rstrip()
               record[colname] = val
         pgcur.close()
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, values, logact): return PgLOG.FAILURE 
      else:
         break
      pgcnt += 1

   if record and tablenames and not fields:
      if PgLOG.PGLOG['DBGLEVEL']:
         PgLOG.pgdbg(1000, "pghget: {} record(s) found from {}".format(record['cntrec'], tablenames))
      return record['cntrec']
   elif PgLOG.PGLOG['DBGLEVEL']:
      cnt = 1 if record else 0
      PgLOG.pgdbg(1000, "pghget: {} record retrieved from {}".format(cnt, tablenames))

   return record

#
# tablenames: comma deliminated string of one or more tables
#     fields: comma deliminated string of one or more field names,
#   cnddicts: condition dict with field names : value lists
# return a dict(field names : value lists) upon success 
#
# retrieve multiple records from tablenames for condition dict
#
def pgmhget(tablenames, fields, cnddicts, logact = PGDBI['ERRLOG']):

   if not tablenames: return PgLOG.pglog("Miss Table name to query", logact)
   if not fields: return PgLOG.pglog("Nothing to query " + tablenames, logact)
   if not cnddicts: return PgLOG.pglog("Miss condition dict values to query " + tablenames, logact)
   sqlstr = prepare_select(tablenames, fields, None, list(cnddicts), logact)
   ucname = True if logact&PgLOG.UCNAME else False

   v = cnddicts.values()
   values = list(zip(*v))
   cndcnt = len(values)

   if PgLOG.PGLOG['DBGLEVEL']:
      for row in values:
         PgLOG.pgdbg(1000, "Query from {} for {}".format(tablenames, row))

   colcnt = ccnt = count = pgcnt = 0
   cols = []
   chrs = []
   records = {}
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      while ccnt < cndcnt:
         cndvals = values[ccnt]
         try:
            pgcur.execute(sqlstr, cndvals)
            ccnt += 1
            rowvals = pgcur.fetchall()
            if rowvals:
               if colcnt == 0:
                  for col in pgcur.description:
                     colname = col[0].upper() if ucname else col[0]
                     if col[1] == CHCODE: chrs.append(colname)
                     cols.append(colname)
                     records[colname] = []
                  colcnt = len(cols)
               rcnt = len(rowvals)
               count += rcnt
               colvals = list(zip(*rowvals))
               for i in range(colcnt):
                  vals = list(colvals[i])
                  colname = cols[i]
                  if chrs and colname in chrs:
                     for j in range(rcnt):
                        if vals[j] and vals[j][-1] == ' ': vals[j] = vals[j].rstrip()
                  records[colname].extend(vals)
         except PgSQL.Error as pgerr:
            if not check_dberror(pgerr, pgcnt, sqlstr, cndvals, logact): return PgLOG.FAILURE
            break
      if ccnt >= cndcnt: break
      pgcnt += 1
   pgcur.close()

   if PgLOG.PGLOG['DBGLEVEL']:
      PgLOG.pgdbg(1000, "pgmhget: {} record(s) retrieved from {}".format(count, tablenames))

   return records

#
# local fucntion: update prepare for pgupdt, pghupdt and pgmupdt
#
def prepare_update(tablename, fields, condition = None, cndflds = None):

   strset = []
   # build set string
   for fld in fields:
      strset.append("{}=%s".format(pgname(fld, '.')))
   strflds = ",".join(strset)

   # build condition string
   if not condition:
      cndset = []
      for fld in cndflds:
         cndset.append("{}=%s".format(pgname(fld, '.')))
      condition = " AND ".join(cndset)
 
   sqlstr = "UPDATE {} SET {} WHERE {}".format(tablename, strflds, condition)
   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, sqlstr)

   return sqlstr

#
# update one or multiple rows in tablename
# tablename: update for one table name each call
#    record: dict with field names : values
# condition: update conditions for where clause)
# return number of rows undated upon success
#
def pgupdt(tablename, record, condition, logact = PGDBI['ERRLOG']):

   global curtran
   if not record: PgLOG.pglog("Nothing updates to " + tablename, logact)
   if not condition or isinstance(condition, int): PgLOG.pglog("Miss condition to update " + tablename, logact)
   sqlstr = prepare_update(tablename, list(record), condition)
   if logact&PgLOG.DODFLT: prepare_default(tablename, record, logact)

   values = tuple(record.values())
   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "Update {} for {}".format(tablename, values))

   ucnt = pgcnt = 0
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      try:
         pgcur.execute(sqlstr, values)
         ucnt = pgcur.rowcount
         pgcur.close()
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, values, logact): return PgLOG.FAILURE 
      else:
         break
      pgcnt += 1

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "pgupdt: {} record(s) updated to {}".format(ucnt, tablename))
   if(logact&PgLOG.ENDLCK):
      endtran()
   elif curtran:
      curtran += ucnt
      if curtran > PGDBI['MTRANS']: starttran()

   return ucnt

#
# update one or multiple records in tablename
# tablename: update for one table name each call
#    record: update values, dict with field names : values
#   cnddict: condition dict with field names : values
# return number of records updated upon success
#
def pghupdt(tablename, record, cnddict, logact = PGDBI['ERRLOG']):

   global curtran
   if not record: PgLOG.pglog("Nothing updates to " + tablename, logact)
   if not cnddict or isinstance(cnddict, int): PgLOG.pglog("Miss condition to update to " + tablename, logact)
   if logact&PgLOG.DODFLT: prepare_defaults(tablename, record, logact)
   sqlstr = prepare_update(tablename, list(record), None, list(cnddict))

   values = tuple(record.values()) + tuple(cnddict.values())

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "Update {} for {}".format(tablename, values))

   ucnt = count = pgcnt = 0
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      try:
         pgcur.execute(sqlstr, values)
         count += 1
         ucnt = pgcur.rowcount
         pgcur.close()
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, values, logact): return PgLOG.FAILURE
      else:
         break
      pgcnt += 1

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "pghupdt: {}/{} record(s) updated to {}".format(ucnt, tablename))
   if(logact&PgLOG.ENDLCK):
      endtran()
   elif curtran:
      curtran += ucnt
      if curtran > PGDBI['MTRANS']: starttran()

   return ucnt

#
# update multiple records in tablename
# tablename: update for one table name each call
#   records: update values, dict with field names : value lists
#   cnddicts: condition dict with field names : value lists
# return number of records updated upon success
#
def pgmupdt(tablename, records, cnddicts, logact = PGDBI['ERRLOG']):

   global curtran
   if not records: PgLOG.pglog("Nothing updates to " + tablename, logact)
   if not cnddicts or isinstance(cnddicts, int): PgLOG.pglog("Miss condition to update to " + tablename, logact)
   if logact&PgLOG.DODFLT: prepare_defaults(tablename, records, logact)
   sqlstr = prepare_update(tablename, list(records), None, list(cnddicts))

   fldvals = tuple(records.values())
   cntrow = len(fldvals[0])
   cndvals = tuple(cnddicts.values())
   cntcnd = len(cndvals[0])
   if cntcnd != cntrow: return PgLOG.pglog("Field/Condition value counts Miss match {}/{} to update {}".format(cntrow, cntcnd, tablename), logact)
   v = fldvals + cndvals
   values = list(zip(*v))

   if PgLOG.PGLOG['DBGLEVEL']:
      for row in values: PgLOG.pgdbg(1000, "Update {} for {}".format(tablename, row))

   ucnt = pgcnt = 0
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      try:
         execute_batch(pgcur, sqlstr, values, page_size=PGDBI['PGSIZE'])
         ucnt = cntrow
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, values[0], logact): return PgLOG.FAILURE 
      else:
         break
      pgcnt += 1

   pgcur.close()

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "pgmupdt: {} record(s) updated to {}".format(ucnt, tablename))
   if(logact&PgLOG.ENDLCK):
      endtran()
   elif curtran:
      curtran += ucnt
      if curtran > PGDBI['MTRANS']: starttran()

   return ucnt

#
# local fucntion: delete prepare for pgdel, pghdel and del
#
def prepare_delete(tablename, condition = None, cndflds = None):

   # build condition string
   if not condition:
      cndset = []
      for fld in cndflds:
         cndset.append("{}=%s".format(fld))
      condition = " AND ".join(cndset)
 
   sqlstr = "DELETE FROM {} WHERE {}".format(tablename, condition)
   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, sqlstr)

   return sqlstr

#
# delete one or mutiple records in tablename according condition
# tablename: delete for one table name each call
# condition: delete conditions for where clause
# return number of records deleted upon success
#
def pgdel(tablename, condition, logact = PGDBI['ERRLOG']):

   global curtran
   if not condition or isinstance(condition, int): PgLOG.pglog("Miss condition to delete from " + tablename, logact)
   sqlstr = prepare_delete(tablename, condition)

   dcnt = pgcnt = 0
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      try:
         pgcur.execute(sqlstr)
         dcnt = pgcur.rowcount
         pgcur.close()
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, None, logact): return PgLOG.FAILURE 
      else:
         break
      pgcnt += 1

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "pgdel: {} record(s) deleted from {}".format(dcnt, tablename))
   if logact&PgLOG.ENDLCK:
      endtran()
   elif curtran:
      curtran += dcnt
      if curtran > PGDBI['MTRANS']: starttran()

   return dcnt

#
# delete one or mutiple records in tablename according condition
# tablename: delete for one table name each call
#    cndict: delete condition dict for names : values
# return number of records deleted upon success
#
def pghdel(tablename, cnddict, logact = PGDBI['ERRLOG']):

   global curtran
   if not cnddict or isinstance(cnddict, int): PgLOG.pglog("Miss condition dict to delete from " + tablename, logact)
   sqlstr = prepare_delete(tablename, None, list(cnddict))

   values = tuple(cnddict.values())
   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "Delete from {} for {}".format(tablename, values))

   dcnt = pgcnt = 0
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      try:
         pgcur.execute(sqlstr, values)
         dcnt = pgcur.rowcount
         pgcur.close()
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, values, logact): return PgLOG.FAILURE 
      else:
         break
      pgcnt += 1

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "pghdel: {} record(s) deleted from {}".format(dcnt, tablename))
   if logact&PgLOG.ENDLCK:
      endtran()
   elif curtran:
      curtran += dcnt
      if curtran > PGDBI['MTRANS']: starttran()

   return dcnt

#
# delete mutiple records in tablename according condition
# tablename: delete for one table name each call
#   cndicts: delete condition dict for names : value lists
# return number of records deleted upon success
#
def pgmdel(tablename, cnddicts, logact = PGDBI['ERRLOG']):

   global curtran
   if not cnddicts or isinstance(cnddicts, int): PgLOG.pglog("Miss condition dict to delete from " + tablename, logact)
   sqlstr = prepare_delete(tablename, None, list(cnddicts))

   v = cnddicts.values()
   values = list(zip(*v))
   if PgLOG.PGLOG['DBGLEVEL']:
      for row in values:
         PgLOG.pgdbg(1000, "Delete from {} for {}".format(tablename, row))

   dcnt = pgcnt = 0
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      try:
         execute_batch(pgcur, sqlstr, values, page_size=PGDBI['PGSIZE'])
         dcnt = len(values)
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, values[0], logact): return PgLOG.FAILURE 
      else:
         break
      pgcnt += 1

   pgcur.close()

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "pgmdel: {} record(s) deleted from {}".format(dcnt, tablename))
   if logact&PgLOG.ENDLCK:
      endtran()
   elif curtran:
      curtran += dcnt
      if curtran > PGDBI['MTRANS']: starttran()

   return dcnt

#
# sqlstr: a complete sql string
# return number of record affected upon success
#
def pgexec(sqlstr, logact = PGDBI['ERRLOG']):

   global curtran
   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(100, sqlstr)

   ret = pgcnt = 0
   while True:
      pgcur = pgcursor()
      if not pgcur: return PgLOG.FAILURE
      try:
         pgcur.execute(sqlstr)
         ret = pgcur.rowcount
         pgcur.close()
      except PgSQL.Error as pgerr:
         if not check_dberror(pgerr, pgcnt, sqlstr, None, logact): return PgLOG.FAILURE 
      else:
         break
      pgcnt += 1

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, "pgexec: {} record(s) affected for {}".format(ret, sqlstr))
   if logact&PgLOG.ENDLCK:
      endtran()
   elif curtran:
      curtran += ret
      if curtran > PGDBI['MTRANS']: starttran()

   return ret

#
# tablename: one table name to a temporary table
# fromtable: table name data gathing from
#    fields: table name data gathing from
# condition: querry conditions for where clause
# return number of records created upon success
#
def pgtemp(tablename, fromtable, fields, condition = None, logact = 0):

   sqlstr = "CREATE TEMPORARY TABLE {} SELECT {} FROM {}".format(tablename, fields, fromtable)
   if condition: sqlstr += " WHERE " + condition

   return pgexec(sqlstr, logact)

#
# get condition for given table name for accessing information_schema
#
def table_condition(tablename):

   ms = re.match(r'(.+)\.(.+)', tablename)
   if ms:
      scname = ms.group(1)
      tbname = ms.group(2)
   else:
      scname = PGDBI['SCNAME']
      tbname = tablename

   return "table_schema = '{}' AND table_name = '{}'".format(scname, tbname)

#
# check if a given table name exists or not
# tablename: one table name to check
#
def pgcheck(tablename, logact = 0):

   condition = table_condition(tablename)

   ret = pgget('information_schema.tables', None, condition, logact)
   return (PgLOG.SUCCESS if ret else PgLOG.FAILURE)

#
# group of functions to check parent records and add an empty one if missed 
# return user.uid upon success, 0 otherwise
#
def check_user_uid(userno, date = None):

   if not userno: return 0
   if type(userno) is str: userno = int(userno)
      
   if date is None:
      datecond = "until_date IS NULL"
      date = 'today'
   else:
      datecond = "(start_date IS NULL OR start_date <= '{}') AND (until_date IS NULL OR until_date >= '{}')".format(date, date)

   pgrec = pgget("dssdb.user", "uid", "userno = {} AND {}".format(userno, datecond), PGDBI['ERRLOG'])
   if pgrec: return pgrec['uid']

   if userno not in NMISSES:
      PgLOG.pglog("{}: Scientist ID NOT on file for {}".format(userno, date), PgLOG.LGWNEM)
      NMISSES.append(userno)

   # check again if a user is on file with different date range
   pgrec = pgget("dssdb.user", "uid", "userno = {}".format(userno), PGDBI['ERRLOG'])
   if pgrec: return pgrec['uid']

   pgrec = ucar_user_info(userno)
   if not pgrec: pgrec = {'userno' : userno, 'stat_flag' : 'M'}
   uid = pgadd("dssdb.user", pgrec, (PGDBI['EXITLG']|PgLOG.AUTOID))
   if uid: PgLOG.pglog("{}: Scientist ID Added as user.uid = {}".format(userno, uid), PgLOG.LGWNEM)

   return uid

#
# return user.uid upon success, 0 otherwise
#
def get_user_uid(logname, date = None):
   
   if not logname: return 0
   if not date:
      date = 'today'
      datecond = "until_date IS NULL"
   else:
      datecond = "(start_date IS NULL OR start_date <= '{}') AND (until_date IS NULL OR until_date >= '{}')".format(date, date)

   pgrec = pgget("dssdb.user", "uid", "logname = '{}' AND {}".format(logname, datecond), PGDBI['ERRLOG'])
   if pgrec: return pgrec['uid']
   
   if logname not in LMISSES:
      PgLOG.pglog("{}: UCAR Login Name NOT on file for {}".format(logname, date), PgLOG.LGWNEM)
      LMISSES.append(logname)

   # check again if a user is on file with different date range
   pgrec = pgget("dssdb.user", "uid", "logname = '{}'".format(logname), PGDBI['ERRLOG'])
   if pgrec: return pgrec['uid']

   pgrec = ucar_user_info(0, logname)
   if not pgrec: pgrec = {'logname' : logname, 'stat_flag' : 'M'}
   uid = pgadd("dssdb.user", pgrec, (PGDBI['EXITLG']|PgLOG.AUTOID))
   if uid: PgLOG.pglog("{}: UCAR Login Name Added as user.uid = {}".format(logname, uid), PgLOG.LGWNEM)

   return uid

#
# get ucar user info for given userno (scientist number) or logname (Ucar login)
#
def ucar_user_info(userno, logname = None):

   MATCH = {
      'upid' : "upid",
      'uid'  : "userno",
      'username' : "logname",
      'lastName' : "lstname",
      'firstName' : "fstname",
      'active' : "stat_flag",
      'internalOrg' : "division",
      'externalOrg' : "org_name",
      'country' : "country",
      'forwardEmail' : "email",
      'email' : "ucaremail",
      'phone' : "phoneno"
   }

   buf = PgLOG.pgsystem("pgperson " + ("-uid {}".format(userno) if userno else "-username {}".format(logname)), PgLOG.LOGWRN, 20)
   if not buf: return None

   pgrec = {}
   for line in buf.split('\n'):
      ms = re.match(r'^(.+)<=>(.*)$', line)
      if ms:
         (key, val) = ms.groups()
         if key in MATCH:
            if key == 'upid' and 'upid' in pgrec: break  # get one record only
            pgrec[MATCH[key]] = val

   if not pgrec: return None

   if userno:
      pgrec['userno'] = userno
   elif pgrec['userno']:
      pgrec['userno'] = userno = int(pgrec['userno'])
   if pgrec['upid']: pgrec['upid'] = int(pgrec['upid'])          
   if pgrec['stat_flag']: pgrec['stat_flag'] = 'A' if pgrec['stat_flag'] == '1' else 'C'
   if pgrec['email'] and re.search(r'(@|\.)ucar\.edu$', pgrec['email'], re.I):
      pgrec['email'] = pgrec['ucaremail']
      pgrec['org_name'] = 'NCAR'
   country = pgrec['country'] if 'country' in pgrec else None
   pgrec['country'] = set_country_code(pgrec['email'], country)
   if pgrec['division']:
      val = "NCAR"
   else:
      val = None
   pgrec['org_type'] = get_org_type(val, pgrec['email'])

   buf = PgLOG.pgsystem("pgusername {}".format(pgrec['logname']), PgLOG.LOGWRN, 20)
   if not buf: return pgrec

   for line in buf.split('\n'):
      ms = re.match(r'^(.+)<=>(.*)$', line)
      if ms:
         (key, val) = ms.groups()
         if key == 'startDate':
            m = re.match(r'^(\d+-\d+-\d+)\s', val)
            if m:
               pgrec['start_date'] = m.group(1)
            else:
               pgrec['start_date'] = val

         if key == 'endDate':
            m = re.match(r'^(\d+-\d+-\d+)\s', val)
            if m:
               pgrec['until_date'] = m.group(1)
            else:
               pgrec['until_date'] = val

   return pgrec

#
#  set country code for given coutry name or email address
#
def set_country_code(email, country = None):

   codes = {
      'CHINA'   : "P.R.CHINA",
      'ENGLAND' : "UNITED.KINGDOM",
      'FR'      : "FRANCE",
      'KOREA'   : "SOUTH.KOREA",
      'USSR'    : "RUSSIA",
      'US'      : "UNITED.STATES",
      'U.S.A.'  : "UNITED.STATES"
   }

   if country:
      country = country.upper()
      ms = re.match(r'^(\w+)\s(\w+)$', country)
      if ms:
         country = ms.group(1) + '.' + ms.group(2)
      elif country in codes:
         country = codes[country]
   else:
      country = email_to_country(email)

   return country

# return wuser.wuid upon success, 0 otherwise
def check_wuser_wuid(email, date = None):

   if not email: return 0
   emcond = "email = '{}'".format(email)
   if not date:
      date = 'today'
      datecond = "until_date IS NULL"
   else:
      datecond = "(start_date IS NULL OR start_date <= '{}') AND (until_date IS NULL OR until_date >= '{}')".format(date, date)

   pgrec = pgget("wuser", "wuid", "{} AND {}".format(emcond, datecond), PGDBI['ERRLOG'])
   if pgrec: return pgrec['wuid']

   # check again if a user is on file with different date range
   pgrec = pgget("wuser", "wuid", emcond, PgLOG.LOGERR)
   if pgrec: return pgrec['wuid']

   # now add one in
   record = {'email' : email} 
   # check again if a ruser is on file
   pgrec = pgget("ruser", "*", emcond + " AND end_date IS NULL", PGDBI['ERRLOG'])
   if not pgrec: pgrec = pgget("ruser", "*", emcond, PGDBI['ERRLOG'])

   if pgrec:
      record['ruid'] = pgrec['id']
      record['fstname'] = pgrec['fname']
      record['lstname'] = pgrec['lname']
      record['country'] = pgrec['country']
      record['org_type'] = get_org_type(pgrec['org_type'], pgrec['email'])
      record['start_date'] = str(pgrec['rdate'])
      if pgrec['end_date']:
         record['until_date'] = str(pgrec['end_date'])
         record['stat_flag'] = 'C'
      else:
         record['stat_flag'] = 'A'

      if pgrec['title']: record['utitle'] = pgrec['title']
      if pgrec['mname']: record['midinit'] = pgrec['mname'][0]
      if pgrec['org']: record['org_name'] = pgrec['org']
   else:
      record['stat_flag'] = 'M'
      record['org_type'] = get_org_type('', email)
      record['country'] = email_to_country(email)

   wuid = pgadd("wuser", record, PgLOG.LOGERR|PgLOG.AUTOID)
   if wuid:
      if pgrec:
         PgLOG.pglog("{}({}, {}) Added as wuid({})".format(email, pgrec['lname'], pgrec['fname'], wuid), PgLOG.LGWNEM)
      else:
         PgLOG.pglog("{} Added as wuid({})".format(email, wuid), PgLOG.LGWNEM)
      return wuid

   return 0

# return wuser.wuid upon success, 0 otherwise
def check_cdp_wuser(username):

   pgrec = pgget("wuser", "wuid", "cdpname = '{}'".format(username), PGDBI['EXITLG'])
   if pgrec: return pgrec['wuid']
   
   idrec = pgget("wuser", "wuid", "email = '{}'".format(pgrec['email']), PGDBI['EXITLG'])
   wuid = idrec['wuid'] if idrec else 0
   if wuid > 0:
      idrec = {}
      idrec['cdpid'] = pgrec['cdpid']
      idrec['cdpname'] = pgrec['cdpname']
      pgupdt("wuser", idrec, "wuid = {}".format(wuid) , PGDBI['EXITLG'])
   else:
      pgrec['stat_flag'] = 'A'
      pgrec['org_type'] = get_org_type(pgrec['org_type'], pgrec['email'])
      pgrec['country'] = email_to_country(pgrec['email'])
      wuid = pgadd("wuser", pgrec, PGDBI['EXITLG']|PgLOG.AUTOID)
      if wuid > 0:
         PgLOG.pglog("CDP User {} added as wuid = {} in RDADB".format(username, wuid), PgLOG.LGWNEM)

   return wuid

#
# for given email to get long country name
#
def email_to_country(email):
   
   ms = re.search(r'\.(\w\w)$', email)
   if ms:
      pgrec = pgget("countries", "token", "domain_id = '{}'".format(ms.group(1)), PGDBI['EXITLG'])
      if pgrec: return pgrec['token']
   elif re.search(r'\.(gov|edu|mil|org|com|net)$', email):
      return "UNITED.STATES"
   else:
      return "UNKNOWN"

#
# if filelists is published for given dataset, reset it to 'P'
#
def reset_rdadb_version(dsid):

   pgexec("UPDATE dataset SET version = version + 1 WHERE dsid = '{}'".format(dsid), PGDBI['ERRLOG'])

#
# check the use rdadb flag in table dataset for a given dataset and given values
#
def use_rdadb(dsid, logact = 0, vals = None):

   ret = ''   # default to empty in case dataset not in RDADB
   if dsid:
      pgrec = pgget("dataset", "use_rdadb", "dsid = '{}'".format(dsid), PGDBI['EXITLG'])
      if pgrec:
         ret = 'N'   # default to 'N' if dataset record in RDADB already
         if pgrec['use_rdadb']:
            if not vals: vals = "IPYMW"  # default to Internal; Publishable; Yes RDADB
            if vals.find(pgrec['use_rdadb']) > -1:
               ret = pgrec['use_rdadb']
      elif logact:
         PgLOG.pglog("Dataset '{}' is not in RDADB!".format(dsid), logact)

   return ret

#
#   fld: field name for querry condition
#  vals: reference to aaray of values
# isstr: 1 for string values requires quotes and support wildcard
# noand: 1 for skiping the leading ' AND ' for condition
# return a condition string for a given field
#
def get_field_condition(fld, vals, isstr = 0, noand = 0):
   
   cnd = wcnd = negative = ''
   sign = "="
   logic = " OR "
   count =  len(vals) if vals else 0
   if count == 0: return ''
   ncnt = scnt = wcnt = cnt = 0
   for i in range(count):
      val = vals[i]
      if val is None or (i > 0 and val == vals[i-1]): continue      
      if i == 0 and val == PGSIGNS[0]:
         negative = "NOT "
         logic = " AND "
         continue
      if scnt == 0 and isinstance(val, str):
         ms = re.match(r'^({})$'.format('|'.join(PGSIGNS[1:])), val)
         if ms:
            osign = sign = ms.group(1)
            scnt += 1
            if sign == "<>":
               scnt += 1
               sign = negative + "BETWEEN"
            elif negative:
               sign = "<=" if (sign == ">") else ">="
            continue
      if isstr:
         if not isinstance(val, str): val = str(val)
         if sign == "=":
            if not val:
               ncnt += 1   # found null string
            elif val.find('%') > -1:
               sign = negative + "LIKE"
            elif re.search(r'[\[\(\?\.]', val):
               sign = negative + "SIMILAR TO"
         if val.find("'") != 0:
            val = "'{}'".format(val)
      elif isinstance(val, str):
         if val.find('.') > -1:
            val = float(val)
         else:
            val = int(val)
      if sign == "=":
         if cnt > 0: cnd += ", "
         cnd += str(val)
         cnt += 1
      else:
         if sign == "AND":
            wcnd += " {} {}".format(sign, val)
         else:
            if wcnt > 0: wcnd += logic
            wcnd += "{} {} {}".format(fld, sign, val)
            wcnt += 1
         if re.search(r'BETWEEN$', sign):
            sign = "AND"
         else:
            sign = "="
            scnt = 0

   if scnt > 0:
      s = 's' if scnt > 1 else ''
      PgLOG.pglog("Need {} value{} after sign '{}'".format(scnt, s, osign), PgLOG.LGEREX)
   if wcnt > 1: wcnd = "({})".format(wcnd)
   if cnt > 0:
      if cnt > 1:
         cnd = "{} {}IN ({})".format(fld, negative, cnd)
      else:   
         cnd = "{} {} {}".format(fld, ("<>" if negative else "="), cnd)
      if ncnt > 0:
         ncnd = "{} IS {}NULL".format(fld, negative)
         cnd = "({}{}{})".format(cnd, logic, ncnd)
      if wcnt > 0: cnd = "({}{}{})".format(cnd, logic, wcnd)
   elif wcnt > 0:
      cnd = wcnd
   if cnd and not noand: cnd = " AND " + cnd

   return cnd

#
# build up fieldname string for given or default condition
#
def fieldname_string(fnames, dnames = None, anames = None, wflds = None):

   if not fnames:
      fnames = dnames   # include default fields names
   elif re.match(r'^all$', fnames, re.I):
      fnames = anames   # include all field names

   if not wflds: return fnames
   
   for wfld in wflds:
      if not wfld or fnames.find(wfld) > -1: continue  # empty field, or included already
      if wfld == "Q":
         pos = fnames.find("R")   # request name
      elif wfld == "Y":
         pos = fnames.find("X")   # parent group name
      elif wfld == "G":
         pos = fnames.find("I")   # group name
      else:
         pos = -1   # prepend other with-field names

      if pos == -1:
         fnames = wfld + fnames   # prepend with-field
      else:
         fnames = fnames[0:pos] + wfld + fnames[pos:]   # insert with-field

   return fnames

#
# Function get_group_field_path(gindex: group index
#                                 dsid: dataset id
#                                field: path field name: webpath or savedpath)
# go through group tree upward to find a none-empty path, return it or null
# 
def get_group_field_path(gindex, dsid, field):

   if gindex:
      pgrec = pgget("dsgroup", "pindex, {}".format(field),
                     "dsid = '{}' AND gindex = {}".format(dsid, gindex), PGDBI['EXITLG'])
   else:
      pgrec = pgget("dataset", field,
                     "dsid = '{}'".format(dsid), PGDBI['EXITLG'])
   if pgrec:
      if pgrec[field]:
         return pgrec[field]
      elif gindex:
         return get_group_field_path(pgrec['pindex'], dsid, field)
   else:
      return None

#
# get the specialist info for a given dataset
#
def get_specialist(dsid, logact = PGDBI['ERRLOG']):

   if dsid in SPECIALIST: return SPECIALIST['dsid']

   pgrec = pgget("dsowner, dssgrp", "specialist, lstname, fstname",
                 "specialist = logname AND dsid = '{}' AND priority = 1".format(dsid), logact)
   if pgrec:
      if pgrec['specialist'] == "datahelp" or pgrec['specialist'] == "dss":
         pgrec['lstname'] = "Help"
         pgrec['fstname'] = "Data"
   else:
      pgrec['specialist'] = "datahelp"
      pgrec['lstname'] = "Help"
      pgrec['fstname'] = "Data"

   SPECIALIST['dsid'] = pgrec  # cache specialist info for dsowner of dsid
   return pgrec

#
#  build customized email from get_email()
#
def build_customized_email(table, field, condition, subject, logact = 0):

   msg = PgLOG.get_email()
   
   if not msg: return PgLOG.FAILURE
   
   sender = PgLOG.PGLOG['CURUID'] + "@ucar.edu"
   receiver = PgLOG.PGLOG['EMLADDR'] if PgLOG.PGLOG['EMLADDR'] else (PgLOG.PGLOG['CURUID'] + "@ucar.edu")
   if receiver.find(sender) < 0: PgLOG.add_carbon_copy(sender, 1)
   ebuf = "From: {}\nTo: {}\n".format(sender, receiver)
   if PgLOG.PGLOG['CCDADDR']: ebuf += "Cc: {}\n".format(PgLOG.PGLOG['CCDADDR']) 
   if not subject: subject = "Message from {}-{}".format(PgLOG.PGLOG['HOSTNAME'], PgLOG.get_command())
   ebuf += "Subject: {}!\n\n{}\n".format(subject, msg)

   estat = cache_customized_email(table, field, condition, ebuf, logact)
   if estat and logact:
      PgLOG.pglog("Email {} cached to '{}.{}' for {}, Subject: {}".format(receiver, table, field, condition, subject), logact)

   return estat

#
# email: full user email address
#
# get user real name from table ruser for a given email address
# opts == 1 : include email
# opts == 2 : include org_type
# opts == 4 : include country
# opts == 8 : include valid_email
# opts == 16 : include org
#
def get_ruser_names(email, opts = 0, date = None):

   fields = "lname lstname, fname fstname"

   if opts&1: fields += ", email"
   if opts&2: fields += ", org_type"
   if opts&4: fields += ", country"
   if opts&8: fields += ", valid_email"
   if opts&16: fields += ", org"

   if date:
      datecond = "rdate <= '{}' AND (end_date IS NULL OR end_date >= '{}')".format(date, date)
   else:
      datecond = "end_date IS NULL"
      date = time.strftime("%Y-%m-%d", (time.gmtime() if PgLOG.PGLOG['GMTZ'] else time.localtime()))
   emcnd = "email = '{}'".format(email)
   pgrec = pgget("ruser", fields, "{} AND {}".format(emcnd, datecond), PgLOG.LGEREX)
   if not pgrec:   # missing user record add one in
      PgLOG.pglog("{}: email not in ruser for {}".format(email, date), PgLOG.LOGWRN)
      # check again if a user is on file with different date range
      pgrec = pgget("ruser", fields, emcnd, PgLOG.LGEREX)
      if not pgrec and pgget("dssdb.user", '', emcnd):
         fields = "lstname, fstname"
         if opts&1: fields += ", email"
         if opts&2: fields += ", org_type"
         if opts&4: fields += ", country"
         if opts&8: fields += ", email valid_email"
         if opts&16: fields += ", org_name org"
         pgrec = pgget("dssdb.user", fields, emcnd, PgLOG.LGEREX)

   if pgrec and pgrec['lstname']:
      pgrec['name'] = (pgrec['fstname'].capitalize() + ' ') if pgrec['fstname'] else ''
      pgrec['name'] += pgrec['lstname'].capitalize()
   else:
      if not pgrec: pgrec = {}
      pgrec['name'] = email.split('@')[0]
      if opts&1: pgrec['email'] = email

   return pgrec

#
# cache a customized email for sending it later
#
def cache_customized_email(table, field, condition, emlmsg, logact = 0):

   pgrec = {field: emlmsg}
   if pgupdt(table, pgrec, condition, logact|PgLOG.ERRLOG):
      if logact: PgLOG.pglog("Email cached to '{}.{}' for {}".format(table, field, condition), logact&(~PgLOG.EXITLG))
      return PgLOG.SUCCESS
   else:
      msg = "cache email to '{}.{}' for {}".format(table, field, condition)
      PgLOG.pglog("Error msg, try to send directly now", logact|PgLOG.ERRLOG)
      return PgLOG.send_customized_email(msg, emlmsg, logact)

#
# otype: user organization type
# email: user email address)
#
# return: orgonizaion type like DSS, NCAR, UNIV...
#
def get_org_type(otype, email):

   if not otype: otype = "OTHER"
   if email:
      ms = re.search(r'(@|\.)ucar\.edu$', email)
      if ms:
         mc = ms.group(1)
         if otype == 'UCAR' or otype == 'OTHER': otype = 'NCAR'
         if otype == 'NCAR' and mc == '@':
            ms = re.match(r'^(.+)@', email)
            if ms and pgget("dssgrp", "", "logname = '{}'".format(ms.group(1))): otype = 'DSS'
      else:
         ms = re.search(r'\.(mil|org|gov|edu|com|net)(\.\w\w|$)', email)
         if ms:
            otype = ms.group(1).upper()
            if otype == 'EDU': otype = "UNIV"

   return otype

#
# join values and handle the null values
#
def join_values(vstr, vals):

   if vstr:
      vstr += "\n"
   elif vstr is None:
      vstr = ''

   return "{}Value{}({})".format(vstr, ('s' if len(vals) > 1 else ''), ', '.join(map(str, vals)))

#
#  check table hostname to find the system down times. Cache the result for 10 minutes
#
def get_system_downs(hostname, logact = 0):

   curtime = int(time.time())
   newhost = 0

   if hostname not in SYSDOWN:
      SYSDOWN[hostname] = {}
      newhost = 1
   if newhost or (curtime - SYSDOWN[hostname]['chktime']) > 600:
      SYSDOWN[hostname]['chktime'] = curtime
      SYSDOWN[hostname]['start'] = 0
      SYSDOWN[hostname]['end'] = 0
      SYSDOWN[hostname]['active'] = 1
      SYSDOWN[hostname]['path'] = None

      pgrec = pgget('hostname', 'service, domain, downstart, downend',
                    "hostname = '{}'".format(hostname), logact)
      if pgrec:
         if pgrec['service'] == 'N':
            SYSDOWN[hostname]['start'] = curtime
            SYSDOWN[hostname]['active'] = 0
         else:
            start = int(datetime.timestamp(pgrec['downstart'])) if pgrec['downstart'] else 0
            end = int(datetime.timestamp(pgrec['downend'])) if pgrec['downend'] else 0
            if start > 0 and (end == 0 or end > curtime):
               SYSDOWN[hostname]['start'] = start
               SYSDOWN[hostname]['end'] = end
            if pgrec['service'] == 'S' and pgrec['domain'] and re.match(r'^/', pgrec['domain']):
               SYSDOWN[hostname]['path'] = pgrec['domain']

   SYSDOWN[hostname]['curtime'] = curtime

   return SYSDOWN[hostname]

#
# return seconds for how long the system will continue to be down
#
def system_down_time(hostname, offset, logact = 0):

   down = get_system_downs(hostname, logact)
   if down['start'] and down['curtime'] >= (down['start'] - offset):
      if not down['end']:
         if PgLOG.PGLOG['PGBATCH'] == PgLOG.PGLOG['PBSNAME']:
            return PgLOG.PGLOG['PBSTIME']
      elif down['curtime'] <= down['end']:
         return (down['end'] - down['curtime'])

   return 0  # the system is not down

#
# return string message if the system is down
#
def system_down_message(hostname, path, offset, logact = 0):

   down = get_system_downs(hostname, logact)
   msg = None
   if down['start'] and down['curtime'] >= (down['start'] - offset):
      match = match_down_path(path, down['path'])
      if match:
         msg = "{}{}:".format(hostname, ('-' + path) if match > 0 else '')
         if not down['active']:
            msg += " Not in Service"
         else:
            msg += " Planned down, started at " + PgLOG.current_datetime(down['start'])
            if not down['end']:
               msg += " And no end time specified"
            elif down['curtime'] <= down['end']:
               msg = " And will end by " + PgLOG.current_datetime(down['end'])

   return msg

#
# return 1 if given path match daemon paths, 0 if not; -1 if cannot compare
#
def match_down_path(path, dpaths):

   if not (path and dpaths): return -1

   paths = re.split(':', dpaths)

   for p in paths:
      if re.match(r'^{}'.format(p), path): return 1

   return 0

# validate is login user is in DECS group
# check all node if skpdsg is false, otherwise check non-DSG nodes
def validate_decs_group(cmdname, logname, skpdsg):
   
   if skpdsg and PgLOG.PGLOG['DSGHOSTS'] and re.search(r'(^|:){}'.format(PgLOG.PGLOG['HOSTNAME']), PgLOG.PGLOG['DSGHOSTS']): return
   if not logname: lgname = PgLOG.PGLOG['CURUID']

   if not pgget("dssgrp", '', "logname = '{}'".format(logname), PgLOG.LGEREX):
      PgLOG.pglog("{}: Must be in DECS Group to run '{}' on {}".format(logname, cmdname, PgLOG.PGLOG['HOSTNAME']), PgLOG.LGEREX)

#
# add an allusage record into yearly table; create a new yearly table if it does not exist
# year    -- year to identify the yearly table, evaluated if missing 
# records -- hash to hold one or multiple records.
# Dict keys: email -- user email address,
#         org_type -- organization type
#          country -- country code
#             dsid -- dataset ID 
#             date -- date data accessed
#             time -- time data accessed
#          quarter -- quarter of the year data accessed
#             size -- bytes of data accessed
#           method -- delivery methods: MSS,Web,Ftp,Tape,Cd,Disk,Paper,cArt,Micro
#           source -- usage source flag: W - wusage, O - ordusage
#             midx -- refer to mbr2loc.midx if not 0
#               ip -- user IP address
#           region -- user region name; for example, Colorado
#
# isarray -- if true, mutiple records provided via arrays for each hash key 
# docheck -- if 1, check and add only if record is not on file
# docheck -- if 2, check and add if record is not on file, and update if exists
# docheck -- if 4, check and add if record is not on file, and update if exists,
#            and also checking NULL email value too 
#
def add_yearly_allusage(year, records, isarray = 0, docheck = 0):

   acnt = 0
   if not year:
      ms = re.match(r'^(\d\d\d\d)', str(records['date'][0] if isarray else records['date']))
      if ms: year = ms.group(1)
   tname = "allusage_{}".format(year)
   if isarray:
      cnt = len(records['email'])
      if 'quarter' not in records: records['quarter'] = [0]*cnt
      for i in range(cnt):
         if not records['quarter'][i]:
            ms = re.search(r'-(\d+)-', str(records['date'][i]))
            if ms: records['quarter'][i] = int((int(ms.group(1))-1)/3)+1
      if docheck:
         for i in range(cnt):
            record = {}
            for key in records:
               record[key] = records[key][i]
            cnd = "email = '{}' AND dsid = '{}' AND method = '{}' AND date = '{}' AND time = '{}'".format(
                   record['email'], record['dsid'], record['method'], record['date'], record['time'])
            pgrec = pgget(tname, 'aidx', cnd, PgLOG.LOGERR|PgLOG.ADDTBL)
            if docheck == 4 and not pgrec:
               cnd = "email IS NULL AND dsid = '{}' AND method = '{}' AND date = '{}' AND time = '{}'".format(
                      record['dsid'], record['method'], record['date'], record['time'])
               pgrec = pgget(tname, 'aidx', cnd, PgLOG.LOGERR|PgLOG.ADDTBL)
            if pgrec:
               if docheck > 1: acnt += pgupdt(tname, record, "aidx = {}".format(pgrec['aidx']), PgLOG.LGEREX)
            else:
               acnt += pgadd(tname, record, PgLOG.LGEREX|PgLOG.ADDTBL)
      else:
         acnt = pgmadd(tname, records, PgLOG.LGEREX|PgLOG.ADDTBL)
   else:
      record = records
      if not ('quarter' in record and record['quarter']):
         ms = re.search(r'-(\d+)-', str(record['date']))
         if ms: record['quarter'] = int((int(ms.group(1))-1)/3)+1
      if docheck:
         cnd = "email = '{}' AND dsid = '{}' AND method = '{}' AND date = '{}' AND time = '{}'".format(
                record['email'], record['dsid'], record['method'], record['date'], record['time'])
         pgrec = pgget(tname, 'aidx', cnd, PgLOG.LOGERR|PgLOG.ADDTBL)
         if docheck == 4 and not pgrec:
            cnd = "email IS NULL AND dsid = '{}' AND method = '{}' AND date = '{}' AND time = '{}'".format(
                   record['dsid'], record['method'], record['date'], record['time'])
            pgrec = pgget(tname, 'aidx', cnd, PgLOG.LOGERR|PgLOG.ADDTBL)
         if pgrec:
            if docheck > 1: acnt = pgupdt(tname, record, "aidx = {}".format(pgrec['aidx']), PgLOG.LGEREX)
            return acnt
      acnt = pgadd(tname, record, PgLOG.LGEREX|PgLOG.ADDTBL)

   return acnt

#
# add a wusage record into yearly table; create a new yearly table if it does not exist
# year    -- year to identify the yearly table, evaluated if missing 
# records -- hash to hold one or multiple records.
# Dict keys: wid - reference to wfile.wid
#      wuid_read - reference to wuser.wuid, 0 if missing email
#           dsid - reference to dataset.dsid at the time of read
#      date_read - date file read
#      time_read - time file read
#        quarter - quarter of the year data accessed
#      size_read - bytes of data read
#         method - download methods: WEB, CURL, MGET, FTP and MGET
#        locflag - location flag: Glade or Object
#             ip - IP address
#
# isarray -- if true, mutiple records provided via arrays for each hash key 
#
def add_yearly_wusage(year, records, isarray = 0):

   acnt = 0
   if not year:
      ms = re.match(r'^(\d\d\d\d)', str(records['date_read'][0] if isarray else records['date_read']))
      if ms: year = ms.group(1)
   tname = "wusage_{}".format(year)
   if isarray:
      if 'quarter' not in records:
         cnt = len(records['wid'])
         records['quarter'] = [0]*cnt
         for i in range(cnt):
            ms = re.search(r'-(\d+)-', str(records['date_read'][i]))
            if ms: records['quarter'][i] = (int((int(ms.group(1))-1)/3)+1)
      acnt = pgmadd(tname, records, PgLOG.LGEREX|PgLOG.ADDTBL)
   else:
      record = records
      if 'quarter' not in record:
         ms = re.search(r'-(\d+)-', str(record['date_read']))
         if ms: record['quarter'] = (int((int(ms.group(1))-1)/3)+1)
      acnt = pgadd(tname, record, PgLOG.LGEREX|PgLOG.ADDTBL)

   return acnt

#
# double quote a array of single or sign delimited strings
#
def pgnames(ary, sign = None, joinstr = None):

   pgary = []
   for a in ary:
      pgary.append(pgname(a, sign))

   if joinstr == None:
      return pgary
   else:
      return joinstr.join(pgary)

#
# double quote a single or sign delimited string
#
def pgname(str, sign = None):

   if sign:
      nstr = ''
      names = str.split(sign[0])
      for name in names:
         if nstr: nstr += sign[0]
         nstr += pgname(name, sign[1:])
   else:
      nstr = str.strip()
      if nstr and nstr.find('"') < 0:
         if not re.match(r'^[a-z_][a-z0-9_]*$', nstr) or nstr in PGRES:
          nstr = '"{}"'.format(nstr)

   return nstr

#
# get a postgres password for given host, port, dbname, usname
#
def get_pgpass_password():

   if PGDBI['PWNAME']: return PGDBI['PWNAME']
   if not DBPASS: read_pgpass()
   dbport = str(PGDBI['DBPORT']) if PGDBI['DBPORT'] else '5432'
   pwname = DBPASS.get((PGDBI['DBSHOST'], dbport, PGDBI['DBNAME'], PGDBI['LNNAME']))
   if not pwname: pwname = DBPASS.get((PGDBI['DBHOST'], dbport, PGDBI['DBNAME'], PGDBI['LNNAME']))

#
# Reads the .pgpass file and returns a dictionary of credentials.
#
def read_pgpass():

   try:
      with open(PgLOG.PGLOG['DSSHOME'] + '/.pgpass', "r") as f:
         for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            dbhost, dbport, dbname, lnname, pwname = line.split(":")
            DBPASS[(dbhost, dbport, dbname, lnname)] = pwname
   except FileNotFoundError:
      pass
