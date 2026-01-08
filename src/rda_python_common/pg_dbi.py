###############################################################################
#     Title: pg_dbi.py  -- PostgreSQL DataBase Interface
#    Author: Zaihua Ji,  zji@ucar.edu
#      Date: 06/07/2022
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-11-24 convert to class PgDBI
#   Purpose: Python library module to handle query and manipulate PostgreSQL database
#    Github: https://github.com/NCAR/rda-python-common.git
###############################################################################
import os
import re
import time
import hvac
from datetime import datetime
import psycopg2 as PgSQL
from psycopg2.extras import execute_values
from psycopg2.extras import execute_batch
from os import path as op
from .pg_log import PgLOG

class PgDBI(PgLOG):
   
   #  PostgreSQL specified query timestamp format
   fmtyr = lambda fn: "extract(year from {})::int".format(fn)
   fmtqt = lambda fn: "extract(quarter from {})::int".format(fn)
   fmtmn = lambda fn: "extract(month from {})::int".format(fn)
   fmtdt = lambda fn: "date({})".format(fn)
   fmtym = lambda fn: "to_char({}, 'yyyy-mm')".format(fn)
   fmthr = lambda fn: "extract(hour from {})::int".format(fn)

   def __init__(self):
      super().__init__()  # initialize parent class
      self.pgdb = None    # reference to a connected database object
      self.curtran = 0    # 0 - no transaction, 1 - in transaction
      self.NMISSES = []   # array of mising userno
      self.LMISSES = []   # array of mising logname
      self.TABLES = {}      # record table field information
      self.SEQUENCES = {}   # record table sequence fielnames
      self.SPECIALIST = {}  # hash array refrences to specialist info of dsids
      self.SYSDOWN = {}
      self.PGDBI = {}
      self.ADDTBLS = []
      self.PGSIGNS = ['!', '<', '>', '<>']
      self.CHCODE = 1042
      # hard coded db ports for dbnames
      self.DBPORTS = {'default': 0}
      self.DBPASS = {}
      self.DBBAOS = {}
      # hard coded db names for given schema names
      self.DBNAMES = {
         'ivaddb': 'ivaddb',
         'cntldb': 'ivaddb',
         'cdmsdb': 'ivaddb',
         'ispddb': 'ispddb',
          'obsua': 'upadb',
        'default': 'rdadb',
      }
      # hard coded socket paths for machine_dbnames
      self.DBSOCKS = {'default': ''}
      # home path for check db on alter host
      self.VIEWHOMES = {'default': self.PGLOG['DSSDBHM']}
      # add more to the list if used for names
      self.PGRES = ['end', 'window']
      self.SETPGDBI('DEFDB', 'rdadb')
      self.SETPGDBI("DEFSC", 'dssdb')
      self.SETPGDBI('DEFHOST', self.PGLOG['PSQLHOST'])
      self.SETPGDBI("DEFPORT", 0)
      self.SETPGDBI("DEFSOCK", '')
      self.SETPGDBI("DBNAME", self.PGDBI['DEFDB'])
      self.SETPGDBI("SCNAME", self.PGDBI['DEFSC'])
      self.SETPGDBI("LNNAME", self.PGDBI['DEFSC'])
      self.SETPGDBI("PWNAME", None)
      self.SETPGDBI("DBHOST", (os.environ['DSSDBHOST'] if os.environ.get('DSSDBHOST') else self.PGDBI['DEFHOST']))
      self.SETPGDBI("DBPORT", 0)
      self.SETPGDBI("ERRLOG", self.LOGERR)   # default error logact
      self.SETPGDBI("EXITLG", self.LGEREX)   # default exit logact
      self.SETPGDBI("DBSOCK", '')
      self.SETPGDBI("DATADIR", self.PGLOG['DSDHOME'])
      self.SETPGDBI("BCKPATH", self.PGLOG['DSSDBHM'] + "/backup")
      self.SETPGDBI("SQLPATH", self.PGLOG['DSSDBHM'] + "/sql")
      self.SETPGDBI("VWNAME", self.PGDBI['DEFSC'])
      self.SETPGDBI("VWPORT", 0)
      self.SETPGDBI("VWSOCK", '')
      self.SETPGDBI("BAOURL", 'https://bao.k8s.ucar.edu/')
      
      self.PGDBI['DBSHOST'] = self.get_short_host(self.PGDBI['DBHOST'])
      self.PGDBI['DEFSHOST'] = self.get_short_host(self.PGDBI['DEFHOST'])
      self.PGDBI['VWHOST'] = self.PGLOG['PVIEWHOST']
      self.PGDBI['MSHOST'] = self.PGLOG['PMISCHOST']
      self.PGDBI['VWSHOST'] = self.get_short_host(self.PGDBI['VWHOST'])
      self.PGDBI['MSSHOST'] = self.get_short_host(self.PGDBI['MSHOST'])
      self.PGDBI['VWHOME'] =  (self.VIEWHOMES[self.PGLOG['HOSTNAME']] if self.PGLOG['HOSTNAME'] in self.VIEWHOMES else self.VIEWHOMES['default'])
      self.PGDBI['SCPATH'] = None       # additional schema path for set search_path
      self.PGDBI['VHSET'] = 0
      self.PGDBI['PGSIZE'] = 1000        # number of records for page_size
      self.PGDBI['MTRANS'] = 5000       # max number of changes in one transactions
      self.PGDBI['MAXICNT'] = 6000000  # maximum number of records in each table

   # set environments and defaults
   def SETPGDBI(self, name, value):
      self.PGDBI[name] = self.get_environment(name, value)

   # create a pgddl command string with
   # table name (tname), prefix (pre) and suffix (suf)
   def get_pgddl_command(self, tname, pre = None, suf = None, scname = None):
      ms = re.match(r'^(.+)\.(.+)$', tname)
      if not scname:
         if ms:
            scname = ms.group(1)
            tname = ms.group(2)
         else:
            scname = self.PGDBI['SCNAME']
      xy = ''
      if suf: xy += ' -x ' + suf
      if pre: xy += ' -y ' + pre
      return "pgddl {} -aa -h {} -d {} -c {} -u {}{}".format(tname, self.PGDBI['DBHOST'], self.PGDBI['DBNAME'], scname, self.PGDBI['LNNAME'], xy)

   # set default connection for dssdb PostgreSQL Server
   def dssdb_dbname(self):
      self.default_scinfo(self.PGDBI['DEFDB'], self.PGDBI['DEFSC'], self.PGLOG['PSQLHOST'])
   dssdb_scname = dssdb_dbname

   # set default connection for obsua PostgreSQL Server
   def obsua_dbname(self):
      self.default_scinfo('upadb', 'obsua', self.PGLOG['PMISCHOST'])
   obsua_scname = obsua_dbname

   # set default connection for ivaddb PostgreSQL Server
   def ivaddb_dbname(self):
      self.default_scinfo('ivaddb', 'ivaddb', self.PGLOG['PMISCHOST'])
   ivaddb_scname = ivaddb_dbname

   # set default connection for ispddb PostgreSQL Server
   def ispddb_dbname(self):
      self.default_scinfo('ispddb', 'ispddb', self.PGLOG['PMISCHOST'])
   ispddb_scname = ispddb_dbname

   # set a default schema info with hard coded info
   def default_dbinfo(self, scname = None, dbhost = None, lnname = None, pwname = None, dbport = None, socket = None):
      return self.default_scinfo(self.get_dbname(scname), scname, dbhost, lnname, pwname, dbport, socket)

   # set default database/schema info with hard coded info
   def default_scinfo(self, dbname = None, scname = None, dbhost = None, lnname = None, pwname = None, dbport = None, socket = None):
      if not dbname: dbname = self.PGDBI['DEFDB']
      if not scname: scname = self.PGDBI['DEFSC']
      if not dbhost: dbhost = self.PGDBI['DEFHOST']
      if dbport is None: dbport = self.PGDBI['DEFPORT']
      if socket is None:  socket = self.PGDBI['DEFSOCK']
      self.set_scname(dbname, scname, lnname, pwname, dbhost, dbport, socket)

   # get the datbase sock file name of a given dbname for local connection
   def get_dbsock(self, dbname):
      return (self.DBSOCKS[dbname] if dbname in self.DBSOCKS else self.DBSOCKS['default'])

   # get the datbase port number of a given dbname for remote connection
   def get_dbport(self, dbname):
      return (self.DBPORTS[dbname] if dbname in self.DBPORTS else self.DBPORTS['default'])

   # get the datbase name of a given schema name for remote connection
   def get_dbname(self, scname):
      if scname:
         if scname in self.DBNAMES: return self.DBNAMES[scname]
         return self.DBNAMES['default']
      return None

   # set connection for viewing database information
   def view_dbinfo(self, scname = None, lnname = None, pwname = None):
      self.view_scinfo(self.get_dbname(scname), scname, lnname, pwname)

   # set connection for viewing database/schema information
   def view_scinfo(self, dbname = None, scname = None, lnname = None, pwname = None):
      if not dbname: dbname = self.PGDBI['DEFDB']
      if not scname: scname = self.PGDBI['DEFSC']
      self.set_scname(dbname, scname, lnname, pwname, self.PGLOG['PVIEWHOST'], self.PGDBI['VWPORT'])

   # set connection for given scname
   def set_dbname(self, scname = None, lnname = None, pwname = None, dbhost = None, dbport = None, socket = None):
      if not scname: scname = self.PGDBI['DEFSC']
      self.set_scname(self.get_dbname(scname), scname, lnname, pwname, dbhost, dbport, socket)

   # set connection for given database & schema names
   def set_scname(self, dbname = None, scname = None, lnname = None, pwname = None, dbhost = None, dbport = None, socket = None):
      changed = 0
      if dbname and dbname != self.PGDBI['DBNAME']:
         self.PGDBI['DBNAME'] = dbname
         changed = 1
      if scname and scname != self.PGDBI['SCNAME']:
         self.PGDBI['LNNAME'] = self.PGDBI['SCNAME'] = scname
         changed = 1
      if lnname and lnname != self.PGDBI['LNNAME']:
         self.PGDBI['LNNAME'] = lnname
         changed = 1
      if pwname != self.PGDBI['PWNAME']:
         self.PGDBI['PWNAME'] = pwname
         changed = 1
      if dbhost and dbhost != self.PGDBI['DBHOST']:
         self.PGDBI['DBHOST'] = dbhost
         self.PGDBI['DBSHOST'] = self.get_short_host(dbhost)
         changed = 1
      if self.PGDBI['DBSHOST'] == self.PGLOG['HOSTNAME']:
         if socket is None: socket = self.get_dbsock(dbname)
         if socket != self.PGDBI['DBSOCK']:
            self.PGDBI['DBSOCK'] = socket
            changed = 1
      else:
         if not dbport: dbport = self.get_dbport(dbname)
         if dbport != self.PGDBI['DBPORT']:
            self.PGDBI['DBPORT'] = dbport
            changed = 1
      if changed and self.pgdb is not None: self.pgdisconnect(1)

   # start a database transaction and exit if fails
   def starttran(self):
      if self.curtran == 1: self.endtran()   # try to end previous transaction
      if not self.pgdb:
         self.pgconnect(0, 0, False)
      else:
         try:
            self.pgdb.isolation_level
         except PgSQL.OperationalError as e:
            self.pgconnect(0, 0, False)
         if self.pgdb.closed:
            self.pgconnect(0, 0, False)
         elif self.pgdb.autocommit:
            self.pgdb.autocommit = False
      self.curtran = 1

   # end a transaction with changes committed and exit if fails
   def endtran(self, autocommit = True):
      if self.curtran and self.pgdb:
         if not self.pgdb.closed: self.pgdb.commit()
         self.pgdb.autocommit = autocommit
         self.curtran = 0 if autocommit else 1

   # end a transaction without changes committed and exit inside if fails
   def aborttran(self, autocommit = True):
      if self.curtran and self.pgdb:
         if not self.pgdb.closed: self.pgdb.rollback()
         self.pgdb.autocommit = autocommit
      self.curtran = 0 if autocommit else 1

   # record error message to dscheck record and clean the lock
   def record_dscheck_error(self, errmsg, logact = None):
      if logact is None: logact = self.PGDBI['EXITLG']
      check = self.PGLOG['DSCHECK']
      chkcnd = check['chkcnd'] if 'chkcnd' in check else "cindex = {}".format(check['cindex'])
      dflags = check['dflags'] if 'dflags' in check else ''
      if self.PGLOG['NOQUIT']: self.PGLOG['NOQUIT'] = 0
      pgrec = self.pgget("dscheck", "mcount, tcount, lockhost, pid", chkcnd, logact)
      if not pgrec: return 0
      if not pgrec['pid'] and not pgrec['lockhost']: return 0
      (chost, cpid) = self.current_process_info()
      if pgrec['pid'] != cpid or pgrec['lockhost'] != chost: return 0
      # update dscheck record only if it is still locked by the current process
      record = {}
      record['chktime'] = int(time.time())
      if logact&self.EXITLG:
         record['status'] = "E"
         record['pid'] = 0   # release lock
      if dflags:
         record['dflags'] = dflags
         record['mcount'] = pgrec['mcount'] + 1
      else:
         record['dflags'] = ''
      if errmsg:
         errmsg = self.break_long_string(errmsg, 512, None, 50, None, 50, 25)
         if pgrec['tcount'] > 1: errmsg = "Try {}: {}".format(pgrec['tcount'], errmsg)
         record['errmsg'] = errmsg
      return self.pgupdt("dscheck", record, chkcnd, logact)

   # local function to log query error
   def qelog(self, dberror, sleep, sqlstr, vals, pgcnt, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      retry = " Sleep {}(sec) & ".format(sleep) if sleep else " "
      if sqlstr:
         if sqlstr.find("Retry ") == 0:
            retry += "the {} ".format(self.int2order(pgcnt+1))
         elif sleep:
            retry += "the {} Retry: \n".format(self.int2order(pgcnt+1))
         elif pgcnt:
            retry = " Error the {} Retry: \n".format(self.int2order(pgcnt))
         else:
            retry = "\n"
         sqlstr = retry + sqlstr
      else:
         sqlstr = ''
      if vals: sqlstr += " with values: " + str(vals)
      if dberror: sqlstr = "{}\n{}".format(dberror, sqlstr)
      if logact&self.EXITLG and self.PGLOG['DSCHECK']: self.record_dscheck_error(sqlstr, logact)
      self.pglog(sqlstr, logact)
      if sleep: time.sleep(sleep)   
      return self.FAILURE    # if not exit in self.pglog()

   # try to add a new table according the table not exist error
   def try_add_table(self, dberror, logact):
      ms = re.match(r'^42P01 ERROR:  relation "(.+)" does not exist', dberror)
      if ms:
         tname = ms.group(1)
         self.add_new_table(tname, logact = logact)

   # add a table for given table name
   def add_a_table(self, tname, logact):
      self.add_new_table(tname, logact = logact)

   # add a new table for given table name
   def add_new_table(self, tname, pre = None, suf = None, logact = 0):
      if pre:
         tbname = '{}_{}'.format(pre, tname)
      elif suf:
         tbname = '{}_{}'.format(tname, suf)
      else:
         tbname = tname
      if tbname in self.ADDTBLS: return
      self.pgsystem(self.get_pgddl_command(tname, pre, suf), logact)
      self.ADDTBLS.append(tbname)

   # validate a table for given table name (tname), prefix (pre) and suffix (suf),
   # and add it if not existing
   def valid_table(self, tname, pre = None, suf = None, logact = 0):
      if pre:
         tbname = '{}_{}'.format(pre, tname)
      elif suf:
         tbname = '{}_{}'.format(tname, suf)
      else:
         tbname = tname
      if tbname in self.ADDTBLS: return tbname
      if not self.pgcheck(tbname, logact): self.pgsystem(self.get_pgddl_command(tname, pre, suf), logact)
      self.ADDTBLS.append(tbname)
      return tbname

   # local function to log query error
   def check_dberror(self, pgerr, pgcnt, sqlstr, ary, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      ret = self.FAILURE
      pgcode = pgerr.pgcode
      pgerror = pgerr.pgerror
      dberror = "{} {}".format(pgcode, pgerror) if pgcode and pgerror else str(pgerr)
      if pgcnt < self.PGLOG['DBRETRY']:
         if not pgcode:
            if self.PGDBI['DBNAME'] == self.PGDBI['DEFDB'] and self.PGDBI['DBSHOST'] != self.PGDBI['DEFSHOST']:
               self.default_dbinfo()
               self.qelog(dberror, 0, "Retry Connecting to {} on {}".format(self.PGDBI['DBNAME'], self.PGDBI['DBHOST']), ary, pgcnt, self.MSGLOG)
            else:
               self.qelog(dberror, 5+5*pgcnt, "Retry Connecting", ary, pgcnt, self.LOGWRN)
            return self.SUCCESS
         elif re.match(r'^(08|57)', pgcode):
            self.qelog(dberror, 0, "Retry Connecting", ary, pgcnt, self.LOGWRN)
            self.pgconnect(1, pgcnt + 1)
            return (self.FAILURE if not self.pgdb else self.SUCCESS)
         elif re.match(r'^55', pgcode):  #  try to lock again
            self.qelog(dberror, 10, "Retry Locking", ary, pgcnt, self.LOGWRN)
            return self.SUCCESS
         elif pgcode == '25P02':   #  try to add table
            self.qelog(dberror, 0, "Rollback transaction", ary, pgcnt, self.LOGWRN)
            self.pgdb.rollback()
            return self.SUCCESS
         elif pgcode == '42P01' and logact&self.ADDTBL:   #  try to add table
            self.qelog(dberror, 0, "Retry after adding a table", ary, pgcnt, self.LOGWRN)
            self.try_add_table(dberror, logact)
            return self.SUCCESS
      if logact&self.DOLOCK and pgcode and re.match(r'^55\w\w\w$', pgcode):
         logact &= ~self.EXITLG   # no exit for lock error
      elif pgcnt > self.PGLOG['DBRETRY']:
         logact |= self.EXITLG   # exit for error count exceeds limit
      return self.qelog(dberror, 0, sqlstr, ary, pgcnt, logact)

   # return hash reference to postgresql batch mode command and output file name
   def pgbatch(self, sqlfile, foreground = 0):
      dbhost = 'localhost' if self.PGDBI['DBSHOST'] == self.PGLOG['HOSTNAME'] else self.PGDBI['DBHOST']
      options = "-h {} -p {}".format(dbhost, self.PGDBI['DBPORT'])
      pwname = self.get_pgpass_password()
      os.environ['PGPASSWORD'] = pwname
      options += " -U {} {}".format(self.PGDBI['LNNAME'], self.PGDBI['DBNAME'])   
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

   # start a connection to dssdb database and return a DBI object; None if error
   # force connect if connect > 0
   def pgconnect(self, reconnect = 0, pgcnt = 0, autocommit = True):
      if self.pgdb:
         if reconnect and not self.pgdb.closed: return self.pgdb    # no need reconnect
      elif reconnect:
         reconnect = 0   # initial connection
      while True:
         config = {'database': self.PGDBI['DBNAME'],
                       'user': self.PGDBI['LNNAME']}
         if self.PGDBI['DBSHOST'] == self.PGLOG['HOSTNAME']:
            config['host'] = 'localhost'
         else:
            config['host'] = self.PGDBI['DBHOST'] if self.PGDBI['DBHOST'] else self.PGDBI['DEFHOST']
            if not self.PGDBI['DBPORT']: self.PGDBI['DBPORT'] = self.get_dbport(self.PGDBI['DBNAME'])
         if self.PGDBI['DBPORT']: config['port'] = self.PGDBI['DBPORT']
         config['password'] = '***'
         sqlstr = "psycopg2.connect(**{})".format(config)
         config['password'] = self.get_pgpass_password()
         if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, sqlstr)
         try:
            self.PGLOG['PGDBBUF'] = self.pgdb = PgSQL.connect(**config)
            if reconnect: self.pglog("{} Reconnected at {}".format(sqlstr, self.current_datetime()), self.MSGLOG|self.FRCLOG)
            if autocommit: self.pgdb.autocommit = autocommit
            return self.pgdb
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, None, self.PGDBI['EXITLG']): return self.FAILURE
            pgcnt += 1

   # return a PostgreSQL cursor upon success
   def pgcursor(self):
      pgcur = None
      if not self.pgdb:
         self.pgconnect()
         if not self.pgdb: return self.FAILURE
      pgcnt = 0
      while True:
         try:
            pgcur = self.pgdb.cursor()
            spath = "SET search_path = '{}'".format(self.PGDBI['SCNAME'])
            if self.PGDBI['SCPATH'] and self.PGDBI['SCPATH'] != self.PGDBI['SCNAME']:
               spath += ", '{}'".format(self.PGDBI['SCPATH'])
            pgcur.execute(spath)
         except PgSQL.Error as pgerr:
            if pgcnt == 0 and self.pgdb.closed:
               self.pgconnect(1)
            elif not self.check_dberror(pgerr, pgcnt, '', None, self.PGDBI['EXITLG']):
               return self.FAILURE
         else:
            break
         pgcnt += 1
      return pgcur

   # disconnect to dssdb database
   def pgdisconnect(self, stopit = 1):
      if self.pgdb:
         if stopit: self.pgdb.close()
         self.PGLOG['PGDBBUF'] = self.pgdb = None

   # gather table field default information as hash array with field names as keys
   # and default values as values
   # the whole table information is cached to a hash array with table names as keys
   def pgtable(self, tablename, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if tablename in self.TABLES: return self.TABLES[tablename].copy()  # cached already
      intms = r'^(smallint||bigint|integer)$'
      fields = "column_name col, data_type typ, is_nullable nil, column_default def"
      condition = self.table_condition(tablename)
      pgcnt = 0
      while True:
         pgrecs = self.pgmget('information_schema.columns', fields, condition, logact)
         cnt = len(pgrecs['col']) if pgrecs else 0
         if cnt: break
         if pgcnt == 0 and logact&self.ADDTBL:
            self.add_new_table(tablename, logact = logact)
         else:
            return self.pglog(tablename + ": Table not exists", logact)
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
               dflt = self.check_default_value(dflt, isint)
         elif pgrecs['nil'][i] == 'YES':
            dflt = None
         elif isint:
            dflt = 0
         else:
            dflt = ''
         pgdefs[name] = dflt
      self.TABLES[tablename] = pgdefs.copy()
      return pgdefs

   # get sequence field name for given table name
   def pgsequence(self, tablename, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if tablename in self.SEQUENCES: return self.SEQUENCES[tablename]  # cached already
      condition = self.table_condition(tablename) + " AND column_default LIKE 'nextval(%'"
      pgrec = self.pgget('information_schema.columns', 'column_name', condition, logact)
      seqname = pgrec['column_name'] if pgrec else None
      self.SEQUENCES[tablename] = seqname
      return seqname

   # check default value for integer & string
   @staticmethod
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

   # local fucntion: insert prepare pgadd()/pgmadd() for given table and field names
   # according to options of multiple place holds and returning sequence id
   def prepare_insert(self, tablename, fields, multi = True, getid = None):   
      strfld = self.pgnames(fields, '.', ',')
      if multi:
         strplc = "(" + ','.join(['%s']*len(fields)) + ")"
      else:
         strplc = '%s'
      sqlstr = "INSERT INTO {} ({}) VALUES {}".format(tablename, strfld, strplc)
      if getid: sqlstr += " RETURNING " + getid
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, sqlstr)
      return sqlstr

   # local fucntion: prepare default value for single record
   def prepare_default(self, tablename, record, logact = 0):   
      table = self.pgtable(tablename, logact)
      for fld in record:
         val = record[fld]
         if val is None:
            vlen = 0
         elif isinstance(val, str):
            vlen = len(val)
         else:
            vlen = 1
         if vlen == 0: record[fld] = table[fld]

   # local fucntion: prepare default value for multiple records
   def prepare_defaults(self, tablename, records, logact = 0):   
      table = self.pgtable(tablename, logact)   
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

   # insert one record into tablename
   # tablename: add record for one table name each call
   #    record: hash reference with keys as field names and hash values as field values
   # return self.SUCCESS or self.FAILURE
   def pgadd(self, tablename, record, logact = None, getid = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if not record: return self.pglog("Nothing adds to " + tablename, logact)
      if logact&self.DODFLT: self.prepare_default(tablename, record, logact)
      if logact&self.AUTOID and not getid: getid = self.pgsequence(tablename, logact)
      sqlstr = self.prepare_insert(tablename, list(record), True, getid)
      values = tuple(record.values())
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "Insert: " + str(values))
      ret = acnt = pgcnt = 0
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
         try:
            pgcur.execute(sqlstr, values)
            acnt = 1
            if getid:
               ret = pgcur.fetchone()[0]
            else:
               ret = self.SUCCESS
            pgcur.close()
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, values, logact): return self.FAILURE
         else:
            break
         pgcnt += 1
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "pgadd: 1 record added to " + tablename + ", return " + str(ret))
      if(logact&self.ENDLCK):
         self.endtran()
      elif self.curtran:
         self.curtran += acnt
         if self.curtran > self.PGDBI['MTRANS']: self.starttran()
      return ret

   # insert multiple records into tablename
   # tablename: add records for one table name each call
   #   records: dict with field names as keys and each value is a list of field values
   #  return self.SUCCESS or self.FAILURE
   def pgmadd(self, tablename, records, logact = None, getid = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if not records: return self.pglog("Nothing to insert to table " + tablename, logact)
      if logact&self.DODFLT: self.prepare_defaults(tablename, records, logact)
      if logact&self.AUTOID and not getid: getid = self.pgsequence(tablename, logact)
      multi = True if getid else False
      sqlstr = self.prepare_insert(tablename, list(records), multi, getid)   
      v = records.values()
      values = list(zip(*v))
      cntrow = len(values)
      ids = [] if getid else None
      if self.PGLOG['DBGLEVEL']:
         for row in values: self.pgdbg(1000, "Insert: " + str(row))
      count = pgcnt = 0
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
         if getid:
            while count < cntrow:
               record = values[count]
               try:
                  pgcur.execute(sqlstr, record)
                  ids.append(pgcur.fetchone()[0])
                  count += 1
               except PgSQL.Error as pgerr:
                  if not self.check_dberror(pgerr, pgcnt, sqlstr, record, logact): return self.FAILURE
                  break
         else:
            try:
               execute_values(pgcur, sqlstr, values, page_size=self.PGDBI['PGSIZE'])
               count = cntrow
            except PgSQL.Error as pgerr:
               if not self.check_dberror(pgerr, pgcnt, sqlstr, values[0], logact): return self.FAILURE
         if count >= cntrow: break
         pgcnt += 1
      pgcur.close()
      if(self.PGLOG['DBGLEVEL']): self.pgdbg(1000, "pgmadd: {} of {} record(s) added to {}".format(count, cntrow, tablename))
      if(logact&self.ENDLCK):
         self.endtran()
      elif self.curtran:
         self.curtran += count
         if self.curtran > self.PGDBI['MTRANS']: self.starttran()
      return (ids if ids else count)

   # local function: select prepare for pgget() and pgmget()
   def prepare_select(self, tablenames, fields = None, condition = None, cndflds = None, logact = 0):
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
         if logact&self.DOLOCK:
            self.starttran()
            sqlstr += " FOR UPDATE"
      elif fields:
         sqlstr = "SELECT " + fields
      elif condition:
         sqlstr = condition
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, sqlstr)   
      return sqlstr

   # tablenames: comma deliminated string of one or more tables and more than one table for joining,
   #     fields: comma deliminated string of one or more field names,
   #  condition: querry conditions for where clause
   # return a dict reference with keys as field names upon success
   def pgget(self, tablenames, fields, condition = None, logact = 0):
      if not logact: logact = self.PGDBI['ERRLOG']
      if fields and condition and not re.search(r'limit 1$', condition, re.I): condition += " LIMIT 1"
      sqlstr = self.prepare_select(tablenames, fields, condition, None, logact)
      if fields and not re.search(r'(^|\s)limit 1($|\s)', sqlstr, re.I): sqlstr += " LIMIT 1"
      ucname = True if logact&self.UCNAME else False
      pgcnt = 0
      record = {}
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
         try:
            pgcur.execute(sqlstr)
            vals = pgcur.fetchone()
            if vals:
               colcnt = len(pgcur.description)
               for i in range(colcnt):
                  col = pgcur.description[i]
                  colname = col[0].upper() if ucname else col[0]
                  val = vals[i]
                  if col[1] == self.CHCODE and val and val[-1] == ' ': val = val.rstrip()
                  record[colname] = val
            pgcur.close()
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, None, logact): return self.FAILURE
         else:
            break
         pgcnt += 1
      if record and tablenames and not fields:
         if self.PGLOG['DBGLEVEL']:
            self.pgdbg(1000, "pgget: {} record(s) found from {}".format(record['cntrec'], tablenames))
         return record['cntrec']
      elif self.PGLOG['DBGLEVEL']:
         cnt = 1 if record else 0
         self.pgdbg(1000, "pgget: {} record retrieved from {}".format(cnt, tablenames))   
      return record

   # tablenames: comma deliminated string of one or more tables and more than one table for joining,
   #     fields: comma deliminated string of one or more field names,
   #  condition: querry conditions for where clause
   # return a dict reference with keys as field names upon success, values for each field name
   #        are in a list. All lists are the same length with missing values set to None
   def pgmget(self, tablenames, fields, condition = None, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      sqlstr = self.prepare_select(tablenames, fields, condition, None, logact)
      ucname = True if logact&self.UCNAME else False
      count = pgcnt = 0
      records = {}
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
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
                  if col[1] == self.CHCODE:
                     for j in range(count):
                        if vals[j] and vals[j][-1] == ' ': vals[j] = vals[j].rstrip()
                  records[colname] = vals
            pgcur.close()
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, None, logact): return self.FAILURE
         else:
            break
         pgcnt += 1
      if self.PGLOG['DBGLEVEL']:
         self.pgdbg(1000, "pgmget: {} record(s) retrieved from {}".format(count, tablenames))
      return records

   # tablenames: comma deliminated string of one or more tables
   #     fields: comma deliminated string of one or more field names,
   #    cnddict: condition dict with field names: values
   # return a dict(field names: values) upon success
   # retrieve one records from tablenames condition dict
   def pghget(self, tablenames, fields, cnddict, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if not tablenames: return self.pglog("Miss Table name to query", logact)
      if not fields: return self.pglog("Nothing to query " + tablenames, logact)
      if not cnddict: return self.pglog("Miss condition dict values to query " + tablenames, logact)
      sqlstr = self.prepare_select(tablenames, fields, None, list(cnddict), logact)
      if fields and not re.search(r'limit 1$', sqlstr, re.I): sqlstr += " LIMIT 1"
      ucname = True if logact&self.UCNAME else False   
      values = tuple(cnddict.values())
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "Query from {} for {}".format(tablenames, values))
      pgcnt = 0
      record = {}
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
         try:
            pgcur.execute(sqlstr, values)
            vals = pgcur.fetchone()
            if vals:
               colcnt = len(pgcur.description)
               for i in range(colcnt):
                  col = pgcur.description[i]
                  colname = col[0].upper() if ucname else col[0]
                  val = vals[i]
                  if col[1] == self.CHCODE and val and val[-1] == ' ': val = val.rstrip()
                  record[colname] = val
            pgcur.close()
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, values, logact): return self.FAILURE
         else:
            break
         pgcnt += 1
      if record and tablenames and not fields:
         if self.PGLOG['DBGLEVEL']:
            self.pgdbg(1000, "pghget: {} record(s) found from {}".format(record['cntrec'], tablenames))
         return record['cntrec']
      elif self.PGLOG['DBGLEVEL']:
         cnt = 1 if record else 0
         self.pgdbg(1000, "pghget: {} record retrieved from {}".format(cnt, tablenames))
      return record

   # tablenames: comma deliminated string of one or more tables
   #     fields: comma deliminated string of one or more field names,
   #   cnddicts: condition dict with field names: value lists
   # return a dict(field names: value lists) upon success
   # retrieve multiple records from tablenames for condition dict
   def pgmhget(self, tablenames, fields, cnddicts, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if not tablenames: return self.pglog("Miss Table name to query", logact)
      if not fields: return self.pglog("Nothing to query " + tablenames, logact)
      if not cnddicts: return self.pglog("Miss condition dict values to query " + tablenames, logact)
      sqlstr = self.prepare_select(tablenames, fields, None, list(cnddicts), logact)
      ucname = True if logact&self.UCNAME else False   
      v = cnddicts.values()
      values = list(zip(*v))
      cndcnt = len(values)
      if self.PGLOG['DBGLEVEL']:
         for row in values:
            self.pgdbg(1000, "Query from {} for {}".format(tablenames, row))
      colcnt = ccnt = count = pgcnt = 0
      cols = []
      chrs = []
      records = {}
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
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
                        if col[1] == self.CHCODE: chrs.append(colname)
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
               if not self.check_dberror(pgerr, pgcnt, sqlstr, cndvals, logact): return self.FAILURE
               break
         if ccnt >= cndcnt: break
         pgcnt += 1
      pgcur.close()   
      if self.PGLOG['DBGLEVEL']:
         self.pgdbg(1000, "pgmhget: {} record(s) retrieved from {}".format(count, tablenames))
      return records

   # local fucntion: update prepare for pgupdt, pghupdt and pgmupdt
   def prepare_update(self, tablename, fields, condition = None, cndflds = None):
      strset = []
      # build set string
      for fld in fields:
         strset.append("{}=%s".format(self.pgname(fld, '.')))
      strflds = ",".join(strset)
      # build condition string
      if not condition:
         cndset = []
         for fld in cndflds:
            cndset.append("{}=%s".format(self.pgname(fld, '.')))
         condition = " AND ".join(cndset)   
      sqlstr = "UPDATE {} SET {} WHERE {}".format(tablename, strflds, condition)
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, sqlstr)   
      return sqlstr

   # update one or multiple rows in tablename
   # tablename: update for one table name each call
   #    record: dict with field names: values
   # condition: update conditions for where clause)
   # return number of rows undated upon success
   def pgupdt(self, tablename, record, condition, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if not record: self.pglog("Nothing updates to " + tablename, logact)
      if not condition or isinstance(condition, int): self.pglog("Miss condition to update " + tablename, logact)
      sqlstr = self.prepare_update(tablename, list(record), condition)
      if logact&self.DODFLT: self.prepare_default(tablename, record, logact)
      values = tuple(record.values())
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "Update {} for {}".format(tablename, values))
      ucnt = pgcnt = 0
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
         try:
            pgcur.execute(sqlstr, values)
            ucnt = pgcur.rowcount
            pgcur.close()
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, values, logact): return self.FAILURE
         else:
            break
         pgcnt += 1   
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "pgupdt: {} record(s) updated to {}".format(ucnt, tablename))
      if(logact&self.ENDLCK):
         self.endtran()
      elif self.curtran:
         self.curtran += ucnt
         if self.curtran > self.PGDBI['MTRANS']: self.starttran()
      return ucnt

   # update one or multiple records in tablename
   # tablename: update for one table name each call
   #    record: update values, dict with field names: values
   #   cnddict: condition dict with field names: values
   # return number of records updated upon success
   def pghupdt(self, tablename, record, cnddict, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if not record: self.pglog("Nothing updates to " + tablename, logact)
      if not cnddict or isinstance(cnddict, int): self.pglog("Miss condition to update to " + tablename, logact)
      if logact&self.DODFLT: self.prepare_defaults(tablename, record, logact)
      sqlstr = self.prepare_update(tablename, list(record), None, list(cnddict))
      values = tuple(record.values()) + tuple(cnddict.values())
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "Update {} for {}".format(tablename, values))
      ucnt = count = pgcnt = 0
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
         try:
            pgcur.execute(sqlstr, values)
            count += 1
            ucnt = pgcur.rowcount
            pgcur.close()
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, values, logact): return self.FAILURE
         else:
            break
         pgcnt += 1
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "pghupdt: {}/{} record(s) updated to {}".format(ucnt, tablename))
      if(logact&self.ENDLCK):
         self.endtran()
      elif self.curtran:
         self.curtran += ucnt
         if self.curtran > self.PGDBI['MTRANS']: self.starttran()
      return ucnt

   # update multiple records in tablename
   # tablename: update for one table name each call
   #   records: update values, dict with field names: value lists
   #   cnddicts: condition dict with field names: value lists
   # return number of records updated upon success
   def pgmupdt(self, tablename, records, cnddicts, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if not records: self.pglog("Nothing updates to " + tablename, logact)
      if not cnddicts or isinstance(cnddicts, int): self.pglog("Miss condition to update to " + tablename, logact)
      if logact&self.DODFLT: self.prepare_defaults(tablename, records, logact)
      sqlstr = self.prepare_update(tablename, list(records), None, list(cnddicts))
      fldvals = tuple(records.values())
      cntrow = len(fldvals[0])
      cndvals = tuple(cnddicts.values())
      cntcnd = len(cndvals[0])
      if cntcnd != cntrow: return self.pglog("Field/Condition value counts Miss match {}/{} to update {}".format(cntrow, cntcnd, tablename), logact)
      v = fldvals + cndvals
      values = list(zip(*v))
      if self.PGLOG['DBGLEVEL']:
         for row in values: self.pgdbg(1000, "Update {} for {}".format(tablename, row))
      ucnt = pgcnt = 0
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
         try:
            execute_batch(pgcur, sqlstr, values, page_size=self.PGDBI['PGSIZE'])
            ucnt = cntrow
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, values[0], logact): return self.FAILURE
         else:
            break
         pgcnt += 1
      pgcur.close()
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "pgmupdt: {} record(s) updated to {}".format(ucnt, tablename))
      if(logact&self.ENDLCK):
         self.endtran()
      elif self.curtran:
         self.curtran += ucnt
         if self.curtran > self.PGDBI['MTRANS']: self.starttran()
      return ucnt

   # local fucntion: delete prepare for pgdel, pghdel and del
   def prepare_delete(self, tablename, condition = None, cndflds = None):
      # build condition string
      if not condition:
         cndset = []
         for fld in cndflds:
            cndset.append("{}=%s".format(fld))
         condition = " AND ".join(cndset)
      sqlstr = "DELETE FROM {} WHERE {}".format(tablename, condition)
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, sqlstr)
      return sqlstr

   # delete one or mutiple records in tablename according condition
   # tablename: delete for one table name each call
   # condition: delete conditions for where clause
   # return number of records deleted upon success
   def pgdel(self, tablename, condition, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if not condition or isinstance(condition, int): self.pglog("Miss condition to delete from " + tablename, logact)
      sqlstr = self.prepare_delete(tablename, condition)
      dcnt = pgcnt = 0
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
         try:
            pgcur.execute(sqlstr)
            dcnt = pgcur.rowcount
            pgcur.close()
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, None, logact): return self.FAILURE
         else:
            break
         pgcnt += 1
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "pgdel: {} record(s) deleted from {}".format(dcnt, tablename))
      if logact&self.ENDLCK:
         self.endtran()
      elif self.curtran:
         self.curtran += dcnt
         if self.curtran > self.PGDBI['MTRANS']: self.starttran()
      return dcnt

   # delete one or mutiple records in tablename according condition
   # tablename: delete for one table name each call
   #    cndict: delete condition dict for names: values
   # return number of records deleted upon success
   def pghdel(self, tablename, cnddict, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if not cnddict or isinstance(cnddict, int): self.pglog("Miss condition dict to delete from " + tablename, logact)
      sqlstr = self.prepare_delete(tablename, None, list(cnddict))
      values = tuple(cnddict.values())
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "Delete from {} for {}".format(tablename, values))
      dcnt = pgcnt = 0
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
         try:
            pgcur.execute(sqlstr, values)
            dcnt = pgcur.rowcount
            pgcur.close()
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, values, logact): return self.FAILURE
         else:
            break
         pgcnt += 1
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "pghdel: {} record(s) deleted from {}".format(dcnt, tablename))
      if logact&self.ENDLCK:
         self.endtran()
      elif self.curtran:
         self.curtran += dcnt
         if self.curtran > self.PGDBI['MTRANS']: self.starttran()
      return dcnt

   # delete mutiple records in tablename according condition
   # tablename: delete for one table name each call
   #   cndicts: delete condition dict for names: value lists
   # return number of records deleted upon success
   def pgmdel(self, tablename, cnddicts, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if not cnddicts or isinstance(cnddicts, int): self.pglog("Miss condition dict to delete from " + tablename, logact)
      sqlstr = self.prepare_delete(tablename, None, list(cnddicts))
      v = cnddicts.values()
      values = list(zip(*v))
      if self.PGLOG['DBGLEVEL']:
         for row in values:
            self.pgdbg(1000, "Delete from {} for {}".format(tablename, row))
      dcnt = pgcnt = 0
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
         try:
            execute_batch(pgcur, sqlstr, values, page_size=self.PGDBI['PGSIZE'])
            dcnt = len(values)
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, values[0], logact): return self.FAILURE
         else:
            break
         pgcnt += 1
      pgcur.close()
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "pgmdel: {} record(s) deleted from {}".format(dcnt, tablename))
      if logact&self.ENDLCK:
         self.endtran()
      elif self.curtran:
         self.curtran += dcnt
         if self.curtran > self.PGDBI['MTRANS']: self.starttran()
      return dcnt

   # sqlstr: a complete sql string
   # return number of record affected upon success
   def pgexec(self, sqlstr, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if self.PGLOG['DBGLEVEL']: self.pgdbg(100, sqlstr)
      ret = pgcnt = 0
      while True:
         pgcur = self.pgcursor()
         if not pgcur: return self.FAILURE
         try:
            pgcur.execute(sqlstr)
            ret = pgcur.rowcount
            pgcur.close()
         except PgSQL.Error as pgerr:
            if not self.check_dberror(pgerr, pgcnt, sqlstr, None, logact): return self.FAILURE
         else:
            break
         pgcnt += 1
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, "pgexec: {} record(s) affected for {}".format(ret, sqlstr))
      if logact&self.ENDLCK:
         self.endtran()
      elif self.curtran:
         self.curtran += ret
         if self.curtran > self.PGDBI['MTRANS']: self.starttran()
      return ret

   # tablename: one table name to a temporary table
   # fromtable: table name data gathing from
   #    fields: table name data gathing from
   # condition: querry conditions for where clause
   # return number of records created upon success
   def pgtemp(self, tablename, fromtable, fields, condition = None, logact = 0):
      sqlstr = "CREATE TEMPORARY TABLE {} SELECT {} FROM {}".format(tablename, fields, fromtable)
      if condition: sqlstr += " WHERE " + condition
      return self.pgexec(sqlstr, logact)

   # get condition for given table name for accessing information_schema
   def table_condition(self, tablename):
      ms = re.match(r'(.+)\.(.+)', tablename)
      if ms:
         scname = ms.group(1)
         tbname = ms.group(2)
      else:
         scname = self.PGDBI['SCNAME']
         tbname = tablename
      return "table_schema = '{}' AND table_name = '{}'".format(scname, tbname)

   # check if a given table name exists or not
   # tablename: one table name to check
   def pgcheck(self, tablename, logact = 0):
      condition = self.table_condition(tablename)
      ret = self.pgget('information_schema.tables', None, condition, logact)
      return (self.SUCCESS if ret else self.FAILURE)

   # group of functions to check parent records and add an empty one if missed
   # return user.uid upon success, 0 otherwise
   def check_user_uid(self, userno, date = None):
      if not userno: return 0
      if type(userno) is str: userno = int(userno)
      if date is None:
         datecond = "until_date IS NULL"
         date = 'today'
      else:
         datecond = "(start_date IS NULL OR start_date <= '{}') AND (until_date IS NULL OR until_date >= '{}')".format(date, date)
      pgrec = self.pgget("dssdb.user", "uid", "userno = {} AND {}".format(userno, datecond), self.PGDBI['ERRLOG'])
      if pgrec: return pgrec['uid']   
      if userno not in self.NMISSES:
         self.pglog("{}: Scientist ID NOT on file for {}".format(userno, date), self.LGWNEM)
         self.NMISSES.append(userno)
      # check again if a user is on file with different date range
      pgrec = self.pgget("dssdb.user", "uid", "userno = {}".format(userno), self.PGDBI['ERRLOG'])
      if pgrec: return pgrec['uid']
      pgrec = self.ucar_user_info(userno)
      if not pgrec: pgrec = {'userno': userno, 'stat_flag': 'M'}
      uid = self.pgadd("dssdb.user", pgrec, (self.PGDBI['EXITLG']|self.AUTOID))
      if uid: self.pglog("{}: Scientist ID Added as user.uid = {}".format(userno, uid), self.LGWNEM)
      return uid

   # return user.uid upon success, 0 otherwise
   def get_user_uid(self, logname, date = None):
      if not logname: return 0
      if not date:
         date = 'today'
         datecond = "until_date IS NULL"
      else:
         datecond = "(start_date IS NULL OR start_date <= '{}') AND (until_date IS NULL OR until_date >= '{}')".format(date, date)
      pgrec = self.pgget("dssdb.user", "uid", "logname = '{}' AND {}".format(logname, datecond), self.PGDBI['ERRLOG'])
      if pgrec: return pgrec['uid']   
      if logname not in self.LMISSES:
         self.pglog("{}: UCAR Login Name NOT on file for {}".format(logname, date), self.LGWNEM)
         self.LMISSES.append(logname)
      # check again if a user is on file with different date range
      pgrec = self.pgget("dssdb.user", "uid", "logname = '{}'".format(logname), self.PGDBI['ERRLOG'])
      if pgrec: return pgrec['uid']
      pgrec = self.ucar_user_info(0, logname)
      if not pgrec: pgrec = {'logname': logname, 'stat_flag': 'M'}
      uid = self.pgadd("dssdb.user", pgrec, (self.PGDBI['EXITLG']|self.AUTOID))
      if uid: self.pglog("{}: UCAR Login Name Added as user.uid = {}".format(logname, uid), self.LGWNEM)
      return uid

   # get ucar user info for given userno (scientist number) or logname (Ucar login)
   def ucar_user_info(self, userno, logname = None):
      matches = {
         'upid': "upid",
         'uid': "userno",
         'username': "logname",
         'lastName': "lstname",
         'firstName': "fstname",
         'active': "stat_flag",
         'internalOrg': "division",
         'externalOrg': "org_name",
         'country': "country",
         'forwardEmail': "email",
         'email': "ucaremail",
         'phone': "phoneno"
      }
      buf = self.pgsystem("pgperson " + ("-uid {}".format(userno) if userno else "-username {}".format(logname)), self.LOGWRN, 20)
      if not buf: return None
      pgrec = {}
      for line in buf.split('\n'):
         ms = re.match(r'^(.+)<=>(.*)$', line)
         if ms:
            (key, val) = ms.groups()
            if key in matches:
               if key == 'upid' and 'upid' in pgrec: break  # get one record only
               pgrec[matches[key]] = val   
      if not pgrec: return None
      if userno:
         pgrec['userno'] = userno
      elif pgrec['userno']:
         pgrec['userno'] = userno = int(pgrec['userno'])
      if pgrec['upid']: pgrec['upid'] = int(pgrec['upid'])
      if pgrec['stat_flag']: pgrec['stat_flag'] = 'A' if pgrec['stat_flag'] == "True" else 'C'
      if pgrec['email'] and re.search(r'(@|\.)ucar\.edu$', pgrec['email'], re.I):
         pgrec['email'] = pgrec['ucaremail']
         pgrec['org_name'] = 'NCAR'
      country = pgrec['country'] if 'country' in pgrec else None
      pgrec['country'] = self.set_country_code(pgrec['email'], country)
      if pgrec['division']:
         val = "NCAR"
      else:
         val = None
      pgrec['org_type'] = self.get_org_type(val, pgrec['email'])
      buf = self.pgsystem("pgusername {}".format(pgrec['logname']), self.LOGWRN, 20)
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

   #  set country code for given coutry name or email address
   def set_country_code(self, email, country = None):
      codes = {
         'CHINA': "P.R.CHINA",
         'ENGLAND': "UNITED.KINGDOM",
         'FR': "FRANCE",
         'KOREA': "SOUTH.KOREA",
         'USSR': "RUSSIA",
         'US': "UNITED.STATES",
         'U.S.A.': "UNITED.STATES"
      }
      if country:
         country = country.upper()
         ms = re.match(r'^(\w+)\s(\w+)$', country)
         if ms:
            country = ms.group(1) + '.' + ms.group(2)
         elif country in codes:
            country = codes[country]
      else:
         country = self.email_to_country(email)   
      return country
   
   # return wuser.wuid upon success, 0 otherwise
   def check_wuser_wuid(self, email, date = None):
      if not email: return 0
      emcond = "email = '{}'".format(email)
      if not date:
         date = 'today'
         datecond = "until_date IS NULL"
      else:
         datecond = "(start_date IS NULL OR start_date <= '{}') AND (until_date IS NULL OR until_date >= '{}')".format(date, date)
      pgrec = self.pgget("wuser", "wuid", "{} AND {}".format(emcond, datecond), self.PGDBI['ERRLOG'])
      if pgrec: return pgrec['wuid']   
      # check again if a user is on file with different date range
      pgrec = self.pgget("wuser", "wuid", emcond, self.LOGERR)
      if pgrec: return pgrec['wuid']
      # now add one in
      record = {'email': email}
      # check again if a ruser is on file
      pgrec = self.pgget("ruser", "*", emcond + " AND end_date IS NULL", self.PGDBI['ERRLOG'])
      if not pgrec: pgrec = self.pgget("ruser", "*", emcond, self.PGDBI['ERRLOG'])
      if pgrec:
         record['ruid'] = pgrec['id']
         record['fstname'] = pgrec['fname']
         record['lstname'] = pgrec['lname']
         record['country'] = pgrec['country']
         record['org_type'] = self.get_org_type(pgrec['org_type'], pgrec['email'])
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
         record['org_type'] = self.get_org_type('', email)
         record['country'] = self.email_to_country(email)
      wuid = self.pgadd("wuser", record, self.LOGERR|self.AUTOID)
      if wuid:
         if pgrec:
            self.pglog("{}({}, {}) Added as wuid({})".format(email, pgrec['lname'], pgrec['fname'], wuid), self.LGWNEM)
         else:
            self.pglog("{} Added as wuid({})".format(email, wuid), self.LGWNEM)
         return wuid   
      return 0
   
   # return wuser.wuid upon success, 0 otherwise
   def check_cdp_wuser(self, username):
      pgrec = self.pgget("wuser", "wuid", "cdpname = '{}'".format(username), self.PGDBI['EXITLG'])
      if pgrec: return pgrec['wuid']
      idrec = self.pgget("wuser", "wuid", "email = '{}'".format(pgrec['email']), self.PGDBI['EXITLG'])
      wuid = idrec['wuid'] if idrec else 0
      if wuid > 0:
         idrec = {}
         idrec['cdpid'] = pgrec['cdpid']
         idrec['cdpname'] = pgrec['cdpname']
         self.pgupdt("wuser", idrec, "wuid = {}".format(wuid) , self.PGDBI['EXITLG'])
      else:
         pgrec['stat_flag'] = 'A'
         pgrec['org_type'] = self.get_org_type(pgrec['org_type'], pgrec['email'])
         pgrec['country'] = self.email_to_country(pgrec['email'])
         wuid = self.pgadd("wuser", pgrec, self.PGDBI['EXITLG']|self.AUTOID)
         if wuid > 0:
            self.pglog("CDP User {} added as wuid = {} in RDADB".format(username, wuid), self.LGWNEM)
      return wuid

   # for given email to get long country name
   def email_to_country(self, email):
      ms = re.search(r'\.(\w\w)$', email)
      if ms:
         pgrec = self.pgget("countries", "token", "domain_id = '{}'".format(ms.group(1)), self.PGDBI['EXITLG'])
         if pgrec: return pgrec['token']
      elif re.search(r'\.(gov|edu|mil|org|com|net)$', email):
         return "UNITED.STATES"
      else:
         return "UNKNOWN"

   # if filelists is published for given dataset, reset it to 'P'
   def reset_rdadb_version(self, dsid):
      self.pgexec("UPDATE dataset SET version = version + 1 WHERE dsid = '{}'".format(dsid), self.PGDBI['ERRLOG'])
   
   # check the use rdadb flag in table dataset for a given dataset and given values
   def use_rdadb(self, dsid, logact = 0, vals = None):
      ret = ''   # default to empty in case dataset not in RDADB
      if dsid:
         pgrec = self.pgget("dataset", "use_rdadb", "dsid = '{}'".format(dsid), self.PGDBI['EXITLG'])
         if pgrec:
            ret = 'N'   # default to 'N' if dataset record in RDADB already
            if pgrec['use_rdadb']:
               if not vals: vals = "IPYMW"  # default to Internal; Publishable; Yes RDADB
               if vals.find(pgrec['use_rdadb']) > -1:
                  ret = pgrec['use_rdadb']
         elif logact:
            self.pglog("Dataset '{}' is not in RDADB!".format(dsid), logact)
      return ret

   #   fld: field name for querry condition
   #  vals: reference to aaray of values
   # isstr: 1 for string values requires quotes and support wildcard
   # noand: 1 for skiping the leading ' AND ' for condition
   # return a condition string for a given field
   def get_field_condition(self, fld, vals, isstr = 0, noand = 0):
      cnd = wcnd = negative = ''
      sign = "="
      logic = " OR "
      count =  len(vals) if vals else 0
      if count == 0: return ''
      ncnt = scnt = wcnt = cnt = 0
      for i in range(count):
         val = vals[i]
         if val is None or (i > 0 and val == vals[i-1]): continue
         if i == 0 and val == self.PGSIGNS[0]:
            negative = "NOT "
            logic = " AND "
            continue
         if scnt == 0 and isinstance(val, str):
            ms = re.match(r'^({})$'.format('|'.join(self.PGSIGNS[1:])), val)
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
         self.pglog("Need {} value{} after sign '{}'".format(scnt, s, osign), self.LGEREX)
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

   # build up fieldname string for given or default condition
   def fieldname_string(self, fnames, dnames = None, anames = None, wflds = None):
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

   # Function get_group_field_path(gindex: group index
   #                                 dsid: dataset id
   #                                field: path field name: webpath or savedpath)
   # go through group tree upward to find a none-empty path, return it or null
   def get_group_field_path(self, gindex, dsid, field):
      if gindex:
         pgrec = self.pgget("dsgroup", f"pindex, {field}",
                            f"dsid = '{dsid}' AND gindex = {gindex}", self.PGDBI['EXITLG'])
      else:
         pgrec = self.pgget("dataset", field, f"dsid = '{dsid}'", self.PGDBI['EXITLG'])
      if pgrec:
         if pgrec[field]:
            return pgrec[field]
         elif gindex:
            return self.get_group_field_path(pgrec['pindex'], dsid, field)
      else:
         return None

   # get the specialist info for a given dataset
   def get_specialist(self, dsid, logact = None):
      if logact is None: logact = self.PGDBI['ERRLOG']
      if dsid in self.SPECIALIST: return self.SPECIALIST['dsid']
   
      pgrec = self.pgget("dsowner, dssgrp", "specialist, lstname, fstname",
                    "specialist = logname AND dsid = '{}' AND priority = 1".format(dsid), logact)
      if pgrec:
         if pgrec['specialist'] == "datahelp" or pgrec['specialist'] == "dss":
            pgrec['lstname'] = "Help"
            pgrec['fstname'] = "Data"
      else:
         pgrec['specialist'] = "datahelp"
         pgrec['lstname'] = "Help"
         pgrec['fstname'] = "Data"
      self.SPECIALIST['dsid'] = pgrec  # cache specialist info for dsowner of dsid
      return pgrec

   #  build customized email from get_email()
   def build_customized_email(self, table, field, condition, subject, logact = 0):
      estat = self.FAILURE
      msg = self.get_email()
      if not msg: return estat
      sender = self.PGLOG['CURUID'] + "@ucar.edu"
      receiver = self.PGLOG['EMLADDR'] if self.PGLOG['EMLADDR'] else (self.PGLOG['CURUID'] + "@ucar.edu")
      if receiver.find(sender) < 0: self.add_carbon_copy(sender, 1)
      cc = self.PGLOG['CCDADDR']
      if not subject: subject = "Message from {}-{}".format(self.PGLOG['HOSTNAME'], self.get_command())
      estat = self.send_python_email(subject, receiver, msg, sender, cc, logact)
      if estat != self.SUCCESS:
         ebuf = "From: {}\nTo: {}\n".format(sender, receiver)
         if cc: ebuf += "Cc: {}\n".format(cc)
         ebuf += "Subject: {}!\n\n{}\n".format(subject, msg)
         if self.PGLOG['EMLSEND']:
            estat = self.send_customized_email(f"{table}.{condition}", ebuf, logact)
         if estat != self.SUCCESS:
            estat = self.cache_customized_email(table, field, condition, ebuf, 0)
            if estat and logact:
               self.pglog("Email {} cached to '{}.{}' for {}, Subject: {}".format(receiver, table, field, condition, subject), logact)
      return estat

   # email: full user email address
   # get user real name from table ruser for a given email address
   # opts == 1: include email
   # opts == 2: include org_type
   # opts == 4: include country
   # opts == 8: include valid_email
   # opts == 16: include org
   def get_ruser_names(self, email, opts = 0, date = None):
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
         date = time.strftime("%Y-%m-%d", (time.gmtime() if self.PGLOG['GMTZ'] else time.localtime()))
      emcnd = "email = '{}'".format(email)
      pgrec = self.pgget("ruser", fields, "{} AND {}".format(emcnd, datecond), self.LGEREX)
      if not pgrec:   # missing user record add one in
         self.pglog("{}: email not in ruser for {}".format(email, date), self.LOGWRN)
         # check again if a user is on file with different date range
         pgrec = self.pgget("ruser", fields, emcnd, self.LGEREX)
         if not pgrec and self.pgget("dssdb.user", '', emcnd):
            fields = "lstname, fstname"
            if opts&1: fields += ", email"
            if opts&2: fields += ", org_type"
            if opts&4: fields += ", country"
            if opts&8: fields += ", email valid_email"
            if opts&16: fields += ", org_name org"
            pgrec = self.pgget("dssdb.user", fields, emcnd, self.LGEREX)
      if pgrec and pgrec['lstname']:
         pgrec['name'] = (pgrec['fstname'].capitalize() + ' ') if pgrec['fstname'] else ''
         pgrec['name'] += pgrec['lstname'].capitalize()
      else:
         if not pgrec: pgrec = {}
         pgrec['name'] = email.split('@')[0]
         if opts&1: pgrec['email'] = email
      return pgrec

   # cache a customized email for sending it later
   def cache_customized_email(self, table, field, condition, emlmsg, logact = 0):
      pgrec = {field: emlmsg}
      if self.pgupdt(table, pgrec, condition, logact|self.ERRLOG):
         if logact: self.pglog("Email cached to '{}.{}' for {}".format(table, field, condition), logact&(~self.EXITLG))
         return self.SUCCESS
      else:
         msg = "cache email to '{}.{}' for {}".format(table, field, condition)
         self.pglog(f"Error {msg}, try to send directly now", logact|self.ERRLOG)
         return self.send_customized_email(msg, emlmsg, logact)

   # otype: user organization type
   # email: user email address)
   # return: orgonizaion type like DSS, NCAR, UNIV...
   def get_org_type(self, otype, email):
      if not otype: otype = "OTHER"
      if email:
         ms = re.search(r'(@|\.)ucar\.edu$', email)
         if ms:
            mc = ms.group(1)
            if otype == 'UCAR' or otype == 'OTHER': otype = 'NCAR'
            if otype == 'NCAR' and mc == '@':
               ms = re.match(r'^(.+)@', email)
               if ms and self.pgget("dssgrp", "", "logname = '{}'".format(ms.group(1))): otype = 'DSS'
         else:
            ms = re.search(r'\.(mil|org|gov|edu|com|net)(\.\w\w|$)', email)
            if ms:
               otype = ms.group(1).upper()
               if otype == 'EDU': otype = "UNIV"
      return otype

   # join values and handle the null values
   @staticmethod
   def join_values(vstr, vals):
      if vstr:
         vstr += "\n"
      elif vstr is None:
         vstr = ''
      return "{}Value{}({})".format(vstr, ('s' if len(vals) > 1 else ''), ', '.join(map(str, vals)))

   #  check table hostname to find the system down times. Cache the result for 10 minutes
   def get_system_downs(self, hostname, logact = 0):
      curtime = int(time.time())
      newhost = 0
      if hostname not in self.SYSDOWN:
         self.SYSDOWN[hostname] = {}
         newhost = 1
      if newhost or (curtime - self.SYSDOWN[hostname]['chktime']) > 600:
         self.SYSDOWN[hostname]['chktime'] = curtime
         self.SYSDOWN[hostname]['start'] = 0
         self.SYSDOWN[hostname]['end'] = 0
         self.SYSDOWN[hostname]['active'] = 1
         self.SYSDOWN[hostname]['path'] = None
         pgrec = self.pgget('hostname', 'service, domain, downstart, downend',
                       "hostname = '{}'".format(hostname), logact)
         if pgrec:
            if pgrec['service'] == 'N':
               self.SYSDOWN[hostname]['start'] = curtime
               self.SYSDOWN[hostname]['active'] = 0
            else:
               start = int(datetime.timestamp(pgrec['downstart'])) if pgrec['downstart'] else 0
               end = int(datetime.timestamp(pgrec['downend'])) if pgrec['downend'] else 0
               if start > 0 and (end == 0 or end > curtime):
                  self.SYSDOWN[hostname]['start'] = start
                  self.SYSDOWN[hostname]['end'] = end
               if pgrec['service'] == 'S' and pgrec['domain'] and re.match(r'^/', pgrec['domain']):
                  self.SYSDOWN[hostname]['path'] = pgrec['domain']
      self.SYSDOWN[hostname]['curtime'] = curtime
      return self.SYSDOWN[hostname]

   # return seconds for how long the system will continue to be down
   def system_down_time(self, hostname, offset, logact = 0):
      down = self.get_system_downs(hostname, logact)
      if down['start'] and down['curtime'] >= (down['start'] - offset):
         if not down['end']:
            if self.PGLOG['PGBATCH'] == self.PGLOG['PBSNAME']:
               return self.PGLOG['PBSTIME']
         elif down['curtime'] <= down['end']:
            return (down['end'] - down['curtime'])
      return 0  # the system is not down

   # return string message if the system is down
   def system_down_message(self, hostname, path, offset, logact = 0):
      down = self.get_system_downs(hostname, logact)
      msg = None
      if down['start'] and down['curtime'] >= (down['start'] - offset):
         match = self.match_down_path(path, down['path'])
         if match:
            msg = "{}{}:".format(hostname, ('-' + path) if match > 0 else '')
            if not down['active']:
               msg += " Not in Service"
            else:
               msg += " Planned down, started at " + self.current_datetime(down['start'])
               if not down['end']:
                  msg += " And no end time specified"
               elif down['curtime'] <= down['end']:
                  msg = " And will end by " + self.current_datetime(down['end'])
      return msg

   # return 1 if given path match daemon paths, 0 if not; -1 if cannot compare
   @staticmethod
   def match_down_path(path, dpaths):
      if not (path and dpaths): return -1
      paths = re.split(':', dpaths)
      for p in paths:
         if re.match(r'^{}'.format(p), path): return 1
      return 0

   # validate is login user is in DECS group
   # check all node if skpdsg is false, otherwise check non-DSG nodes
   def validate_decs_group(self, cmdname, logname, skpdsg):
      if skpdsg and self.PGLOG['DSGHOSTS'] and re.search(r'(^|:){}'.format(self.PGLOG['HOSTNAME']), self.PGLOG['DSGHOSTS']): return
      if not logname: lgname = self.PGLOG['CURUID']
      if not self.pgget("dssgrp", '', "logname = '{}'".format(logname), self.LGEREX):
         self.pglog("{}: Must be in DECS Group to run '{}' on {}".format(logname, cmdname, self.PGLOG['HOSTNAME']), self.LGEREX)

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
   # isarray -- if true, mutiple records provided via arrays for each hash key
   # docheck -- if 1, check and add only if record is not on file
   # docheck -- if 2, check and add if record is not on file, and update if exists
   # docheck -- if 4, check and add if record is not on file, and update if exists,
   #            and also checking NULL email value too
   def add_yearly_allusage(self, year, records, isarray = 0, docheck = 0):
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
               pgrec = self.pgget(tname, 'aidx', cnd, self.LOGERR|self.ADDTBL)
               if docheck == 4 and not pgrec:
                  cnd = "email IS NULL AND dsid = '{}' AND method = '{}' AND date = '{}' AND time = '{}'".format(
                         record['dsid'], record['method'], record['date'], record['time'])
                  pgrec = self.pgget(tname, 'aidx', cnd, self.LOGERR|self.ADDTBL)
               if pgrec:
                  if docheck > 1: acnt += self.pgupdt(tname, record, "aidx = {}".format(pgrec['aidx']), self.LGEREX)
               else:
                  acnt += self.pgadd(tname, record, self.LGEREX|self.ADDTBL)
         else:
            acnt = self.pgmadd(tname, records, self.LGEREX|self.ADDTBL)
      else:
         record = records
         if not ('quarter' in record and record['quarter']):
            ms = re.search(r'-(\d+)-', str(record['date']))
            if ms: record['quarter'] = int((int(ms.group(1))-1)/3)+1
         if docheck:
            cnd = "email = '{}' AND dsid = '{}' AND method = '{}' AND date = '{}' AND time = '{}'".format(
                   record['email'], record['dsid'], record['method'], record['date'], record['time'])
            pgrec = self.pgget(tname, 'aidx', cnd, self.LOGERR|self.ADDTBL)
            if docheck == 4 and not pgrec:
               cnd = "email IS NULL AND dsid = '{}' AND method = '{}' AND date = '{}' AND time = '{}'".format(
                      record['dsid'], record['method'], record['date'], record['time'])
               pgrec = self.pgget(tname, 'aidx', cnd, self.LOGERR|self.ADDTBL)
            if pgrec:
               if docheck > 1: acnt = self.pgupdt(tname, record, "aidx = {}".format(pgrec['aidx']), self.LGEREX)
               return acnt
         acnt = self.pgadd(tname, record, self.LGEREX|self.ADDTBL)
      return acnt

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
   # isarray -- if true, mutiple records provided via arrays for each hash key
   def add_yearly_wusage(self, year, records, isarray = 0):
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
         acnt = self.pgmadd(tname, records, self.LGEREX|self.ADDTBL)
      else:
         record = records
         if 'quarter' not in record:
            ms = re.search(r'-(\d+)-', str(record['date_read']))
            if ms: record['quarter'] = (int((int(ms.group(1))-1)/3)+1)
         acnt = self.pgadd(tname, record, self.LGEREX|self.ADDTBL)
      return acnt

   # double quote a array of single or sign delimited strings
   def pgnames(self, ary, sign = None, joinstr = None):
      pgary = []
      for a in ary:
         pgary.append(self.pgname(a, sign))
      if joinstr == None:
         return pgary
      else:
         return joinstr.join(pgary)

   # double quote a single or sign delimited string
   def pgname(self, str, sign = None):
      if sign:
         nstr = ''
         names = str.split(sign[0])
         for name in names:
            if nstr: nstr += sign[0]
            nstr += self.pgname(name, sign[1:])
      else:
         nstr = str.strip()
         if nstr and nstr.find('"') < 0:
            if not re.match(r'^[a-z_][a-z0-9_]*$', nstr) or nstr in self.PGRES:
             nstr = '"{}"'.format(nstr)
      return nstr

   # get a postgres password for given host, port, dbname, usname
   def get_pgpass_password(self):
      if self.PGDBI['PWNAME']: return self.PGDBI['PWNAME']
      pwname = self.get_baopassword()
      if not pwname: pwname = self.get_pgpassword()
      return pwname

   # get the pg passwords from file .pgpass
   def get_pgpassword(self):
      if not self.DBPASS: self.read_pgpass()
      dbport = str(self.PGDBI['DBPORT']) if self.PGDBI['DBPORT'] else '5432'
      pwname = self.DBPASS.get((self.PGDBI['DBSHOST'], dbport, self.PGDBI['DBNAME'], self.PGDBI['LNNAME']))
      if not pwname: pwname = self.DBPASS.get((self.PGDBI['DBHOST'], dbport, self.PGDBI['DBNAME'], self.PGDBI['LNNAME']))
      return pwname

   # get the pg passwords from OpenBao
   def get_baopassword(self):
      dbname = self.PGDBI['DBNAME']
      if dbname not in self.DBBAOS: self.read_openbao()
      return self.DBBAOS[dbname].get(self.PGDBI['LNNAME'])

   # Reads the .pgpass file and returns a dictionary of credentials.
   def read_pgpass(self):
      pgpass = self.PGLOG['DSSHOME'] + '/.pgpass'
      if not op.isfile(pgpass): pgpass = self.PGLOG['GDEXHOME'] + '/.pgpass'
      try:
         with open(pgpass, "r") as f:
            for line in f:
               line = line.strip()
               if not line or line.startswith("#"): continue
               dbhost, dbport, dbname, lnname, pwname = line.split(":")
               self.DBPASS[(dbhost, dbport, dbname, lnname)] = pwname
      except Exception as e:
          self.pglog(str(e), self.PGDBI['ERRLOG'])

   # Reads OpenBao secrets and returns a dictionary of credentials.
   def read_openbao(self):
      dbname = self.PGDBI['DBNAME']
      self.DBBAOS[dbname] = {}
      url = 'https://bao.k8s.ucar.edu/'
      baopath = {
         'ivaddb': 'gdex/pgdb03',
         'ispddb': 'gdex/pgdb03',
         'default': 'gdex/pgdb01'
      }
      dbpath = baopath[dbname] if dbname in baopath else baopath['default']
      client = hvac.Client(url=self.PGDBI.get('BAOURL'))
      client.token = self.PGLOG.get('BAOTOKEN')
      try:
         read_response = client.secrets.kv.v2.read_secret_version(
             path=dbpath,
             mount_point='kv',
             raise_on_deleted_version=False
         )
      except Exception as e:
         return self.pglog(str(e), self.PGDBI['ERRLOG'])
      baos = read_response['data']['data']
      for key in baos:
         ms = re.match(r'^(\w*)pass(\w*)$', key)
         if not ms: continue
         baoname = None
         pre = ms.group(1)
         suf = ms.group(2)
         if pre:
            baoname =  'metadata' if pre == 'meta' else pre
         elif suf == 'word':
            baoname = 'postgres'
         if baoname: self.DBBAOS[dbname][baoname] = baos[key] 
