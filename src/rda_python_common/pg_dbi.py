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
   """PostgreSQL Database Interface layer extending PgLOG.

   Provides a high-level API for connecting to and querying PostgreSQL databases
   using psycopg2. Supports single and batch INSERT, SELECT, UPDATE, and DELETE
   operations, transaction management, schema introspection, user lookups, usage
   tracking, and credential retrieval from .pgpass or OpenBao.

   Inherits all logging and utility helpers from PgLOG.

   Instance Attributes:
      pgdb (connection | None): Active psycopg2 connection, or None when disconnected.
      curtran (int): Transaction counter: 0 = idle, >0 = inside a transaction.
      NMISSES (list): Cached list of scientist IDs (userno) not found in the DB.
      LMISSES (list): Cached list of login names not found in the DB.
      TABLES (dict): Cache of table-field default info keyed by table name.
      SEQUENCES (dict): Cache of sequence field names keyed by table name.
      SPECIALIST (dict): Cache of specialist records keyed by dataset ID.
      SYSDOWN (dict): Cache of system-down status records keyed by hostname.
      PGDBI (dict): Active connection and configuration parameters.
      PGSIGNS (list): Special comparison sign tokens recognised by get_field_condition().
      CHCODE (int): psycopg2 type code for CHAR columns (used to strip trailing spaces).
      DBPORTS (dict): Mapping of database names to non-default TCP port numbers.
      DBPASS (dict): Credentials loaded from .pgpass, keyed by (host, port, db, user).
      DBBAOS (dict): Credentials loaded from OpenBao, keyed by database name.
      DBNAMES (dict): Mapping of schema names to their parent database names.
      DBSOCKS (dict): Mapping of database names to Unix socket paths.
      VIEWHOMES (dict): Mapping of hostnames to home directories for the view host.
      PGRES (list): Reserved PostgreSQL keywords that must be double-quoted as identifiers.
      ADDTBLS (list): Names of tables already created in this session (avoids duplicates).
   """

   def __init__(self):
      """Initialise PgDBI with default connection parameters and format helpers.

      Calls the parent PgLOG.__init__(), then sets up SQL timestamp format lambdas,
      connection configuration defaults (host, port, socket, schema, credentials),
      and operational limits (page size, max transaction size, max record count).
      Values are overridden by environment variables when present.
      """
      super().__init__()  # initialize parent class

      #  PostgreSQL specified query timestamp format
      self.fmtyr = lambda fn=self: "extract(year from {})::int".format(fn)
      self.fmtqt = lambda fn=self: "extract(quarter from {})::int".format(fn)
      self.fmtmn = lambda fn=self: "extract(month from {})::int".format(fn)
      self.fmtdt = lambda fn=self: "date({})".format(fn)
      self.fmtym = lambda fn=self: "to_char({}, 'yyyy-mm')".format(fn)
      self.fmthr = lambda fn=self: "extract(hour from {})::int".format(fn)

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

   def SETPGDBI(self, name, value):
      """Set a PGDBI configuration key, preferring the matching environment variable.

      Args:
         name (str): Configuration key to set in self.PGDBI.
         value: Default value to use when no matching environment variable exists.
      """
      self.PGDBI[name] = self.get_environment(name, value)

   def get_pgddl_command(self, tname, pre = None, suf = None, scname = None):
      """Build a pgddl shell command string for a given table.

      Args:
         tname (str): Table name, optionally prefixed with schema (e.g. 'schema.table').
         pre (str | None): Optional prefix appended with '-y' flag.
         suf (str | None): Optional suffix appended with '-x' flag.
         scname (str | None): Schema name override; parsed from tname when omitted.

      Returns:
         str: A pgddl command string ready for use with pgsystem().
      """
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

   def dssdb_dbname(self):
      """Switch the active connection to the default dssdb/dssdb schema."""
      self.default_scinfo(self.PGDBI['DEFDB'], self.PGDBI['DEFSC'], self.PGLOG['PSQLHOST'])
   dssdb_scname = dssdb_dbname

   def obsua_dbname(self):
      """Switch the active connection to the upadb/obsua schema on the misc host."""
      self.default_scinfo('upadb', 'obsua', self.PGLOG['PMISCHOST'])
   obsua_scname = obsua_dbname

   def ivaddb_dbname(self):
      """Switch the active connection to the ivaddb/ivaddb schema on the misc host."""
      self.default_scinfo('ivaddb', 'ivaddb', self.PGLOG['PMISCHOST'])
   ivaddb_scname = ivaddb_dbname

   def ispddb_dbname(self):
      """Switch the active connection to the ispddb/ispddb schema on the misc host."""
      self.default_scinfo('ispddb', 'ispddb', self.PGLOG['PMISCHOST'])
   ispddb_scname = ispddb_dbname

   def default_dbinfo(self, scname = None, dbhost = None, lnname = None, pwname = None, dbport = None, socket = None):
      """Set default connection info derived from a schema name.

      Looks up the parent database name for scname and delegates to default_scinfo().

      Args:
         scname (str | None): Schema name; uses current DEFSC when None.
         dbhost (str | None): Host override.
         lnname (str | None): Login name override.
         pwname (str | None): Password override.
         dbport (int | None): Port override.
         socket (str | None): Unix socket path override.
      """
      return self.default_scinfo(self.get_dbname(scname), scname, dbhost, lnname, pwname, dbport, socket)

   def default_scinfo(self, dbname = None, scname = None, dbhost = None, lnname = None, pwname = None, dbport = None, socket = None):
      """Set the active connection to hard-coded default values.

      Any argument left as None falls back to the corresponding PGDBI default
      (DEFDB, DEFSC, DEFHOST, DEFPORT, DEFSOCK). Disconnects if parameters changed.

      Args:
         dbname (str | None): Database name override.
         scname (str | None): Schema name override.
         dbhost (str | None): Host override.
         lnname (str | None): Login name override.
         pwname (str | None): Password override.
         dbport (int | None): Port override.
         socket (str | None): Unix socket path override.
      """
      if not dbname: dbname = self.PGDBI['DEFDB']
      if not scname: scname = self.PGDBI['DEFSC']
      if not dbhost: dbhost = self.PGDBI['DEFHOST']
      if dbport is None: dbport = self.PGDBI['DEFPORT']
      if socket is None:  socket = self.PGDBI['DEFSOCK']
      self.set_scname(dbname, scname, lnname, pwname, dbhost, dbport, socket)

   def get_dbsock(self, dbname):
      """Return the Unix socket path for a database, falling back to the default.

      Args:
         dbname (str): Database name to look up in DBSOCKS.

      Returns:
         str: Socket path, or the 'default' entry if dbname is not found.
      """
      return (self.DBSOCKS[dbname] if dbname in self.DBSOCKS else self.DBSOCKS['default'])

   def get_dbport(self, dbname):
      """Return the TCP port for a database, falling back to the default.

      Args:
         dbname (str): Database name to look up in DBPORTS.

      Returns:
         int: Port number, or the 'default' entry if dbname is not found.
      """
      return (self.DBPORTS[dbname] if dbname in self.DBPORTS else self.DBPORTS['default'])

   def get_dbname(self, scname):
      """Return the parent database name for a given schema name.

      Args:
         scname (str | None): Schema name to look up in DBNAMES.

      Returns:
         str | None: Resolved database name, the 'default' entry as fallback,
                     or None when scname is falsy.
      """
      if scname:
         if scname in self.DBNAMES: return self.DBNAMES[scname]
         return self.DBNAMES['default']
      return None

   def view_dbinfo(self, scname = None, lnname = None, pwname = None):
      """Set the active connection to the view host for read-only queries.

      Args:
         scname (str | None): Schema name; uses DEFSC when None.
         lnname (str | None): Login name override.
         pwname (str | None): Password override.
      """
      self.view_scinfo(self.get_dbname(scname), scname, lnname, pwname)

   def view_scinfo(self, dbname = None, scname = None, lnname = None, pwname = None):
      """Set the active connection to the view host with explicit database/schema names.

      Args:
         dbname (str | None): Database name; uses DEFDB when None.
         scname (str | None): Schema name; uses DEFSC when None.
         lnname (str | None): Login name override.
         pwname (str | None): Password override.
      """
      if not dbname: dbname = self.PGDBI['DEFDB']
      if not scname: scname = self.PGDBI['DEFSC']
      self.set_scname(dbname, scname, lnname, pwname, self.PGLOG['PVIEWHOST'], self.PGDBI['VWPORT'])

   def set_dbname(self, scname = None, lnname = None, pwname = None, dbhost = None, dbport = None, socket = None):
      """Set the active connection parameters derived from a schema name.

      Resolves the parent database from scname and calls set_scname().

      Args:
         scname (str | None): Schema name; uses DEFSC when None.
         lnname (str | None): Login name override.
         pwname (str | None): Password override.
         dbhost (str | None): Host override.
         dbport (int | None): Port override.
         socket (str | None): Unix socket path override.
      """
      if not scname: scname = self.PGDBI['DEFSC']
      self.set_scname(self.get_dbname(scname), scname, lnname, pwname, dbhost, dbport, socket)

   def set_scname(self, dbname = None, scname = None, lnname = None, pwname = None, dbhost = None, dbport = None, socket = None):
      """Update active connection parameters and disconnect if anything changed.

      Compares each supplied argument against the current PGDBI value and updates
      it when different. Automatically selects socket vs. port depending on whether
      the target host matches the local hostname. Calls pgdisconnect() when any
      parameter changes so the next operation reconnects with the new settings.

      Args:
         dbname (str | None): Database name override.
         scname (str | None): Schema name override (also resets LNNAME).
         lnname (str | None): Login name override.
         pwname (str | None): Password override (None is a meaningful value).
         dbhost (str | None): Host override.
         dbport (int | None): Port override.
         socket (str | None): Unix socket path override.
      """
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

   def starttran(self):
      """Begin a new database transaction.

      Ends any in-progress transaction first, then connects if not already connected,
      and disables autocommit so subsequent DML is grouped into a single transaction.
      """
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

   def endtran(self, autocommit = True):
      """Commit the current transaction and optionally restore autocommit mode.

      Args:
         autocommit (bool): When True (default) re-enables autocommit after commit
                            and resets curtran to 0; when False keeps curtran active.
      """
      if self.curtran and self.pgdb:
         if not self.pgdb.closed: self.pgdb.commit()
         self.pgdb.autocommit = autocommit
         self.curtran = 0 if autocommit else 1

   def aborttran(self, autocommit = True):
      """Roll back the current transaction without committing changes.

      Args:
         autocommit (bool): When True (default) re-enables autocommit after rollback
                            and resets curtran to 0; when False keeps curtran active.
      """
      if self.curtran and self.pgdb:
         if not self.pgdb.closed: self.pgdb.rollback()
         self.pgdb.autocommit = autocommit
      self.curtran = 0 if autocommit else 1

   def record_dscheck_error(self, errmsg, logact = None):
      """Write an error message to the dscheck record and release its process lock.

      Only updates the record when the current process still holds the lock
      (matched by host and PID). Sets status to 'E' and clears the PID when
      logact includes EXITLG.

      Args:
         errmsg (str): Error message to store in dscheck.errmsg.
         logact (int | None): Logging action flags; defaults to PGDBI['EXITLG'].

      Returns:
         int: Number of rows updated (SUCCESS/FAILURE).
      """
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

   def qelog(self, dberror, sleep, sqlstr, vals, pgcnt, logact = None):
      """Log a database query error and optionally sleep before a retry.

      Formats a human-readable message combining the DB error, retry context,
      SQL string, and bound values, then passes it to pglog(). When a dscheck
      record is active and logact includes EXITLG, also records the error there.

      Args:
         dberror (str): Raw database error string (pgcode + pgerror).
         sleep (int): Seconds to sleep after logging; 0 means no sleep.
         sqlstr (str): SQL statement or short retry description.
         vals: Bound parameter values shown in the log message.
         pgcnt (int): Retry attempt counter (0-based).
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         int: Always self.FAILURE so callers can use ``return self.qelog(...)``.
      """
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

   def try_add_table(self, dberror, logact):
      """Create a missing table when the DB error indicates it does not exist.

      Parses a '42P01 relation not found' error string to extract the table name,
      then calls add_new_table() to create it via pgddl.

      Args:
         dberror (str): Full database error string to inspect.
         logact (int): Logging action flags forwarded to add_new_table().
      """
      ms = re.match(r'^42P01 ERROR:  relation "(.+)" does not exist', dberror)
      if ms:
         tname = ms.group(1)
         self.add_new_table(tname, logact = logact)

   def add_a_table(self, tname, logact):
      """Create a new table by name (thin wrapper around add_new_table).

      Args:
         tname (str): Table name to create.
         logact (int): Logging action flags forwarded to add_new_table().
      """
      self.add_new_table(tname, logact = logact)

   def add_new_table(self, tname, pre = None, suf = None, logact = 0):
      """Create a table via pgddl, skipping if already created this session.

      Builds the final table name from tname combined with any prefix or suffix,
      checks ADDTBLS to avoid duplicate creation, then runs the pgddl command.

      Args:
         tname (str): Base table name (used as the pgddl target).
         pre (str | None): Prefix joined with '_' to form the final table name.
         suf (str | None): Suffix joined with '_' to form the final table name.
         logact (int): Logging action flags forwarded to pgsystem(); default 0.
      """
      if pre:
         tbname = '{}_{}'.format(pre, tname)
      elif suf:
         tbname = '{}_{}'.format(tname, suf)
      else:
         tbname = tname
      if tbname in self.ADDTBLS: return
      self.pgsystem(self.get_pgddl_command(tname, pre, suf), logact)
      self.ADDTBLS.append(tbname)

   def valid_table(self, tname, pre = None, suf = None, logact = 0):
      """Ensure a table exists, creating it via pgddl if necessary.

      Skips the existence check when the table was already created this session
      (tracked in ADDTBLS). Otherwise calls pgcheck() and runs pgddl when absent.

      Args:
         tname (str): Base table name.
         pre (str | None): Prefix joined with '_' to form the final table name.
         suf (str | None): Suffix joined with '_' to form the final table name.
         logact (int): Logging action flags; default 0.

      Returns:
         str: The resolved (possibly prefixed/suffixed) table name.
      """
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

   def check_dberror(self, pgerr, pgcnt, sqlstr, ary, logact = None):
      """Classify a psycopg2 error and decide whether to retry or abort.

      Handles connection errors (08xxx, 57xxx), lock errors (55xxx), aborted
      transactions (25P02), and missing-table errors (42P01 with ADDTBL flag).
      Retries up to PGLOG['DBRETRY'] times; exits after that threshold.

      Args:
         pgerr (psycopg2.Error): The caught database exception.
         pgcnt (int): Current retry count (0-based).
         sqlstr (str): SQL statement that caused the error, for logging.
         ary: Bound values that were passed to the statement, for logging.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         int: self.SUCCESS to signal the caller should retry, self.FAILURE to abort.
      """
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

   def pgbatch(self, sqlfile, foreground=0):
      """Build a psql batch command dict or foreground pipeline string.

      Sets the PGPASSWORD environment variable before constructing the command so
      psql can authenticate non-interactively.

      Args:
         sqlfile (str | None): Path to a SQL file to execute. When None or empty,
                               returns only the psql option string.
         foreground (int): When non-zero, returns a foreground pipeline string
                           (``psql ... < file |``). When 0, returns a dict with
                           keys 'cmd' (full shell command) and 'out' (output file path).

      Returns:
         str | dict: Option string when sqlfile is falsy; pipeline string when
                     foreground is set; otherwise a dict with 'cmd' and 'out' keys.
      """
      dbhost = 'localhost' if self.PGDBI['DBSHOST'] == self.PGLOG['HOSTNAME'] else self.PGDBI['DBHOST']
      options = "-h {} -p {}".format(dbhost, self.PGDBI['DBPORT'])
      os.environ['PGPASSWORD'] = self.get_pgpass_password()
      options += " -U {} {}".format(self.PGDBI['LNNAME'], self.PGDBI['DBNAME'])
      if not sqlfile: return options
      if foreground:
         return "psql {} < {} |".format(options, sqlfile)
      batch = {}
      batch['out'] = sqlfile
      if re.search(r'\.sql$', batch['out']):
         batch['out'] = re.sub(r'\.sql$', '.out', batch['out'])
      else:
         batch['out'] += ".out"
      batch['cmd'] = "psql {} < {} > {} 2>&1".format(options, sqlfile, batch['out'])
      return batch

   def pgconnect(self, reconnect = 0, pgcnt = 0, autocommit = True):
      """Connect to PostgreSQL and return the connection object.

      Skips reconnection when already connected unless reconnect is non-zero.
      Retries on transient errors up to the configured DBRETRY limit, using
      pgpass or OpenBao credentials for authentication.

      Args:
         reconnect (int): 0 = connect fresh; non-zero = reconnect only if closed.
         pgcnt (int): Internal retry counter (start at 0 for external callers).
         autocommit (bool): Whether to enable autocommit on the new connection.

      Returns:
         connection | int: psycopg2 connection on success, self.FAILURE on error.
      """
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

   def pgcursor(self):
      """Return a cursor with the active schema search path already set.

      Connects automatically if not yet connected. Retries on closed-connection
      errors. The search path includes PGDBI['SCPATH'] when it differs from SCNAME.

      Returns:
         cursor | int: psycopg2 cursor on success, self.FAILURE on error.
      """
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

   def pgdisconnect(self, stopit = 1):
      """Close the active database connection and clear the connection reference.

      Args:
         stopit (int): When non-zero (default), actually closes the connection.
                       Pass 0 to clear the reference without closing (e.g. after fork).
      """
      if self.pgdb:
         if stopit: self.pgdb.close()
         self.PGLOG['PGDBBUF'] = self.pgdb = None

   def pgtable(self, tablename, logact = None):
      """Return a dict of column default values for a table, with caching.

      Queries information_schema.columns for the table's column metadata and
      maps each column to its effective default value (0 for integers, '' for
      strings, None for nullable columns, 0 for sequence/serial columns).
      Results are cached in self.TABLES.

      Args:
         tablename (str): Fully-qualified (schema.table) or bare table name.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         dict | None: Mapping of column_name → default_value, or None on error.
      """
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

   def pgsequence(self, tablename, logact = None):
      """Return the name of the auto-increment (sequence/serial) column for a table.

      Queries information_schema.columns for a column whose default starts with
      'nextval('. Results are cached in self.SEQUENCES.

      Args:
         tablename (str): Fully-qualified or bare table name.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         str | None: Column name of the sequence field, or None if not found.
      """
      if logact is None: logact = self.PGDBI['ERRLOG']
      if tablename in self.SEQUENCES: return self.SEQUENCES[tablename]  # cached already
      condition = self.table_condition(tablename) + " AND column_default LIKE 'nextval(%'"
      pgrec = self.pgget('information_schema.columns', 'column_name', condition, logact)
      seqname = pgrec['column_name'] if pgrec else None
      self.SEQUENCES[tablename] = seqname
      return seqname

   @staticmethod
   def check_default_value(dflt, isint):
      """Normalise a raw column_default string from information_schema.

      Converts integer defaults to int, strips PostgreSQL type-cast notation from
      string defaults, and leaves other expressions unchanged.

      Args:
         dflt (str): Raw default expression from information_schema.columns.
         isint: Truthy when the column is an integer type.

      Returns:
         int | str: The normalised default value.
      """
      if isint:
         ms = re.match(r"^'{0,1}(\d+)", dflt)
         if ms: dflt = int(ms.group(1))
      elif dflt[0] == "'":
         ms = re.match(r"^(.+)::", dflt)
         if ms: dflt = ms.group(1)
      elif dflt != 'NULL':
         dflt = "'{}'".format(dflt)
      return dflt

   def prepare_insert(self, tablename, fields, multi = True, getid = None):
      """Build a parameterised INSERT SQL statement.

      Args:
         tablename (str): Target table name.
         fields (list[str]): Ordered list of column names to insert.
         multi (bool): When True uses a multi-value placeholder tuple; when False
                       uses a single %s (for execute_values).
         getid (str | None): Column name to return via RETURNING clause.

      Returns:
         str: Complete INSERT SQL string with %s placeholders.
      """
      strfld = self.pgnames(fields, '.', ',')
      if multi:
         strplc = "(" + ','.join(['%s']*len(fields)) + ")"
      else:
         strplc = '%s'
      sqlstr = "INSERT INTO {} ({}) VALUES {}".format(tablename, strfld, strplc)
      if getid: sqlstr += " RETURNING " + getid
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, sqlstr)
      return sqlstr

   def prepare_default(self, tablename, record, logact = 0):
      """Fill missing (None/empty) values in a single record dict with table defaults.

      Modifies record in-place: for each field whose value is None or an empty
      string, replaces it with the column default from pgtable().

      Args:
         tablename (str): Table name used to look up column defaults.
         record (dict): Mapping of column_name → value to be updated in-place.
         logact (int): Logging action flags forwarded to pgtable(); default 0.
      """
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

   def prepare_defaults(self, tablename, records, logact = 0):
      """Fill missing (None/empty) values in a multi-record dict with table defaults.

      Modifies records in-place: for each field and each position whose value is
      None or an empty string, replaces it with the column default from pgtable().

      Args:
         tablename (str): Table name used to look up column defaults.
         records (dict): Mapping of column_name → list-of-values, updated in-place.
         logact (int): Logging action flags forwarded to pgtable(); default 0.
      """
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

   def pgadd(self, tablename, record, logact = None, getid = None):
      """Insert a single record into a database table.

      Args:
         tablename (str): Target table name.
         record (dict): Mapping of column_name → value for the row to insert.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].
         getid (str | None): When set, returns the value of this RETURNING column
                             (typically the sequence/serial primary key).

      Returns:
         int | any: The RETURNING column value when getid is set; self.SUCCESS (1)
                    on a plain insert; self.FAILURE on error.
      """
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

   def pgmadd(self, tablename, records, logact = None, getid = None):
      """Insert multiple records into a database table efficiently.

      When getid is set, executes individual inserts to capture each returned ID.
      Otherwise uses psycopg2 execute_values() for a single bulk INSERT.

      Args:
         tablename (str): Target table name.
         records (dict): Mapping of column_name → list-of-values; all lists must
                         have the same length.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].
         getid (str | None): Column name to collect via RETURNING for each row.

      Returns:
         list | int: List of returned IDs when getid is set; count of rows inserted
                     otherwise; self.FAILURE on error.
      """
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

   def prepare_select(self, tablenames, fields = None, condition = None, cndflds = None, logact = 0):
      """Build a SELECT (or raw) SQL statement from components.

      When tablenames is provided, constructs a full SELECT…FROM…WHERE statement.
      When only fields is provided (no tablenames), returns ``SELECT <fields>``.
      When only condition is provided, returns condition verbatim (raw SQL).
      Appends ``FOR UPDATE`` and starts a transaction when DOLOCK is set.

      Args:
         tablenames (str | None): Comma-separated table names for the FROM clause.
         fields (str | None): Comma-separated column expressions; None → COUNT(*).
         condition (str | None): WHERE clause string or ORDER/GROUP/LIMIT suffix.
         cndflds (list | None): Column names for parameterised WHERE clauses (%s).
         logact (int): Logging action flags; default 0.

      Returns:
         str: Complete SQL statement string.
      """
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

   def pgget(self, tablenames, fields, condition = None, logact = 0):
      """Fetch a single row from one or more tables.

      Appends LIMIT 1 automatically. Returns a count integer when fields is None,
      otherwise returns a dict of column_name → value (empty dict when no row found).
      CHAR columns are right-stripped. Column names are upper-cased when UCNAME is set.

      Args:
         tablenames (str): Comma-separated table names (supports JOINs via WHERE).
         fields (str | None): Comma-separated column expressions; None → row count.
         condition (str | None): WHERE / ORDER / LIMIT clause.
         logact (int): Logging action flags; default PGDBI['ERRLOG'].

      Returns:
         dict | int | int: Row dict, count integer, or self.FAILURE on error.
      """
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

   def pgmget(self, tablenames, fields, condition = None, logact = None):
      """Fetch multiple rows from one or more tables.

      Returns results as a column-oriented dict: each key is a column name and its
      value is a list of that column's values across all returned rows. CHAR columns
      are right-stripped. Column names are upper-cased when UCNAME is set.

      Args:
         tablenames (str): Comma-separated table names.
         fields (str | None): Comma-separated column expressions.
         condition (str | None): WHERE / ORDER / LIMIT clause.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         dict | int: Column-oriented result dict (may be empty), or self.FAILURE.
      """
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

   def pghget(self, tablenames, fields, cnddict, logact = None):
      """Fetch a single row using a condition dict (parameterised query).

      Builds a WHERE clause from the keys of cnddict and binds its values via %s
      placeholders, avoiding SQL injection. Appends LIMIT 1 automatically.

      Args:
         tablenames (str): Comma-separated table names.
         fields (str): Comma-separated column expressions.
         cnddict (dict): Mapping of column_name → value used for the WHERE clause.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         dict | int: Row dict, count integer, or self.FAILURE on error.
      """
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

   def pgmhget(self, tablenames, fields, cnddicts, logact = None):
      """Fetch multiple rows using a multi-value condition dict (parameterised).

      Executes one query per row of condition values and accumulates results into a
      single column-oriented dict. Useful for bulk lookups with varying WHERE values.

      Args:
         tablenames (str): Comma-separated table names.
         fields (str): Comma-separated column expressions.
         cnddicts (dict): Mapping of column_name → list-of-values; each position
                          forms one WHERE clause evaluation.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         dict | int: Accumulated column-oriented result dict, or self.FAILURE.
      """
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

   def prepare_update(self, tablename, fields, condition = None, cndflds = None):
      """Build a parameterised UPDATE SQL statement.

      Accepts either a raw condition string or a list of condition field names.
      Field names containing a dot separator are double-quoted appropriately via pgname().

      Args:
         tablename (str): Table to update.
         fields (list[str]): Column names to set (SET col=%s …).
         condition (str | None): Raw WHERE clause string.
         cndflds (list | None): Column names for parameterised WHERE clause.

      Returns:
         str: Complete UPDATE SQL string with %s placeholders.
      """
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

   def pgupdt(self, tablename, record, condition, logact = None):
      """Update rows in a table using a raw WHERE condition string.

      Args:
         tablename (str): Target table name.
         record (dict): Mapping of column_name → new value.
         condition (str): WHERE clause string (must not be empty or numeric).
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         int: Number of rows updated, or self.FAILURE on error.
      """
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

   def pghupdt(self, tablename, record, cnddict, logact = None):
      """Update rows in a table using a condition dict (parameterised).

      Args:
         tablename (str): Target table name.
         record (dict): Mapping of column_name → new value.
         cnddict (dict): Mapping of column_name → value for the WHERE clause.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         int: Number of rows updated, or self.FAILURE on error.
      """
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

   def pgmupdt(self, tablename, records, cnddicts, logact = None):
      """Update multiple rows using parallel value and condition dicts.

      Uses psycopg2 execute_batch() for efficient bulk updates. The number of
      values in records and cnddicts must match.

      Args:
         tablename (str): Target table name.
         records (dict): Mapping of column_name → list-of-new-values.
         cnddicts (dict): Mapping of column_name → list-of-condition-values.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         int: Number of rows updated, or self.FAILURE on error.
      """
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

   def prepare_delete(self, tablename, condition = None, cndflds = None):
      """Build a parameterised DELETE SQL statement.

      Args:
         tablename (str): Table to delete from.
         condition (str | None): Raw WHERE clause string.
         cndflds (list | None): Column names for parameterised WHERE clause.

      Returns:
         str: Complete DELETE SQL string with %s placeholders.
      """
      # build condition string
      if not condition:
         cndset = []
         for fld in cndflds:
            cndset.append("{}=%s".format(fld))
         condition = " AND ".join(cndset)
      sqlstr = "DELETE FROM {} WHERE {}".format(tablename, condition)
      if self.PGLOG['DBGLEVEL']: self.pgdbg(1000, sqlstr)
      return sqlstr

   def pgdel(self, tablename, condition, logact = None):
      """Delete rows from a table using a raw WHERE condition string.

      Args:
         tablename (str): Target table name.
         condition (str): WHERE clause (must not be empty or numeric).
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         int: Number of rows deleted, or self.FAILURE on error.
      """
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

   def pghdel(self, tablename, cnddict, logact = None):
      """Delete rows from a table using a condition dict (parameterised).

      Args:
         tablename (str): Target table name.
         cnddict (dict): Mapping of column_name → value for the WHERE clause.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         int: Number of rows deleted, or self.FAILURE on error.
      """
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

   def pgmdel(self, tablename, cnddicts, logact = None):
      """Delete multiple rows using a multi-value condition dict.

      Uses psycopg2 execute_batch() for efficient bulk deletes.

      Args:
         tablename (str): Target table name.
         cnddicts (dict): Mapping of column_name → list-of-condition-values.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         int: Number of rows deleted, or self.FAILURE on error.
      """
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

   def pgexec(self, sqlstr, logact = None):
      """Execute a raw SQL statement and return the affected row count.

      Use for DDL or DML that does not fit the structured helpers (e.g. TRUNCATE,
      custom UPDATE with subqueries). Not suitable for SELECT queries.

      Args:
         sqlstr (str): Complete SQL statement to execute.
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         int: rowcount from the cursor, or self.FAILURE on error.
      """
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

   def pgtemp(self, tablename, fromtable, fields, condition = None, logact = 0):
      """Create a temporary table populated from a SELECT query.

      Args:
         tablename (str): Name for the new temporary table.
         fromtable (str): Source table name.
         fields (str): Column expressions for the SELECT.
         condition (str | None): Optional WHERE clause.
         logact (int): Logging action flags; default 0.

      Returns:
         int: Number of rows created, or self.FAILURE on error.
      """
      sqlstr = "CREATE TEMPORARY TABLE {} SELECT {} FROM {}".format(tablename, fields, fromtable)
      if condition: sqlstr += " WHERE " + condition
      return self.pgexec(sqlstr, logact)

   def table_condition(self, tablename):
      """Build an information_schema WHERE condition for a given table name.

      Splits schema-qualified names (schema.table) or uses the current SCNAME.

      Args:
         tablename (str): Fully-qualified or bare table name.

      Returns:
         str: Condition string suitable for querying information_schema.tables
              or information_schema.columns.
      """
      ms = re.match(r'(.+)\.(.+)', tablename)
      if ms:
         scname = ms.group(1)
         tbname = ms.group(2)
      else:
         scname = self.PGDBI['SCNAME']
         tbname = tablename
      return "table_schema = '{}' AND table_name = '{}'".format(scname, tbname)

   def pgcheck(self, tablename, logact = 0):
      """Check whether a table exists in the current schema.

      Args:
         tablename (str): Fully-qualified or bare table name.
         logact (int): Logging action flags; default 0.

      Returns:
         int: self.SUCCESS if the table exists, self.FAILURE otherwise.
      """
      condition = self.table_condition(tablename)
      ret = self.pgget('information_schema.tables', None, condition, logact)
      return (self.SUCCESS if ret else self.FAILURE)

   def check_user_uid(self, userno, date = None):
      """Return the user.uid for a scientist ID, adding a record if missing.

      Looks up the active user record for userno on the given date. If not found,
      logs a warning, attempts a date-range-independent lookup, and finally fetches
      UCAR person info to insert a new user record.

      Args:
         userno (int | str): UCAR scientist number.
         date (str | None): Reference date (YYYY-MM-DD); None means today
                            (uses until_date IS NULL condition).

      Returns:
         int: user.uid on success, 0 if userno is falsy or insert fails.
      """
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

   def get_user_uid(self, logname, date = None):
      """Return the user.uid for a UCAR login name, adding a record if missing.

      Similar to check_user_uid() but looks up by logname instead of userno.

      Args:
         logname (str): UCAR login name.
         date (str | None): Reference date (YYYY-MM-DD); None means today.

      Returns:
         int: user.uid on success, 0 if logname is falsy or insert fails.
      """
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

   def ucar_user_info(self, userno, logname = None):
      """Fetch UCAR person info for a scientist ID or login name via pgperson/pgusername.

      Runs the pgperson command-line tool and parses its key<=>value output.
      Maps UCAR API fields to database column names, normalises country code,
      organisation type, and employment dates.

      Args:
         userno (int): Scientist number; pass 0 to look up by logname instead.
         logname (str | None): UCAR login name; used when userno is 0.

      Returns:
         dict | None: Mapping of column_name → value suitable for pgadd('dssdb.user'),
                      or None when pgperson returns no output.
      """
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

   def set_country_code(self, email, country = None):
      """Normalise a country name or derive it from an email domain.

      Applies a correction table for common aliases (e.g. 'ENGLAND' → 'UNITED.KINGDOM'),
      joins two-word country names with a dot, and falls back to email_to_country()
      when country is None.

      Args:
         email (str): User email address used as fallback for country detection.
         country (str | None): Raw country string to normalise.

      Returns:
         str: Normalised country name (e.g. 'UNITED.STATES', 'FRANCE').
      """
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
   
   def check_wuser_wuid(self, email, date = None):
      """Return the wuser.wuid for an email address, inserting a record if missing.

      Searches wuser by email and active date range, then falls back to a
      cross-table lookup in ruser and dssdb.user to populate the new record.

      Args:
         email (str): User email address.
         date (str | None): Reference date (YYYY-MM-DD); None means today.

      Returns:
         int: wuser.wuid on success, 0 if email is falsy or insert fails.
      """
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
   
   def check_cdp_wuser(self, username):
      """Return or create the wuser.wuid for a CDP username.

      Looks up by cdpname first, then by email. Updates cdpid/cdpname on an
      existing record or inserts a new one.

      Args:
         username (str): CDP (Collaborative Data Portal) username.

      Returns:
         int: wuser.wuid on success, 0 on failure.
      """
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

   def email_to_country(self, email):
      """Infer a country name from an email address domain.

      Checks for a two-letter country-code TLD and looks it up in the countries
      table. Recognises common US TLDs (.gov, .edu, .mil, .org, .com, .net).

      Args:
         email (str): Email address to inspect.

      Returns:
         str: Country name (e.g. 'UNITED.STATES', 'GERMANY'), or 'UNKNOWN'.
      """
      ms = re.search(r'\.(\w\w)$', email)
      if ms:
         pgrec = self.pgget("countries", "token", "domain_id = '{}'".format(ms.group(1)), self.PGDBI['EXITLG'])
         if pgrec: return pgrec['token']
      elif re.search(r'\.(gov|edu|mil|org|com|net)$', email):
         return "UNITED.STATES"
      else:
         return "UNKNOWN"

   def reset_rdadb_version(self, dsid):
      """Increment the version counter for a dataset record in RDADB.

      Args:
         dsid (str): Dataset ID (e.g. 'd123000').
      """
      self.pgexec("UPDATE dataset SET version = version + 1 WHERE dsid = '{}'".format(dsid), self.PGDBI['ERRLOG'])
   
   def use_rdadb(self, dsid, logact = 0, vals = None):
      """Return the use_rdadb flag for a dataset if it matches an allowed set.

      Args:
         dsid (str | None): Dataset ID to query.
         logact (int): Logging action flags for missing-dataset warnings; default 0.
         vals (str | None): Accepted flag characters; defaults to 'IPYMW' when None.

      Returns:
         str: The use_rdadb flag character when found and in vals; 'N' when the
              dataset exists but the flag is not in vals; '' when dsid is falsy
              or the dataset is not in RDADB.
      """
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

   def get_field_condition(self, fld, vals, isstr = 0, noand = 0):
      """Build a SQL condition fragment for a field given a list of values.

      Supports equality, range (IN), comparison signs (<, >, <>), LIKE, SIMILAR TO,
      and negation (leading '!' in vals). Multiple values are combined with IN or
      the appropriate comparison. Prepends ' AND ' unless noand is set.

      Args:
         fld (str): Column name for the condition.
         vals (list): Values to match; may include sign tokens from PGSIGNS.
         isstr (int): 1 to treat values as strings (adds quotes, handles wildcards).
         noand (int): 1 to omit the leading ' AND ' prefix.

      Returns:
         str: SQL condition fragment, empty string when vals is empty.
      """
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

   def fieldname_string(self, fnames, dnames = None, anames = None, wflds = None):
      """Resolve a field-name string and insert any required with-fields.

      Returns dnames when fnames is falsy, anames when fnames is 'ALL' (case-insensitive),
      otherwise uses fnames as-is. Then inserts wflds entries at appropriate positions.

      Args:
         fnames (str | None): Requested field names string.
         dnames (str | None): Default field names string.
         anames (str | None): All-fields string used when fnames == 'all'.
         wflds (list | None): Additional fields to insert at specific positions.

      Returns:
         str | None: Resolved field-names string.
      """
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

   def get_group_field_path(self, gindex, dsid, field):
      """Walk the group tree upward to find the first non-empty path field.

      Recursively follows pindex links from the given group up to the dataset
      level until a non-empty value for field is found.

      Args:
         gindex (int | None): Group index to start from; None or 0 queries dataset.
         dsid (str): Dataset ID.
         field (str): Column name to retrieve (e.g. 'webpath' or 'savedpath').

      Returns:
         str | None: First non-empty path value found, or None.
      """
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

   def get_specialist(self, dsid, logact=None):
      """Return specialist contact info for a dataset, with caching.

      Queries dsowner and dssgrp to find the primary specialist for dsid.
      Falls back to 'datahelp' / 'Data Help' when none is found. Results
      are cached in self.SPECIALIST keyed by dsid.

      Args:
         dsid (str): Dataset ID (e.g. 'd123000').
         logact (int | None): Logging action flags; defaults to PGDBI['ERRLOG'].

      Returns:
         dict | None: Record with keys 'specialist', 'lstname', 'fstname',
                      or None on query error.
      """
      if logact is None: logact = self.PGDBI['ERRLOG']
      if dsid in self.SPECIALIST: return self.SPECIALIST[dsid]
   
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
      self.SPECIALIST[dsid] = pgrec  # cache specialist info for dsowner of dsid
      return pgrec

   def build_customized_email(self, table, field, condition, subject, logact = 0):
      """Send a buffered email, falling back to DB caching on failure.

      Retrieves the accumulated email body from get_email(), addresses it,
      and sends via send_python_email(). On failure, tries send_customized_email()
      and finally cache_customized_email() to store the message for later delivery.

      Args:
         table (str): Table name used for cache fallback storage.
         field (str): Column name used for cache fallback storage.
         condition (str): WHERE condition identifying the cache row.
         subject (str | None): Email subject; auto-generated when None.
         logact (int): Logging action flags; default 0.

      Returns:
         int: self.SUCCESS on successful send or cache, self.FAILURE otherwise.
      """
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

   def get_ruser_names(self, email, opts = 0, date = None):
      """Retrieve user name fields from ruser (or dssdb.user) for an email address.

      Selects the active record for email on date, adding a derived 'name' key
      (First Last). Falls back to dssdb.user when ruser has no match.

      Args:
         email (str): User email address.
         opts (int): Bitmask to include extra fields:
                     1=email, 2=org_type, 4=country, 8=valid_email, 16=org.
         date (str | None): Reference date (YYYY-MM-DD); None means today.

      Returns:
         dict: Record with at least 'name'; may include extra fields per opts.
      """
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

   def cache_customized_email(self, table, field, condition, emlmsg, logact = 0):
      """Store an email message in a database column for later delivery.

      Attempts a pgupdt() to write emlmsg into table.field for condition. Falls
      back to send_customized_email() when the update fails.

      Args:
         table (str): Target table name.
         field (str): Column to store the email body in.
         condition (str): WHERE condition identifying the target row.
         emlmsg (str): Full email message text (headers + body).
         logact (int): Logging action flags; default 0.

      Returns:
         int: self.SUCCESS on update, result of send_customized_email() on failure.
      """
      pgrec = {field: emlmsg}
      if self.pgupdt(table, pgrec, condition, logact|self.ERRLOG):
         if logact: self.pglog("Email cached to '{}.{}' for {}".format(table, field, condition), logact&(~self.EXITLG))
         return self.SUCCESS
      else:
         msg = "cache email to '{}.{}' for {}".format(table, field, condition)
         self.pglog(f"Error {msg}, try to send directly now", logact|self.ERRLOG)
         return self.send_customized_email(msg, emlmsg, logact)

   def get_org_type(self, otype, email):
      """Derive an organisation type from a type hint and email domain.

      Recognises UCAR/NCAR addresses and refines NCAR to DSS for DECS group members.
      Maps common TLDs to org types (EDU→UNIV, GOV, MIL, ORG, COM, NET).

      Args:
         otype (str | None): Initial organisation type hint; defaults to 'OTHER'.
         email (str | None): Email address used for TLD/domain inference.

      Returns:
         str: Normalised organisation type string (e.g. 'DSS', 'NCAR', 'UNIV').
      """
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

   @staticmethod
   def join_values(vstr, vals):
      """Append a formatted value list to an existing string.

      Builds a line like 'Value(a, b, c)' or 'Values(a, b)' and appends it to
      vstr (newline-separated). Treats None vstr as an empty string.

      Args:
         vstr (str | None): Existing accumulated string, or None to start fresh.
         vals (list): Values to include in the appended line.

      Returns:
         str: Updated string with the new value line appended.
      """
      if vstr:
         vstr += "\n"
      elif vstr is None:
         vstr = ''
      return "{}Value{}({})".format(vstr, ('s' if len(vals) > 1 else ''), ', '.join(map(str, vals)))

   def get_system_downs(self, hostname, logact = 0):
      """Return the cached system-down status for a hostname, refreshing every 10 minutes.

      Queries the hostname table for service status, planned downtime start/end, and
      optional path restrictions. Results are stored in self.SYSDOWN[hostname].

      Args:
         hostname (str): Short hostname to query.
         logact (int): Logging action flags; default 0.

      Returns:
         dict: Status dict with keys: 'start', 'end', 'active', 'path',
               'chktime', 'curtime'.
      """
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

   def system_down_time(self, hostname, offset, logact = 0):
      """Return the number of seconds a system will continue to be down.

      Uses cached data from get_system_downs(). Returns PBS job time when the
      system is down indefinitely and the caller is a PBS batch job.

      Args:
         hostname (str): Host to check.
         offset (int): Seconds before the scheduled start to consider the system down.
         logact (int): Logging action flags; default 0.

      Returns:
         int: Remaining down seconds, or 0 when the system is up.
      """
      down = self.get_system_downs(hostname, logact)
      if down['start'] and down['curtime'] >= (down['start'] - offset):
         if not down['end']:
            if self.PGLOG['PGBATCH'] == self.PGLOG['PBSNAME']:
               return self.PGLOG['PBSTIME']
         elif down['curtime'] <= down['end']:
            return (down['end'] - down['curtime'])
      return 0  # the system is not down

   def system_down_message(self, hostname, path, offset, logact = 0):
      """Return a human-readable downtime message, or None when the system is up.

      Args:
         hostname (str): Host to check.
         path (str | None): Service path to match against scheduled-down paths.
         offset (int): Seconds before scheduled start to report as down.
         logact (int): Logging action flags; default 0.

      Returns:
         str | None: Downtime description string, or None when the system is up
                     or the path does not match.
      """
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

   @staticmethod
   def match_down_path(path, dpaths):
      """Check whether a service path matches any colon-separated down paths.

      Args:
         path (str | None): Service path to test.
         dpaths (str | None): Colon-separated list of path prefixes to match.

      Returns:
         int: 1 if path matches a prefix in dpaths, 0 if no match,
              -1 if either argument is falsy.
      """
      if not (path and dpaths): return -1
      paths = re.split(':', dpaths)
      for p in paths:
         if re.match(r'^{}'.format(p), path): return 1
      return 0

   def validate_decs_group(self, cmdname, logname, skpdsg):
      """Verify that a login name belongs to the DECS group, exiting if not.

      When skpdsg is True and the current host is in PGLOG['DSGHOSTS'], the check
      is skipped (DSG nodes are exempt). Falls back to the current user (CURUID)
      when logname is not supplied.

      Args:
         cmdname (str): Name of the command being run, used in the error message.
         logname (str | None): Login name to validate; uses CURUID when falsy.
         skpdsg (bool | int): When True, skip the check on DSG-designated hosts.
      """
      if skpdsg and self.PGLOG['DSGHOSTS'] and re.search(r'(^|:){}'.format(self.PGLOG['HOSTNAME']), self.PGLOG['DSGHOSTS']): return
      if not logname: logname = self.PGLOG['CURUID']
      if not self.pgget("dssgrp", '', "logname = '{}'".format(logname), self.LGEREX):
         self.pglog("{}: Must be in DECS Group to run '{}' on {}".format(logname, cmdname, self.PGLOG['HOSTNAME']), self.LGEREX)

   def add_yearly_allusage(self, year, records, isarray = 0, docheck = 0):
      """Insert or upsert usage records into the per-year allusage_YYYY table.

      Creates the table via pgddl (ADDTBL flag) when it does not yet exist.
      Auto-computes the quarter from the date field when not supplied.

      Args:
         year (int | str | None): Four-digit year; derived from records['date'] when 0/None.
         records (dict): Usage data. Keys match allusage columns; see class docstring
                         or inline comment for the full field list.
         isarray (int): 0 for a single record dict, 1 for parallel value lists.
         docheck (int): 0=insert always; 1=skip if exists; 2=upsert; 4=upsert+NULL email.

      Returns:
         int: Number of rows inserted or updated.
      """
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

   def add_yearly_wusage(self, year, records, isarray = 0):
      """Insert web-usage records into the per-year wusage_YYYY table.

      Creates the table via pgddl (ADDTBL flag) when it does not yet exist.
      Auto-computes the quarter from date_read when not supplied.

      Args:
         year (int | str | None): Four-digit year; derived from records['date_read'] when 0/None.
         records (dict): Web-usage data. Keys match wusage columns; see class docstring
                         or inline comment for the full field list.
         isarray (int): 0 for a single record dict, 1 for parallel value lists.

      Returns:
         int: Number of rows inserted.
      """
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

   def pgnames(self, ary, sign=None, joinstr=None):
      """Double-quote a list of identifiers, optionally joining them.

      Delegates each name to pgname() for quoting, then either returns the list
      or joins it with joinstr.

      Args:
         ary (list[str]): Identifier strings to process.
         sign (str | None): Separator character(s) forwarded to pgname() for
                            schema.table or table.column splitting.
         joinstr (str | None): String to join the results with; None returns a list.

      Returns:
         list[str] | str: List of quoted names when joinstr is None, otherwise a
                          single joined string.
      """
      pgary = []
      for a in ary:
         pgary.append(self.pgname(a, sign))
      if joinstr is None:
         return pgary
      else:
         return joinstr.join(pgary)

   def pgname(self, str, sign = None):
      """Double-quote a single identifier or a sign-delimited compound identifier.

      Recursively splits on the first character of sign (e.g. '.' for schema.table),
      quotes each component, then reassembles. A component is quoted only when it
      contains characters outside [a-z0-9_], starts with a digit, or is a reserved
      PostgreSQL word listed in PGRES.

      Args:
         str (str): Identifier string to process.
         sign (str | None): Separator string; None treats str as a simple identifier.

      Returns:
         str: Properly double-quoted identifier string.
      """
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

   def get_pgpass_password(self):
      """Return the database password for the current connection settings.

      Checks PGDBI['PWNAME'] first, then tries OpenBao (get_baopassword()), and
      finally falls back to the .pgpass file (get_pgpassword()).

      Returns:
         str | None: Password string, or None when no credential is found.
      """
      if self.PGDBI['PWNAME']: return self.PGDBI['PWNAME']
      pwname = self.get_baopassword()
      if not pwname: pwname = self.get_pgpassword()
      return pwname

   def get_pgpassword(self):
      """Look up the password in the cached .pgpass credentials.

      Tries matching on short hostname first, then full hostname, using the
      current DBPORT (defaulting to 5432), DBNAME, and LNNAME.

      Returns:
         str | None: Password string, or None when no matching entry is found.
      """
      if not self.DBPASS: self.read_pgpass()
      dbport = str(self.PGDBI['DBPORT']) if self.PGDBI['DBPORT'] else '5432'
      pwname = self.DBPASS.get((self.PGDBI['DBSHOST'], dbport, self.PGDBI['DBNAME'], self.PGDBI['LNNAME']))
      if not pwname: pwname = self.DBPASS.get((self.PGDBI['DBHOST'], dbport, self.PGDBI['DBNAME'], self.PGDBI['LNNAME']))
      return pwname

   def get_baopassword(self):
      """Look up the password from OpenBao for the current database and login name.

      Loads OpenBao secrets for PGDBI['DBNAME'] on first call (or when not cached).

      Returns:
         str | None: Password string, or None when not found in OpenBao.
      """
      dbname = self.PGDBI['DBNAME']
      if dbname not in self.DBBAOS: self.read_openbao()
      return self.DBBAOS[dbname].get(self.PGDBI['LNNAME'])

   def read_pgpass(self):
      """Read the .pgpass file and populate DBPASS with credentials.

      Searches for .pgpass in DSSHOME first, then GDEXHOME. Each entry is parsed
      into a (host, port, dbname, username) tuple key mapping to a password value.
      """
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

   def read_openbao(self):
      """Read OpenBao secrets and populate DBBAOS with credentials for DBNAME.

      Uses the hvac client to fetch key-value secrets from the configured BAOURL.
      Parses keys matching 'pass' patterns to extract database usernames and passwords.
      """
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
