#!/usr/bin/env python3
##################################################################################
#     Title: pgpassword
#    Author: Zaihua Ji, zji@ucar.edu
#      Date: 2025-10-27
#            2025-12-02 convert to class PgPassword
#   Purpose: python script to retrieve passwords for postgresql login to connect a
#            gdex database from inside an python application
#    Github: https://github.com/NCAR/rda-python-common.git
##################################################################################
"""
pgpassword.py - Command-line helper that retrieves a PostgreSQL password.

Provides the PgPassword class and a ``main`` entry point used by the
``pgpassword`` console script. The password is looked up first from
OpenBao (using the URL/token configured in PgDBI) and, if not found,
from the user's ``.pgpass`` file. The result is printed to stdout so
that shell wrappers and other RDA utilities can capture it.
"""
import sys
import re
from .pg_dbi import PgDBI

class PgPassword(PgDBI):
   """
   Command-line helper for retrieving a PostgreSQL login password.

   Inherits from PgDBI to reuse its database-connection metadata
   (PGDBI), default schema handling, and password-lookup methods
   (``get_baopassword`` / ``get_pgpassword``).

   Instance attributes set in __init__:
      DBFLDS    -- mapping of CLI option letters to PGDBI field names
      DBINFO    -- per-invocation overrides for dbname/scname/lnname/
                   dbhost/dbport supplied via CLI options
      dbopt     -- True once at least one DB-override option is seen,
                   triggering ``default_scinfo`` before lookup
      password  -- the retrieved password (set by ``start_actions``)
   """

   def __init__(self):
      """Initialize PgPassword with empty DB-override info and option maps."""
      super().__init__()  # initialize parent class
      self.DBFLDS = {
         'd': 'dbname',
         'c': 'scname',
         'h': 'dbhost',
         'p': 'dbport',
         'u': 'lnname'
      }
      self.DBINFO = {
         'dbname': "",
         'scname': "",
         'lnname': "",
         'dbhost': "",
         'dbport': 5432
      }
      self.dbopt = False
      self.password = ''

   # read in command line parameters
   def read_parameters(self):
      """
      Parse ``sys.argv`` and apply CLI overrides.

      Recognized options:
         -? / --help -- show usage and exit
         -l URL      -- OpenBao URL (stored in self.PGDBI['BAOURL'])
         -k TOKEN    -- OpenBao token name (stored in self.PGDBI['BAOTOKEN'])
         -d NAME     -- PostgreSQL database name
         -c NAME     -- PostgreSQL schema name
         -u NAME     -- PostgreSQL login user name
         -h HOST     -- PostgreSQL server host name
         -p PORT     -- PostgreSQL port number

      With no arguments, all defaults inherited from PgDBI/PgLOG are
      used and the password lookup proceeds. Unknown options, stray
      values, or an option without its required value cause an
      immediate error exit via ``self.pglog(..., LGEREX)``.
      """
      argv = sys.argv[1:]
      opt = None
      for arg in argv:
         if arg in ('-?', '-help', '--help'):
            self.set_help_path(__file__)
            self.show_usage("pgpassword")
         elif re.match(r'^-[a-zA-Z]$', arg):
            if opt:
               self.pglog("-" + opt + ": missing option value", self.LGEREX)
            opt = arg[1:]
         elif opt:
            if opt == 'l':
               self.PGDBI['BAOURL'] = arg
            elif opt == 'k':
               self.PGDBI['BAOTOKEN'] = arg
            elif opt in self.DBFLDS:
               self.dbopt = True
               self.DBINFO[self.DBFLDS[opt]] = arg
            else:
               self.pglog("-" + opt + ": Unknown option", self.LGEREX)
            opt = None
         else:
            self.pglog(arg + ": value provided without option", self.LGEREX)
      if opt:
         self.pglog("-" + opt + ": missing option value", self.LGEREX)

   # get the pgpassword
   def start_actions(self):
      """
      Look up the password and store it in ``self.password``.

      Applies any CLI-supplied DB overrides via ``default_scinfo``, then
      tries OpenBao first (``get_baopassword``) and falls back to the
      ``.pgpass`` file (``get_pgpassword``) if OpenBao returns nothing.
      """
      if self.dbopt:
         self.default_scinfo(self.DBINFO['dbname'], self.DBINFO['scname'], self.DBINFO['dbhost'],
                             self.DBINFO['lnname'], None, self.DBINFO['dbport'])   
      self.password = self.get_baopassword()
      if not self.password: self.password = self.get_pgpassword()

# main function to excecute this script
def main():
   """Entry point for the ``pgpassword`` console script: print the retrieved password to stdout."""
   object = PgPassword()
   object.read_parameters()
   object.start_actions()   
   print(object.password)
   sys.exit(0)

# call main() to start program
if __name__ == "__main__": main()
