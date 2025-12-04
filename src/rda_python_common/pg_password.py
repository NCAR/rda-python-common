#!/usr/bin/env python3
#
##################################################################################
#
#     Title: pg_password
#    Author: Zaihua Ji, zji@ucar.edu
#      Date: 2025-10-27
#   Purpose: python script to retrieve passwords for postgrsql login to connect a
#            gdex database from inside an python application
#
#    Github: https://github.com/NCAR/rda-python-common.git
#
##################################################################################

import os
import sys
import re
import pwd
import hvac
from . import PgLOG
from . import PgDBI

DBFLDS = {
   'd' : 'dbname',
   'c' : 'scname',
   'h' : 'dbhost',
   'p' : 'dbport',
   'u' : 'lnname'
}

DBINFO = {
   'dbname' : "",
   'scname' : "",
   'lnname' : "",
   'dbhost' : "",
   'dbport' : 5432
}

#
# main function to excecute this script
#
def main():

   permit = False
   argv = sys.argv[1:]
   opt = None
   dohelp = True
   dbopt = False

   for arg in argv:
      if re.match(r'^-\w+$', arg):
         opt = arg[1:]
      elif opt:
         if opt == 'l':
            PgDBI.PGDBI['BAOURL'] = arg
         elif opt == 'k':
            PgDBI.PGDBI['BAOTOKEN'] = arg
         elif opt in DBFLDS:
            dbopt = True
            DBINFO[DBFLDS[opt]] = arg
         else:
            PgLOG.pglog(arg + ": Unknown option", PgLOG.LGEREX)
         dohelp = False
      else:
         PgLOG.pglog(arg + ": Value provided without option", PgLOG.LGEREX)

   if dohelp:
      print("Usage: pg_password [-l OpenBaoURL] [-k TokenName] [-d DBNAME]  \\")
      print("                   [-c SCHEMA] [-u USName] [-h DBHOST] [-p DBPORT]")
      print("  -l OpenBao URL to retrieve passwords")
      print("  -k OpenBao Token Name to retrieve passwords")
      print("  -d PostgreSQL Database Name")
      print("  -c PostgreSQL Schema Name")
      print("  -u PostgreSQL Login User Name")
      print("  -h PostgreSQL Server Host Name")
      print("  -p PostgreSQL Port Number")
      sys.exit(0)

   if dbopt:
      PgDBI.default_scinfo(DBINFO['dbname'], DBINFO['scname'], DBINFO['dbhost'],
                           DBINFO['lnname'], None, DBINFO['dbport'])   

   pwname = PgDBI.get_baopassword()
   if not pwname: pwname = PgDBI.get_pgpassword()
   print(pwname)
   sys.exit(0)

#
# call main() to start program
#
if __name__ == "__main__": main()
