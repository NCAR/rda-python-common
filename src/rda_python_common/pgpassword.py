#!/usr/bin/env python3
##################################################################################
#     Title: pgpassword
#    Author: Zaihua Ji, zji@ucar.edu
#      Date: 2025-10-27
#            2025-12-02 convert to class PgPassword
#   Purpose: python script to retrieve passwords for postgrsql login to connect a
#            gdex database from inside an python application
#    Github: https://github.com/NCAR/rda-python-common.git
##################################################################################
import sys
import re
from .pg_dbi import PgDBI

class PgPassword(PgDBI):

   def __init__(self):
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
      argv = sys.argv[1:]
      opt = None
      dohelp = True
      for arg in argv:
         if re.match(r'^-\w+$', arg):
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
               self.pglog(arg + ": Unknown option", self.LGEREX)
            dohelp = False
         else:
            self.pglog(arg + ": Value provided without option", self.LGEREX)
      if dohelp:
         print("Usage: pgpassword [-l OpenBaoURL] [-k TokenName] [-d DBNAME]  \\")
         print("                  [-c SCHEMA] [-u USName] [-h DBHOST] [-p DBPORT]")
         print("  -l OpenBao URL to retrieve passwords")
         print("  -k OpenBao Token Name to retrieve passwords")
         print("  -d PostgreSQL Database Name")
         print("  -c PostgreSQL Schema Name")
         print("  -u PostgreSQL Login User Name")
         print("  -h PostgreSQL Server Host Name")
         print("  -p PostgreSQL Port Number")
         sys.exit(0)

   # get the pgpassword
   def start_actions(self):
      if self.dbopt:
         self.default_scinfo(self.DBINFO['dbname'], self.DBINFO['scname'], self.DBINFO['dbhost'],
                             self.DBINFO['lnname'], None, self.DBINFO['dbport'])   
      self.password = self.get_baopassword()
      if not self.password: self.password = self.get_pg_pass()

# main function to excecute this script
def main():
   object = PgPassword()
   object.read_parameters()
   object.start_actions()   
   print(object.password)
   sys.exit(0)

# call main() to start program
if __name__ == "__main__": main()
