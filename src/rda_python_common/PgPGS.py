#
###############################################################################
#
#     Title : PgPGS.py  -- PostgreSQL Interface for CDP DataBase Per psql
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 08/31/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module to handle sql scripts to retrieve info
#             from cdp database per psql
#
#    Github : https://github.com/NCAR/rda-python-common.git
#
###############################################################################
#
import os
import re
from . import PgLOG

PGPGS = {}
PGPGS["PGSSERV"] = PgLOG.get_environment("PGSSERV", '-h vetsdbprod -p 5432 -U acadmin access_control');
PGPGS["SQLPATH"] = PgLOG.get_environment("SQLPATH", PgLOG.PGLOG['DSSHOME']+ "/dssdb/sql");

#
# local function: create sql file
#
def pgs_sql_file(tablenames, fields, condition = None):

   sqlfile = "{}/pgs{}.sql".format(PGPGS['SQLPATH'], os.getpid())

   sqlstr = "SELECT {}\nFROM {}".format(fields, tablenames)
   if condition:
      if re.match(r'^\s*(ORDER|GROUP|HAVING)\s', condition, re.I):
         sqlstr += "\n{}".format(condition)
      else:
         sqlstr += "\nWHERE {}".format(condition)
   sqlstr += ";\n"
   try:
      SQL = open(sqlfile, 'w')
      SQL.write(sqlstr)
      SQL.close()
   except Exception as e:
      PgLOG.pglog("Error Open '{}': {}".format(sqlfile, str(e)), PgLOG.LGWNEX)

   if PgLOG.PGLOG['DBGLEVEL']: PgLOG.pgdbg(1000, sqlstr)

   return sqlfile

#
# tablenames: comma deliminated string of table names
#     fields: fieldnames for query pgscle database,
#  condition: querry conditions for where clause)
#   Return: one record from tablename, a hash reference with keys as field names
#           and values as field values upon success, FAILURE otherwise 
#
def pgsget(tablenames, fields, condition = None, logact = 0):

   sqlfile = pgs_sql_file(tablenames, fields, condition)
   sqlout = PgLOG.pgsystem("psql {} < {}".format(PGPGS['PGSSERV'], sqlfile), logact, 273+1024)   # 1+16+256

   colcnt = 0
   record = {}
   if sqlout:
      for line in re.split(r'\n', sqlout):
         vals = re.split(r'\s*\|\s+', line)
         if colcnt:    # gather data
            record = dict(zip(fields, vals))
            break
         else:    # gather field names
            flds = vals
            colcnt = len(flds)
   elif PgLOG.PGLOG['SYSERR']:    # error happens
      PgLOG.pglog(PgLOG.PGLOG['SYSERR'], logact|PgLOG.ERRLOG)

   if PgLOG.PGLOG['DBGLEVEL']:
      if record:
         PgLOG.pgdbg(1000, "pgsget: 1 record retrieved from {}:\n{}".format(tablenames, str(record)))
      else:
         PgLOG.pgdbg(1000, "pgsget: 0 record retrieved from " + tablenames)

   os.remove(sqlfile)

   return record

#
# tablenames: comma deliminated string of tables
#     fields: fieldnames for query pgscle database,
#  condition: querry conditions for where clause)
# Return: mutiple records from tablenames, a dict with field names as keys and lists
#         of retrieved values. All arrays are same size. FAILURE if not success
#
def pgsmget(tablenames, fields, condition = None, logact = 0):

   sqlfile = pgs_sql_file(tablenames, fields, condition)
   sqlout = PgLOG.pgsystem("psql {} < {}".format(PGPGS['PGSSERV'], sqlfile), logact, 273+1024)   # 1+16+256

   rowcnt = colcnt = 0
   records = {}
   vals = []
   if sqlout:
      for line in re.split(r'\n', sqlout):
         row = re.split(r'\s*\|\s+', line)
         if colcnt:    # gather data
            vals.append(row)
            rowcnt += 1
         else:    # gather field names
            flds = row
            colcnt = len(flds)
      if rowcnt > 0:
         records = dict(zip(flds, list(zip(*vals))))
   elif PgLOG.PGLOG['SYSERR']:    # error happens
      PgLOG.pglog(PgLOG.PGLOG['SYSERR'], logact|PgLOG.ERRLOG)

   if PgLOG.PGLOG['DBGLEVEL']:
      PgLOG.pgdbg(1000, "pgsmget: {} record(s) retrieved from {}".format(rowcnt, tablenames))
   
   os.remove(sqlfile)    # remove sqlfile when successful

   return records

#
#    email: cdp user email address,
#   userid: cdp user ID,
# username: cdp user name
#   Return: one record from CDP PostGreSQL database; PGLOG.FAILURE otherwise
#
def get_cdp_user(email, userid = 0, username = None, logact = 0):
 
   if userid:
       condition = "id = {}".format(userid)
   elif email:
       condition = "email = '{}'".format(email)
   elif username:
      condition = "username = '{}'".format(username)
   else:
      return PgLOG.FAILURE

   fields = ("id as cdpid, firstname as fstname, middlename as midinit, " +
            "lastname as lstname, email, username as cdpname, " +
            "organization as org_name, organization_type as org_type, country")
   return  pgsget('users', fields, condition, logact)

#
#   name: field name
#  value: field value
# Return: converted value from upcases to lower case
#
def convert_pgs_case(name, value):

   if name == "username" or name == "email":
      return value.lower()
   else:
      return value    # no change
