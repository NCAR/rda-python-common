###############################################################################
#     Title: pg_split.py  -- PostgreSQL DataBase Interface foe table wfile
#    Author: Zaihua Ji,  zji@ucar.edu
#      Date: 09/010/2024
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-12-01 convert to class PgSplit
#   Purpose: Python library module to handle query and manipulate table wfile
#    Github: https://github.com/NCAR/rda-python-common.git
###############################################################################
import os
import re
from os import path as op
from .pg_util import PgUtil

class PgSplit(PgUtil):

   def __init__(self):
      super().__init__()  # initialize parent class

   # compare wfile records between tables wfile and wfile_dNNNNNN,
   # and return the records need to be added, modified and deleted 
   def compare_wfile(self, wfrecs, dsrecs):
      flds = dsrecs.keys()
      flen = len(flds)
      arecs = dict(zip(flds, [[]]*flen))
      mrecs = {}
      drecs = []
      wfcnt = len(wfrecs['wid'])
      dscnt = len(dsrecs['wid'])
      pi = pj = -1
      i = j = 0
      while i < wfcnt and j < dscnt:
         if i > pi:
            wfrec = self.onerecord(wfrecs, i)
            wwid = wfrec['wid']
            pi = i
         if j > pj:
            dsrec = self.onerecord(dsrecs, j)
            dwid = dsrec['wid']
            pj = j
         if wwid == dwid:
            mrec = self.compare_one_record(flds, wfrec, dsrec)
            if mrec: mrecs[wwid] = mrec
            i += 1
            j += 1
         elif wwid > dwid:
            drecs.append(dwid)
            j += 1
         else:
            for fld in flds:
               arecs[fld].append(wfrec[fld])
            i += 1
      if i < wfcnt:
         for fld in flds:
            arecs[fld].extend(wfrecs[fld][i:wfcnt])
      elif j < dscnt:
         drecs.extend(dsrecs['wid'][j:dscnt])      
      if len(arecs['wid']) == 0: arecs = {}
      return (arecs, mrecs, drecs)
   
   # Compare column values and return the new one; empty if the same
   @staticmethod
   def compare_one_record(flds, wfrec, dsrec):
      mrec = {}
      for fld in flds:
         if wfrec[fld] != dsrec[fld]: mrec[fld] = wfrec[fld]
      return mrec

   # convert wfile records to wfile_dsid records  
   @staticmethod
   def wfile2wdsid(wfrecs, wids = None):
      dsrecs = {}
      if wfrecs:
         for fld in wfrecs:
            if fld == 'dsid': continue
            dsrecs[fld] = wfrecs[fld]
         if wids: dsrecs['wid'] = wids
      return dsrecs

   # trim wfile records 
   @staticmethod
   def trim_wfile_fields(wfrecs):
      records = {}
      if 'wfile' in wfrecs: records['wfile'] = wfrecs['wfile']
      if 'dsid' in wfrecs: records['dsid'] = wfrecs['dsid']
      return records

   # check the condition string, and add dsid if needed
   @staticmethod
   def get_dsid_condition(dsid, condition):
      if condition:
         if re.search(r'(^|.| )(wid|dsid)\s*=', condition):
            return condition
         else:
            dscnd = "wfile.dsid = '{}' ".format(dsid)
            if not re.match(r'^\s*(ORDER|GROUP|HAVING|OFFSET|LIMIT)\s', condition, re.I): dscnd += 'AND '
            return dscnd + condition      # no where clause, append directly
      else:
         return "wfile.dsid = '{}'".format(dsid)

   # insert one record into wfile and/or wfile_dsid
   def pgadd_wfile(self, dsid, wfrec, logact = None, getid = None):
      if logact is None: logact = self.LOGERR
      record = {'wfile': wfrec['wfile'],
                'dsid': (wfrec['dsid'] if 'dsid' in wfrec else dsid)}
      wret = self.pgadd('wfile', record, logact, 'wid')
      if wret:
         record = self.wfile2wdsid(wfrec, wret)
         self.pgadd('wfile_' + dsid, record, logact|self.ADDTBL)
      if logact&self.AUTOID or getid:
         return wret
      else:
         return 1 if wret else 0

   # insert multiple records into wfile and/or wfile_dsid
   def pgmadd_wfile(self, dsid, wfrecs, logact = None, getid = None):
      if logact is None: logact = self.LOGERR
      records = {'wfile': wfrecs['wfile'],
                 'dsid': (wfrecs['dsid'] if 'dsid' in wfrecs else [dsid]*len(wfrecs['wfile']))}
      wret = self.pgmadd('wfile', records, logact, 'wid')
      wcnt = wret if isinstance(wret, int) else len(wret)
      if wcnt:
         records = self.wfile2wdsid(wfrecs, wret)
         self.pgmadd('wfile_' + dsid, records, logact|self.ADDTBL)
      if logact&self.AUTOID or getid:
         return wret
      else:
         return wcnt

   # update one or multiple rows in wfile and/or wfile_dsid
   # exclude dsid in condition
   def pgupdt_wfile(self, dsid, wfrec, condition, logact = None):
      if logact is None: logact = self.LOGERR
      record = self.trim_wfile_fields(wfrec)
      if record:
         wret = self.pgupdt('wfile', record, self.get_dsid_condition(dsid, condition), logact)
      else:
         wret = 1
      if wret:
         record = self.wfile2wdsid(wfrec)
         if record: wret = self.pgupdt("wfile_" + dsid, record, condition, logact|self.ADDTBL)
      return wret

   # update one row in wfile and/or wfile_dsid with dsid change
   # exclude dsid in condition
   def pgupdt_wfile_dsid(self, dsid, odsid, wfrec, wid, logact = None):
      if logact is None: logact = self.LOGERR
      record = self.trim_wfile_fields(wfrec)
      cnd = 'wid = {}'.format(wid)
      if record:
         wret = self.pgupdt('wfile', record, cnd, logact)
      else:
         wret = 1
      if wret:
         record = self.wfile2wdsid(wfrec)
         tname = 'wfile_' + dsid
         doupdt = True
         if odsid and odsid != dsid:
            oname = 'wfile_' + odsid
            pgrec = self.pgget(oname, '*', cnd, logact|self.ADDTBL)
            if pgrec:
               for fld in record:
                  pgrec[fld] = record[fld]
               wret = self.pgadd(tname, pgrec, logact|self.ADDTBL)
               if wret: self.pgdel(oname, cnd, logact)
               doupdt = False
         if doupdt and record:
            wret = self.pgupdt(tname, record, cnd, logact|self.ADDTBL)
      return wret

   # delete one or multiple rows in wfile and/or wfile_dsid, and add the record(s) into wfile_delete
   # exclude dsid in conidtion
   def pgdel_wfile(self, dsid, condition, logact = None):
      if logact is None: logact = self.LOGERR
      pgrecs = self.pgmget_wfile(dsid, '*', condition, logact|self.ADDTBL)
      wret = self.pgdel('wfile', self.get_dsid_condition(dsid, condition), logact)   
      if wret: self.pgdel("wfile_" + dsid, condition, logact)
      if wret and pgrecs: self.pgmadd('wfile_delete', pgrecs, logact)
      return wret

   # delete one or multiple rows in sfile, and add the record(s) into sfile_delete
   def pgdel_sfile(self, condition, logact = None):
      if logact is None: logact = self.LOGERR
      pgrecs = self.pgmget('sfile', '*', condition, logact)
      sret = self.pgdel('sfile', condition, logact)   
      if sret and pgrecs: self.pgmadd('sfile_delete', pgrecs, logact)
      return sret

   # update one or multiple rows in wfile and/or wfile_dsid for multiple dsid
   # exclude dsid in condition
   def pgupdt_wfile_dsids(self, dsid, dsids, brec, bcnd, logact = None):
      if logact is None: logact = self.LOGERR
      record = self.trim_wfile_fields(brec)
      if record:
         wret = self.pgupdt("wfile", record, bcnd, logact)
      else:
         wret = 1
      if wret:
         record = self.wfile2wdsid(brec)
         if record:
            wret = 0
            dids = [dsid]
            if dsids: dids.extend(dsids.split(','))
            for did in dids:
               wret += self.pgupdt("wfile_" + did, record, bcnd, logact|self.ADDTBL)
      return wret

   # get one record from wfile or wfile_dsid
   # exclude dsid in fields and condition
   def pgget_wfile(self, dsid, fields, condition, logact = None):
      if logact is None: logact = self.LOGERR
      tname = "wfile_" + dsid
      flds = fields.replace('wfile.', tname + '.')
      cnd = condition.replace('wfile.', tname + '.')
      record = self.pgget(tname, flds, cnd, logact|self.ADDTBL)
      if record and flds == '*': record['dsid'] = dsid
      return record

   # get one record from wfile or wfile_dsid joing other tables
   # exclude dsid in fields and condition
   def pgget_wfile_join(self, dsid, tjoin, fields, condition, logact = None):
      if logact is None: logact = self.LOGERR
      tname = "wfile_" + dsid
      flds = fields.replace('wfile.', tname + '.')
      jname = tname + ' ' + tjoin.replace('wfile.', tname + '.')
      cnd = condition.replace('wfile.', tname + '.')
      record = self.pgget(jname, flds, cnd, logact|self.ADDTBL)
      if record and flds == '*': record['dsid'] = dsid
      return record

   # get multiple records from wfile or wfile_dsid
   # exclude dsid in fields and condition
   def pgmget_wfile(self, dsid, fields, condition, logact = None):
      if logact is None: logact = self.LOGERR
      tname = "wfile_" + dsid
      flds = fields.replace('wfile.', tname + '.')
      cnd = condition.replace('wfile.', tname + '.')
      records = self.pgmget(tname, flds, cnd, logact|self.ADDTBL)
      if records and flds == '*': records['dsid'] = [dsid]*len(records['wid'])
      return records

   # get multiple records from wfile or wfile_dsid joining other tables
   # exclude dsid in fields and condition
   def pgmget_wfile_join(self, dsid, tjoin, fields, condition, logact = None):
      if logact is None: logact = self.LOGERR
      tname = "wfile_" + dsid
      flds = fields.replace('wfile.', tname + '.')
      jname = tname + ' ' + tjoin.replace('wfile.', tname + '.')
      cnd = condition.replace('wfile.', tname + '.')
      records = self.pgmget(jname, flds, cnd, logact|self.ADDTBL)
      if records and flds == '*': records['dsid'] = [dsid]*len(records['wid'])
      return records
