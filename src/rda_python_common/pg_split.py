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
   """Manages synchronisation of wfile records between shared and per-dataset tables.

   Handles compare, add, update, and delete operations between the shared
   ``wfile`` table and per-dataset ``wfile_<dsid>`` partition tables.
   """

   def __init__(self):
      """Initialise PgSplit by delegating to the PgUtil parent."""
      super().__init__()  # initialize parent class

   def compare_wfile(self, wfrecs, dsrecs):
      """Compare wfile records between wfile and wfile_<dsid>, returning diffs.

      Args:
         wfrecs: Multi-record dict from the wfile table (keys are field names,
            values are lists of values).
         dsrecs: Multi-record dict from the wfile_<dsid> table.

      Returns:
         Tuple (arecs, mrecs, drecs) where arecs is a multi-record dict of
         records to add, mrecs is a dict keyed by wid of records to modify,
         and drecs is a list of wids to delete.
      """
      flds = list(dsrecs.keys())
      flen = len(flds)
      arecs = {fld: [] for fld in flds}
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

   @staticmethod
   def compare_one_record(flds, wfrec, dsrec):
      """Compare column values between two single-row dicts and return differences.

      Args:
         flds: Iterable of field names to compare.
         wfrec: Single-record dict from the wfile table.
         dsrec: Single-record dict from the wfile_<dsid> table.

      Returns:
         Dict mapping field names to the wfrec value for every field that
         differs between the two records.  Empty dict if all fields match.
      """
      mrec = {}
      for fld in flds:
         if wfrec[fld] != dsrec[fld]: mrec[fld] = wfrec[fld]
      return mrec

   @staticmethod
   def wfile2wdsid(wfrecs, wids=None):
      """Convert a wfile multi-record dict to a wfile_<dsid> multi-record dict.

      Strips the ``dsid`` field and optionally replaces the ``wid`` list.

      Args:
         wfrecs: Multi-record dict from the wfile table.
         wids: Optional list of wid values to use in the returned dict.

      Returns:
         Multi-record dict suitable for insertion into wfile_<dsid>, or an
         empty dict if wfrecs is falsy.
      """
      dsrecs = {}
      if wfrecs:
         for fld in wfrecs:
            if fld == 'dsid': continue
            dsrecs[fld] = wfrecs[fld]
         if wids: dsrecs['wid'] = wids
      return dsrecs

   @staticmethod
   def trim_wfile_fields(wfrecs):
      """Extract only the wfile-table fields (wfile and dsid) from a record dict.

      Args:
         wfrecs: Record dict potentially containing many fields.

      Returns:
         Dict containing only the ``wfile`` and ``dsid`` keys that are present
         in wfrecs.
      """
      records = {}
      if 'wfile' in wfrecs: records['wfile'] = wfrecs['wfile']
      if 'dsid' in wfrecs: records['dsid'] = wfrecs['dsid']
      return records

   @staticmethod
   def get_dsid_condition(dsid, condition):
      """Build a WHERE-clause fragment that scopes a query to a specific dsid.

      If condition already references ``wid`` or ``dsid``, it is returned
      unchanged.  Otherwise a ``wfile.dsid = '<dsid>'`` prefix is prepended.

      Args:
         dsid: Dataset identifier string used to filter rows.
         condition: Existing SQL condition string, or empty/None.

      Returns:
         SQL condition string that includes a dsid equality predicate.
      """
      if condition:
         if re.search(r'(^|.| )(wid|dsid)\s*=', condition):
            return condition
         else:
            dscnd = "wfile.dsid = '{}' ".format(dsid)
            if not re.match(r'^\s*(ORDER|GROUP|HAVING|OFFSET|LIMIT)\s', condition, re.I): dscnd += 'AND '
            return dscnd + condition      # no where clause, append directly
      else:
         return "wfile.dsid = '{}'".format(dsid)

   def pgadd_wfile(self, dsid, wfrec, logact=None, getid=None):
      """Insert one record into wfile and the corresponding wfile_<dsid> table.

      Args:
         dsid: Dataset identifier string.
         wfrec: Single-record dict to insert.
         logact: Logging action flags; defaults to self.LOGERR.
         getid: If truthy, return the generated wid instead of a success flag.

      Returns:
         The new wid (int or list) when logact includes AUTOID or getid is
         truthy; otherwise 1 on success or 0 on failure.
      """
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

   def pgmadd_wfile(self, dsid, wfrecs, logact=None, getid=None):
      """Insert multiple records into wfile and the corresponding wfile_<dsid> table.

      Args:
         dsid: Dataset identifier string.
         wfrecs: Multi-record dict to insert.
         logact: Logging action flags; defaults to self.LOGERR.
         getid: If truthy, return the list of generated wids instead of a count.

      Returns:
         List of new wids when logact includes AUTOID or getid is truthy;
         otherwise the count of rows inserted.
      """
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

   def pgupdt_wfile(self, dsid, wfrec, condition, logact=None):
      """Update one or more rows in wfile and wfile_<dsid>.

      Args:
         dsid: Dataset identifier string.
         wfrec: Record dict containing the fields and values to update.
         condition: SQL WHERE-clause fragment (must not include a dsid filter).
         logact: Logging action flags; defaults to self.LOGERR.

      Returns:
         Number of rows updated, or 0 on failure.
      """
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

   def pgupdt_wfile_dsid(self, dsid, odsid, wfrec, wid, logact=None):
      """Update one row in wfile and wfile_<dsid>, handling a dsid change.

      When the dataset id changes (odsid != dsid), the corresponding row in
      wfile_<odsid> is copied to wfile_<dsid> with the new field values
      applied, then deleted from wfile_<odsid>.

      Args:
         dsid: New dataset identifier string.
         odsid: Old dataset identifier string; may be None or equal to dsid.
         wfrec: Record dict containing the fields and values to update.
         wid: Primary key (wid) of the row to update.
         logact: Logging action flags; defaults to self.LOGERR.

      Returns:
         Number of rows affected, or 0 on failure.
      """
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

   def pgdel_wfile(self, dsid, condition, logact=None):
      """Delete rows from wfile and wfile_<dsid> and archive them in wfile_delete.

      Args:
         dsid: Dataset identifier string.
         condition: SQL WHERE-clause fragment (must not include a dsid filter).
         logact: Logging action flags; defaults to self.LOGERR.

      Returns:
         Number of rows deleted from wfile, or 0 on failure.
      """
      if logact is None: logact = self.LOGERR
      pgrecs = self.pgmget_wfile(dsid, '*', condition, logact|self.ADDTBL)
      wret = self.pgdel('wfile', self.get_dsid_condition(dsid, condition), logact)
      if wret: self.pgdel("wfile_" + dsid, condition, logact)
      if wret and pgrecs: self.pgmadd('wfile_delete', pgrecs, logact)
      return wret

   def pgdel_sfile(self, condition, logact=None):
      """Delete rows from sfile and archive them in sfile_delete.

      Args:
         condition: SQL WHERE-clause fragment identifying rows to delete.
         logact: Logging action flags; defaults to self.LOGERR.

      Returns:
         Number of rows deleted, or 0 on failure.
      """
      if logact is None: logact = self.LOGERR
      pgrecs = self.pgmget('sfile', '*', condition, logact)
      sret = self.pgdel('sfile', condition, logact)
      if sret and pgrecs: self.pgmadd('sfile_delete', pgrecs, logact)
      return sret

   def pgupdt_wfile_dsids(self, dsid, dsids, brec, bcnd, logact=None):
      """Update rows in wfile and in multiple wfile_<dsid> partition tables.

      Args:
         dsid: Primary dataset identifier string.
         dsids: Comma-separated string of additional dataset identifiers.
         brec: Record dict containing the fields and values to update.
         bcnd: SQL WHERE-clause fragment applied to all tables.
         logact: Logging action flags; defaults to self.LOGERR.

      Returns:
         Total number of rows updated across all partition tables, or the
         result of the wfile update when no partition fields are present.
      """
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

   def pgget_wfile(self, dsid, fields, condition, logact=None):
      """Retrieve one record from the wfile_<dsid> partition table.

      Args:
         dsid: Dataset identifier string.
         fields: Comma-separated field list or ``'*'``.  References to
            ``wfile.`` are rewritten to ``wfile_<dsid>.``.
         condition: SQL WHERE-clause fragment.  References to ``wfile.`` are
            rewritten to ``wfile_<dsid>.``.
         logact: Logging action flags; defaults to self.LOGERR.

      Returns:
         Single-record dict, or None if no matching row is found.
      """
      if logact is None: logact = self.LOGERR
      tname = "wfile_" + dsid
      flds = fields.replace('wfile.', tname + '.')
      cnd = condition.replace('wfile.', tname + '.')
      record = self.pgget(tname, flds, cnd, logact|self.ADDTBL)
      if record and flds == '*': record['dsid'] = dsid
      return record

   def pgget_wfile_join(self, dsid, tjoin, fields, condition, logact=None):
      """Retrieve one record from wfile_<dsid> joined with another table.

      Args:
         dsid: Dataset identifier string.
         tjoin: SQL JOIN clause fragment; references to ``wfile.`` are
            rewritten to ``wfile_<dsid>.``.
         fields: Comma-separated field list or ``'*'``.
         condition: SQL WHERE-clause fragment.
         logact: Logging action flags; defaults to self.LOGERR.

      Returns:
         Single-record dict, or None if no matching row is found.
      """
      if logact is None: logact = self.LOGERR
      tname = "wfile_" + dsid
      flds = fields.replace('wfile.', tname + '.')
      jname = tname + ' ' + tjoin.replace('wfile.', tname + '.')
      cnd = condition.replace('wfile.', tname + '.')
      record = self.pgget(jname, flds, cnd, logact|self.ADDTBL)
      if record and flds == '*': record['dsid'] = dsid
      return record

   def pgmget_wfile(self, dsid, fields, condition, logact=None):
      """Retrieve multiple records from the wfile_<dsid> partition table.

      Args:
         dsid: Dataset identifier string.
         fields: Comma-separated field list or ``'*'``.  References to
            ``wfile.`` are rewritten to ``wfile_<dsid>.``.
         condition: SQL WHERE-clause fragment.
         logact: Logging action flags; defaults to self.LOGERR.

      Returns:
         Multi-record dict, or None if no matching rows are found.
      """
      if logact is None: logact = self.LOGERR
      tname = "wfile_" + dsid
      flds = fields.replace('wfile.', tname + '.')
      cnd = condition.replace('wfile.', tname + '.')
      records = self.pgmget(tname, flds, cnd, logact|self.ADDTBL)
      if records and flds == '*': records['dsid'] = [dsid]*len(records['wid'])
      return records

   def pgmget_wfile_join(self, dsid, tjoin, fields, condition, logact=None):
      """Retrieve multiple records from wfile_<dsid> joined with another table.

      Args:
         dsid: Dataset identifier string.
         tjoin: SQL JOIN clause fragment; references to ``wfile.`` are
            rewritten to ``wfile_<dsid>.``.
         fields: Comma-separated field list or ``'*'``.
         condition: SQL WHERE-clause fragment.
         logact: Logging action flags; defaults to self.LOGERR.

      Returns:
         Multi-record dict, or None if no matching rows are found.
      """
      if logact is None: logact = self.LOGERR
      tname = "wfile_" + dsid
      flds = fields.replace('wfile.', tname + '.')
      jname = tname + ' ' + tjoin.replace('wfile.', tname + '.')
      cnd = condition.replace('wfile.', tname + '.')
      records = self.pgmget(jname, flds, cnd, logact|self.ADDTBL)
      if records and flds == '*': records['dsid'] = [dsid]*len(records['wid'])
      return records
