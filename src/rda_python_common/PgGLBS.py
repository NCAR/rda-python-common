#
###############################################################################
#
#     Title : PgGLBS.py
#    Author : Thomas Cram, tcram@ucar.edu
#      Date : 12/10/2014
#             10/10/2020, Zaihua Ji, zji@ucar.edu:
#             converted from perl package to python module
#             2025-01-10, Zaihua Ji, zji@ucar.edu:
#             transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module for Globus functions and utilities
#
#    Github : https://github.com/NCAR/rda-python-common.git
# 
###############################################################################
#
import os
import re
from . import PgLOG
from . import PgUtil
from MyGlobus import MyGlobus, MyEndpoints, GLOBUS_REQUEST_DOMAIN
from . import PgDBI

try:
   from urllib.parse import urlencode
except:
   from urllib import urlencode

BFILES = {}  # cache backup file names and dates for each bid

#
# Remove the Globus share rule ID for a dsrqst share 
#
def remove_globus_rid(ridx, dsid):

   if not ridx: return PgLOG.pglog("[remove_globus_rid] Request index is not defined", PgLOG.LOGWRN)
   if not dsid: return PgLOG.pglog("[remove_globus_rid] Dataset ID is not defined", PgLOG.LOGWRN)

   cmd = "dsglobus"
   args = "'-rp -ri {}'".format(ridx)
   action = "RP"
   host = "PBS"
   workdir = "/glade/u/home/tcram"
   opts = "'-l walltime=15:00'"
   spec = "tcram"
   check_cmd = "dscheck ac -cm {} -av {} -ds {} -an {} -hn {} -wd {} -sn {} -qs {} -md".format(cmd, args, dsid, action, host, workdir, spec, opts)

   PgLOG.pgsystem(check_cmd)

#
# Submit a Globus transfer of the request output on behalf of the user
#
def submit_globus_transfer(ridx):

   # call dsglobus to submit transfer
   cmd = "dsglobus -st -ri {}".format(ridx)
   return PgLOG.pgsystem(cmd, PgLOG.LOGWRN, 16)

#
# check a RDA file is backed up or not for given file record;
# clear the cached bfile records if frec is None.
# return 0 if not yet, 1 if backed up, or -1 if backed up but modified
#
def file_backup_status(frec, chgdays = 1, logact = 0):

   if frec is None:
      BFILES.clear()
      return 0

   bid = frec['bid']
   if not bid: return 0

   fields = 'bfile, dsid, date_modified'
   if chgdays > 0: fields += ', note'
   if bid not in BFILES: BFILES[bid] = PgDBI.pgget('bfile', fields, 'bid = {}'.format(bid), logact)
   brec = BFILES[bid]
   if not brec: return 0

   if 'sfile' in frec:
      fname = frec['sfile']
      ftype = 'Saved'
   else:
      fname = frec['wfile']
      ftype = 'Web'
   ret = 1
   fdate = frec['date_modified']
   bdate = brec['date_modified']
   if chgdays > 0 and PgUtil.diffdate(fdate, bdate) >= chgdays:
      ret = -1
      if brec['note']:
         mp = r'{}<:>{}<:>(\d+)<:>(\w+)<:>'.format(fname, frec['type']) 
         ms = re.search(mp, brec['note'])
         if ms:
            fsize = int(ms.group(1))
            cksum = ms.group(2)
            if cksum and cksum == frec['checksum'] or not cksum and fsize == frec['data_size']:
               ret = 1

   if logact:
      if ret == 1:
         msg = "{}-{}: {} file backed up to /{}/{} by {}".format(frec['dsid'], fname, ftype, brec['dsid'], brec['bfile'], bdate)
      else:
         msg = "{}-{}: {} file changed on {}".format(frec['dsid'], fname, ftype, fdate)
      PgLOG.pglog(msg, logact)

   return ret

#=========================================================================================
def get_request_file_url(rfile, rpath=None, logact=0):
   """ Returns the URL for a request file 
       Input arguments:
          rfile = request file
	  dsid = dataset ID (dsnnn.n)
	  rpath = path to request file, relatvie to RDA data base path (e.g. '/dsrqst/<rqstid>/'
   """
   domain = GLOBUS_REQUEST_DOMAIN

   if not rpath:
      try:
         cond = "wfile='{}'".format(rfile)
         wfrqst = PgDBI.pgget('wfrqst', 'rindex', cond, logact)
      except:
         msg = "[get_request_file_url] Problem getting rindex for request file {}".format(rfile)
         PgLOG.pglog(msg)
      if not wfrqst:
         raise TypeError("Request file {} not found in table 'wfrqst'".format(rfile))
      rpath = get_request_path(wfrqst['rindex'], logact=0)

   if (rpath.find('/',0,1) != -1):
      rpath = rpath.replace('/','',1)
 
   url = os.path.join(domain, rpath, rfile)
   return url

#=========================================================================================
def get_request_path(rindex, logact=0):
   """ Returns relative path to request file 
       Example: '/dsrqst/<rqstid>/'
   """
   try:
      fields = 'rqstid, location'
      cond = 'rindex={}'.format(rindex)
      rqst_info = PgDBI.pgget('dsrqst', fields, cond, logact)
   except:
      msg = "[get_request_path] Problem getting info for request index {}".format(rindex)
      PgLOG.pglog(msg)
   if not rqst_info:
      raise TypeError("Request index {} not found in RDADB".format(rindex))	

   if rqst_info['location']:
      base_path = MyGlobus['data_request_endpoint_base']
      loc = rqst_info['location']
      loc = loc.rstrip("/")
      if (loc.find(base_path) != -1):
         path_len = len(base_path)
         path = "/{0}/".format(loc[path_len:])
      else:
         path = "/"
   else:
      path = "/dsrqst/{0}/".format(rqst_info['rqstid'])

   return path

#=========================================================================================
def get_guest_collection_url(dsid=None, locflag=None, rindex=None, logact=0):
   """ Returns the URL for the guest collection endpoint in the Globus File Manager.
       Either dataset ID (dsid) or request index (rindex) is required.  If neither
       dsid or rindex are provided, the default URL returned is the top level URL for 
       the 'NCAR RDA Dataset Archive' guest collection.
	
       Optional argument locflag = location flag of dataset ('G' = glade, 'O' = stratus, 
       'B' = both glade and stratus, 'C' = CGD data under /glade/campaign/cgd/cesm)
   """

   if rindex:
      origin_id = MyEndpoints['rda#data_request']
      origin_path = get_request_path(rindex, logact=logact)
   elif dsid:
      if not locflag:
         cond = "dsid='{}'".format(dsid)
         pgloc = PgDBI.pgget('dataset', 'locflag', cond, logact)
         locflag = pgloc['locflag']
      if locflag == 'C':
         origin_id = MyEndpoints['rda-cgd']
         origin_path = "/"
      elif locflag == 'O' or locflag == 'B':
         origin_id = MyEndpoints['rda-stratus']
         origin_path = "/{}/".format(dsid)
      else:
         origin_id = MyEndpoints['rda#datashare']
         origin_path = "/{}/".format(dsid)
   else:
      origin_id = MyEndpoints['rda#datashare']
      origin_path = "/"
	
   params = {'origin_id': origin_id, 'origin_path': origin_path}
   url = '{0}?{1}'.format(MyGlobus['globus_share_url'], urlencode(params))

   return url
