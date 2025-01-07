#!/usr/bin/env python3

"""
Script to read observation records from the original ISPD HDF5 files and
insert the records into the ISPD database (ISPDDB) at the NCAR RDA.
"""

import logging
import logging.handlers
import os, sys

from rda_ispd_python.ispddb import FillISPD

#=========================================================================================
def main(args):

   add_inventory = args.addinventory
   lead_uid = args.leaduid
   check_existing = args.checkexisting

   fill_ispd = FillISPD(add_inventory=add_inventory, lead_uid=lead_uid, check_existing=check_existing)
   fill_ispd.initialize_db()
   fill_ispd.get_input_files(args.files)
   fill_ispd.initialize_indices()
   fill_ispd.fill_ispd_data()
   fill_ispd.close_db()

#=========================================================================================
def configure_log(**kwargs):
   """ Congigure logging """
   logpath = '/glade/scratch/tcram/logs/ispd/'
   file = os.path.basename(__file__)
   logfile = '{}/{}.log'.format(logpath, os.path.splitext(file)[0])

   if 'loglevel' in kwargs:
      loglevel = kwargs['loglevel']
   else:
      loglevel = 'info'

   level = getattr(logging, loglevel.upper())
   format = '%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s'
   logging.basicConfig(filename=logfile, level=level, format=format)

   return

#=========================================================================================
def parse_opts():
   """ Parse command line arguments """
   import argparse
   import textwrap
	
   desc = "Read ISPD records from pre-processed ASCII data files and store information in ISPDDB."	
   epilog = textwrap.dedent('''\
   Example:
      - Read the ISPD records from ispd_v4_1950-01.txt and store the information in ISPDDB:
         fill_ispddb.py -i -e ispd_v4_1950-01.txt
   ''')

   parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=desc, epilog=textwrap.dedent(epilog))
   parser.add_argument('files', nargs="+", help="Input ISPD file names (ASCII format).  A minimum of one file name is required.")
   parser.add_argument('-i', '--addinventory', action="store_true", default="False", help='Add daily counting records into inventory table.')
   parser.add_argument('-u', '--leaduid', action="store_true", default="False", help='Standalone attachment records with leading 6-character UID.')
   parser.add_argument('-e', '--checkexisting', action="store_true", default="False", help='Check for existing record before adding record to DB.')
   parser.add_argument('-l', '--loglevel', default="info", choices=['debug', 'info', 'warning', 'error', 'critical'], help='Set the logging level.  Default = info.')

   if len(sys.argv)==1:
      parser.print_help()
      sys.exit(1)

   args = parser.parse_args(sys.argv[1:])

   return args

#=========================================================================================

if __name__ == "__main__":
   args = parse_opts()
   configure_log(loglevel=args.loglevel)
   logger = logging.getLogger(__name__)
   main(args)