#
###############################################################################
#
#     Title : PgUtil.py  -- module for misc utilities.
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 07/27/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module for global misc utilities
#
#    Github : https://github.com/NCAR/rda-python-common.git
# 
###############################################################################
#
import os
import re
import time
import datetime
import calendar
import glob
from os import path as op
import PgLOG

DATEFMTS = {
   'C' : '(CC|C)',                   # century
   'Y' : '(YYYY|YY00|YYY|YY|YEAR|YR|Y)',  # YYY means decade
   'Q' : '(QQ|Q)',                   # quarter
   'M' : '(Month|Mon|MM|M)',         # numeric or string month
   'W' : '(Week|Www|W)',             # string or numeric weedday
   'D' : '(DDD|DD|D)',               # days in year or month
   'H' : '(HHH|HH|H)',               # hours in month or day
   'N' : '(NNNN|NN|N)',              # minutes in day or hour
   'S' : '(SSSS|SS|S)'               # seconds in hour or minute
}

MONTHS = [
   "january", "february", "march",     "april",   "may",      "june",
   "july",    "august",   "september", "october", "november", "december"
]
MNS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
WDAYS = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]

WDS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]
MDAYS = [365, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

#
# dt: optional given date in format of "YYYY-MM-DD"
# return weekday: 0 - Sunday, 1 - Monday, ..., 6 - Saturday
#
def get_weekday(date = None): 

   if date is None:
      ct = time.gmtime() if PgLOG.PGLOG['GMTZ'] else time.localtime()
   else:
      ct = time.strptime(str(date), "%Y-%m-%d")

   return (ct[6]+1)%7

#
#  mn: given month string like "Jan" or "January", or numeric number 1 to 12
# Return: numeric Month if not fmt (default); three-charater or full month names for given fmt
#
def get_month(mn, fmt = None):

   if not isinstance(mn, int):
      if re.match(r'^\d+$', mn):
         mn = int(mn)
      else:
         for m in range(12):
            if re.match(mn, MONTHS[m], re.I):
               mn = m + 1
               break

   if fmt and mn > 0 and mn < 13:
      slen = len(fmt)
      if slen == 2:
         smn = "{:02}".format(mn)
      elif re.match(r'^mon', fmt, re.I):
         smn = MNS[mn-1] if slen == 3 else MONTHS[mn-1]
         if re.match(r'^Mon', fmt):
            smn = smn.capitalize()
         elif re.match(r'^MON', fmt):
            smn = smn.upper()
      else:
         smn = str(mn)
      return smn
   else:
      return mn

#
# wday: given weekday string like "Sun" or "Sunday", or numeric number 0 to 6
# Return: numeric Weekday if !fmt (default); three-charater or full week name for given fmt
#
def get_wday(wday, fmt = None):

   if not isinstance(wday, int):
      if re.match(r'^\d+$', wday):
         wday = int(wday)
      else:
         for w in range(7):
            if re.match(wday, WDAYS[w], re.I):
               wday = w
               break

   if fmt and wday >= 0 and wday <= 6:
      slen = len(fmt)
      if slen == 4:
         swday = WDAYS[w]
         if re.match(r'^We', fmt):
            swday = swday.capitalize()
         elif re.match(r'^WE', fmt):
            swday = swday.upper()
      elif slen == 3:
         swday = WDS[wday]
         if re.match(r'^Ww', fmt):
            swday = swday.capitalize()
         elif re.match(r'^WW', fmt):
            swday = swday.upper()
      else:
         swday = str(wday)         
      return swday
   else:
      return wday

#
#   file: given file name 
# Return: type if given file name is a valid online file; '' otherwise
#
def valid_online_file(file, type = None, exists = None):
   
   if exists is None or exists:
      if not op.exists(file): return ''    # file does not exist

   bname = op.basename(file)
   if re.match(r'^,.*', bname): return ''       # hidden file
   
   if re.search(r'index\.(htm|html|shtml)$', bname, re.I): return ''   # index file

   if  type and type != 'D': return type

   if re.search(r'\.(doc|php|html|shtml)(\.|$)', bname, re.I): return ''    # file with special extention

   return 'D'

#
# Return: current time string in format of HH:MM:SS
#
def curtime(getdate = False):

   ct = time.gmtime() if PgLOG.PGLOG['GMTZ'] else time.localtime()

   fmt = "%Y-%m-%d %H:%M:%S" if getdate else "%H:%M:%S"

   return time.strftime(fmt, ct)

#
# wrapper function of curtime(True) to get datetime in form of YYYY-MM-DD HH:NN:SS
#
def curdatetime():
   return curtime(True)

#
#    fmt: optional date format, defaults to YYYY-MM-DD
# Return: current (date, hour)
#
def curdatehour(fmt = None):

   ct = time.gmtime() if PgLOG.PGLOG['GMTZ'] else time.localtime()
 
   dt =  fmtdate(ct[0], ct[1], ct[2], fmt) if fmt else time.strftime("%Y-%m-%d", ct)
   
   return [dt, ct[3]]

#
#     tm: optional time in seconds since the Epoch
# Return: current date and time strings
#
def get_date_time(tm = None):

   act = ct = None
   if tm == None:
      ct = time.gmtime() if PgLOG.PGLOG['GMTZ'] else time.localtime()
   elif isinstance(tm, str):
      act = tm.split(' ')
   elif isinstance(tm, (int, float)):
      ct = time.localtime(tm)
   elif isinstance(tm, datetime.datetime):
      act = str(tm).split(' ')
   elif isinstance(tm, datetime.date):
      act = [str(tm), '00:00:00'] 
   elif isinstance(tm, datetime.time):
      act = [None, str(tm)] 

   if ct == None:
      return act if act else None
   else:
      return [time.strftime("%Y-%m-%d", ct), time.strftime("%H:%M:%S", ct)]

#
#     tm: optional time in seconds since the Epoch
# Return: current datetime strings
#
def get_datetime(tm = None):

   if tm == None:
      ct = time.gmtime() if PgLOG.PGLOG['GMTZ'] else time.localtime()
      return time.strftime("%Y-%m-%d %H:%M:%S", ct)
   elif isinstance(tm, str):
      return tm
   elif isinstance(tm, (int, float)):
      ct = time.localtime(tm)
      return time.strftime("%Y-%m-%d %H:%M:%S", ct)   
   elif isinstance(tm, datetime.datetime):
      return str(tm)
   elif isinstance(tm, datetime.date):
      return (str(tm) + ' 00:00:00')

   return tm


#
#   file: file name, get curent timestamp if missed
# Return: timestsmp string in format of 'YYYYMMDDHHMMSS
#
def timestamp(file = None):
   
   if file is None:
      ct = time.gmtime() if PgLOG.PGLOG['GMTZ'] else time.localtime()
   else:
      mt = os.stat(file).st_mtime    # file last modified time
      ct = time.gmtime(mt) if PgLOG.PGLOG['GMTZ'] else time.localtime(mt)

   return time.strftime("%Y%m%d%H%M%S", ct)

#
#  dt: datetime string
# check date/time and set to default one if empty date
#
def check_datetime(date, default):

   if not date: return default
   if not isinstance(date, str): date = str(date)
   if re.match(r'^0000', date): return default

   return date

#
#    fmt: date format, default to "YYYY-MM-DD"
# Return: new formated current date string
#
def curdate(fmt = None):

   ct = time.gmtime() if PgLOG.PGLOG['GMTZ'] else time.localtime()
 
   return fmtdate(ct[0], ct[1], ct[2], fmt) if fmt else time.strftime("%Y-%m-%d", ct)

#
# check given string to identify temporal pattern and their units
# defined in (keys DATEFMTS)
#
def temporal_pattern_units(string, seps):

   mkeys = ['D', 'Q', 'M', 'C', 'Y', 'H', 'N', 'S']
   units = {}
   match = seps[0] + "([^" + seps[1] + "]+)" + seps[1]
   patterns = re.findall(match, string)

   for pattern in patterns:
      # skip generic pattern and current time
      if re.match(r'^(P\d*|C.+C)$', pattern, re.I): continue

      for mkey in mkeys:
         ms = re.findall(DATEFMTS[mkey], pattern, re.I)
         if ms:
            if mkey == 'Q':
               units[mkey] = 3
            elif mkey == 'C':
               units[mkey] = 100
            else:
               units[mkey] = 1

            for m in ms:
               pattern = pattern.replace(m, '', 1)

   return units

#
# format output for given date and hour
#
def format_datehour(date, hour, tofmt = None, fromfmt = None):

   if date:
      datehour = format_date(str(date), tofmt, fromfmt)
   elif tofmt:
      datehour = tofmt
   else:
      datehour = ''

   if hour != None:
      if tofmt:
         fmts = re.findall(DATEFMTS['H'], datehour, re.I)
         for fmt in fmts:
            if len(fmt) > 1:
               shr = "{:02}".format(int(hour))
            else:
               shr = str(hour)
            datehour = re.sub(fmt, shr, datehour, 1)
      else:
         datehour += " {:02}".format(int(hour))

   return datehour

#
# split a date, time or datetime into an array according to
# the sep value; str to int for digital values
#
def split_datetime(sdt, sep = '\D'):

   if not isinstance(sdt, str): sdt = str(sdt)
   adt = re.split(sep, sdt)
   acnt = len(adt)
   for i in range(acnt):
      if re.match('^\d+$', adt[i]): adt[i] = int(adt[i])
   return adt

#
#    date: given date in format of fromfmt
#   tofmt: date formats; ex. "Month D, YYYY"
# fromfmt: date formats, default to YYYY-MM-DD 
#  Return: new formated date string according to tofmt
#
def format_date(cdate, tofmt = None, fromfmt = None):

   if not cdate: return cdate
   if not isinstance(cdate, str): cdate = str(cdate)
   dates = [None, None, None]
   sep = '|'
   mns = sep.join(MNS)
   months = sep.join(MONTHS)
   mkeys = ['D', 'M', 'Q', 'Y', 'C', 'H']
   PATTERNS = ['(\d\d\d\d)', '(\d+)', '(\d\d)',
               '(\d\d\d)', '(' + mns + ')', '(' + months + ')']

   if not fromfmt:
      if not tofmt:
         if re.match(r'^\d\d\d\d-\d\d-\d\d$', cdate): return cdate   # no need formatting
      ms = re.match(r'^\d+(\W)\d+(\W)\d+', cdate)
      if ms:
         fromfmt = "Y" + ms.group(1) + "M" + ms.group(2) + "D"
      else:
         PgLOG.pglog(cdate + ": Invalid date, should be in format YYYY-MM-DD", PgLOG.LGEREX)

   pattern = fromfmt
   fmts = {}
   formats = {}
   for mkey in mkeys:
      ms = re.search(DATEFMTS[mkey], pattern, re.I)
      if ms:
         fmts[mkey] = ms.group(1)
         pattern = re.sub(fmts[mkey], '', pattern)
         
   cnt = 0
   for mkey in fmts:
      fmt = fmts[mkey]
      i = len(fmt)
      if mkey == 'D':
         if i == 4: i = 1
      elif mkey == 'M':
         if i == 3: i = 4
      elif mkey == 'Y':
         if i == 4: i = 0
      formats[fromfmt.find(fmt)] = fmt
      fromfmt = fromfmt.replace(fmt, PATTERNS[i])
      cnt += 1
         
   ms = re.findall(fromfmt, cdate)
   mcnt = len(ms[0]) if ms else 0
   i = 0
   for k in sorted(formats):
      if i >= mcnt: break
      fmt = formats[k]
      val = ms[0][i]
      if re.match(r'^Y', fmt, re.I):
         dates[0] = int(val)
         if len(fmt) == 3: dates[0] *= 10
      elif re.match(r'^C', fmt, re.I):
         dates[0] = 100 * int(val)      # year at end of century
      elif re.match(r'^M', fmt, re.I):
         if re.match(r'^Mon', fmt, re.I):
            dates[1] = get_month(val)
         else:
            dates[1] = int(val)
      elif re.match(r'^Q', fmt, re.I):
         dates[1] = 3 * int(val)        # month at end of quarter
      elif re.match(r'^H', fmt, re.I):  # hour
         dates.append(int(val))
      else:    # day
         dates[2] = int(val)
      i += 1

   if len(dates) > 3:
      cdate = fmtdatehour(dates[0], dates[1], dates[2], dates[3], tofmt)
   else:
      cdate = fmtdate(dates[0], dates[1], dates[2], tofmt)

   return cdate

#
#     yr: year value
#     mn: month value, 1-12
#     dy: day of the month
#     hr: hour of the day
#     nn: minute of the hour
#     ss: second of the minute
#  tofmt: date format, ex. "Month D, YYYY", default to "YYYY-MM-DD HH:NN:SS"
# Return: new formated datehour string
#
def fmtdatetime(yr, mn, dy, hr = None, nn = None, ss = None, tofmt = None):

   if not tofmt: tofmt = "YYYY-MM-DD HH:NN:SS"

   tms = [ss, nn, hr, dy]
   fks = ['S', 'N', 'H']
   ups = [60, 60, 24]

   # adjust second/minute/hour values out of range
   for i in range(3):
      if tms[i] != None and tms[i+1] != None:
         if tms[i] < 0:
            while tms[i] < 0:
               tms[i] += ups[i]
               tms[i+1] -= 1
         elif tms[i] >= ups[i]:
            while tms[i] >= ups[i]:
               tms[i] -= ups[i]
               tms[i+1] += 1

   sdt = fmtdate(yr, mn, dy, tofmt)

   # format second/minute/hour values
   for i in range(3):
      if tms[i] != None:
         ms = re.search(DATEFMTS[fks[i]], sdt, re.I)
         if ms:
            fmt = ms.group(1)
            if len(fmt) == 2:
               str = "{:02}".format(tms[i])
            else:
               str = str(tms[i])
         sdt = re.sub(fmt, str, sdt, 1)

   return sdt

#
#     yr: year value
#     mn: month value, 1-12
#     dy: day of the month
#     hr: hour of the day
#  tofmt: date format, ex. "Month D, YYYY", default to "YYYY-MM-DD:HH"
# Return: new formated datehour string
#
def fmtdatehour(yr, mn, dy, hr, tofmt = None):

   if not tofmt: tofmt = "YYYY-MM-DD:HH"

   if hr != None and dy != None:   # adjust hour value out of range
      if hr < 0:
         while hr < 0:
            hr += 24
            dy -= 1
      elif hr > 23:
         while hr > 23:
            hr -= 24
            dy += 1

   datehour = fmtdate(yr, mn, dy, tofmt)

   if hr != None:
      ms = re.search(DATEFMTS['H'], datehour, re.I)
      if ms:
         fmt = ms.group(1)
         if len(fmt) == 2:
            shr = "{:02}".format(hr)
         else:
            shr = str(hr)
         datehour = re.sub(fmt, shr, datehour, 1)

   return datehour

#
#     yr: year value
#     mn: month value, 1-12
#     dy: day of the month
#  tofmt: date format, ex. "Month D, YYYY", default to "YYYY-MM-DD"
# Return: new formated date string
#
def fmtdate(yr, mn, dy, tofmt = None):

   (y, m, d) = adjust_ymd(yr, mn, dy)
   if not tofmt or tofmt == 'YYYY-MM-DD': return "{}-{:02}-{:02}".format(y, m, d)

   if dy != None:
      md = re.search(DATEFMTS['D'], tofmt, re.I)
      if md:
         fmt = md.group(1)   # day
         slen = len(fmt)
         if slen > 2:    # days of the year
            for i in range(1, m): d += MDAYS[i]
            sdy = "{:03}".format(d)
         elif slen == 2:
            sdy = "{:02}".format(d)
         else:
            sdy = str(d)
         tofmt = re.sub(fmt, sdy, tofmt, 1)

   if mn != None:
      md = re.search(DATEFMTS['M'], tofmt, re.I)
      if md:
         fmt = md.group(1)   # month
         slen = len(fmt)
         if slen == 2:
            smn = "{:02}".format(m)
         elif re.match(r'^mon', fmt, re.I):
            smn = MNS[m-1] if slen == 3 else MONTHS[m-1]
            if re.match(r'^Mo', fmt):
               smn = smn.capitalize()
            elif re.match(r'^MO', fmt):
               smn = smn.upper()
         else:
            smn = str(m)
         tofmt = re.sub(fmt, smn, tofmt, 1)
      else:
         md = re.search(DATEFMTS['Q'], tofmt, re.I)
         if md:
            fmt = md.group(1)   # quarter
            m = int((m+2)/3)
            smn = "{:02}".format(m) if len(fmt) == 2 else str(m) 
            tofmt = re.sub(fmt, smn, tofmt, 1)

   if yr != None:
      md = re.search(DATEFMTS['Y'], tofmt, re.I)
      if md:
         fmt = md.group(1)   # year
         slen = len(fmt)
         if slen == 2:
            syr = "{:02}".format(y%100)
         elif slen == 3:      # decade
            if y > 999: y = int(y/10)
            syr = "{:03}".format(y)
         else:
            if re.search(r'^YY00', fmt, re.I):  y = 100*int(y/100)    # hundred years 
            syr = "{:04}".format(y)
         tofmt = re.sub(fmt, syr, tofmt, 1)
      else:
         md = re.search(DATEFMTS['C'], tofmt, re.I)
         if md:
            fmt = md.group(1)   # century
            slen = len(fmt)
            if y > 999:
               y = 1 + int(y/100)
            elif y > 99:
               y = 1 + int(yr/10)
            syr = "{:02}".format(y)
            tofmt = re.sub(fmt, syr, tofmt, 1)

   return tofmt

#
# format given date and time into standard timestamp
#
def join_datetime(sdate, stime):

   if not sdate: return None
   if not stime: stime = "00:00:00"
   if not isinstance(sdate, str): sdate = str(sdate)
   if not isinstance(stime, str): stime = str(stime)
   if re.match(r'^\d:', stime): stime = '0' + stime

   return "{} {}".format(sdate, stime)

fmttime = join_datetime

#
# split a date or datetime into an array of [date, time]
#
def date_and_time(sdt):

   if not sdt: return [None, None]
   if not isinstance(sdt, str): sdt = str(sdt)
   adt = re.split(' ', sdt)
   acnt = len(adt)
   if acnt == 1: adt.append('00:00:00')
   return adt

#
# convert given date/time to unix epoch time; -1 if cannot
# 
def unixtime(stime):

   pt = [0]*9
   if not isinstance(stime, str): stime  = str(stime)
   ms = re.match(r'^(\d+)-(\d+)-(\d+)', stime)
   if ms:
      for i in range(3):
         pt[i] = int(ms.group(i+1))
   ms = re.search(r'^(\d+):(\d+):(\d+)$', stime)
   if ms:
      for i in range(3):
         pt[i+3] = int(ms.group(i+1))

   pt[8] = -1
   return time.mktime(time.struct_time(pt))

#
#  sdate: start date in form of 'YYYY' or 'YYYY-MM' or 'YYYY-MM-DD'
#  edate: end date in form of 'YYYY' or 'YYYY-MM' or 'YYYY-MM-DD'
# Return: list of start and end dates in format of YYYY-MM-DD
#
def daterange(sdate, edate):

   if sdate:
      if not isinstance(sdate, str): sdate = str(sdate)
      if not re.search(r'\d+-\d+-\d+', sdate):
         ms = re.match(r'^(\W*)(\d+)-(\d+)(\W*)$', sdate)
         if ms:
            sdate = "{}{}-{}-01{}".format(ms.group(1), ms.group(2), ms.group(3), ms.group(4))
         else:
            ms = re.match(r'^(\W*)(\d+)(\W*)$', sdate)
            if ms:
               sdate = "{}{}-01-01{}".format(ms.group(1), ms.group(2), ms.group(3))

   if edate:
      if not isinstance(edate, str): edate = str(edate)
      if not re.search(r'\d+-\d+-\d+', edate):
         ms = re.match(r'^(\W*)(\d+)-(\d+)(\W*)$', edate)
         if ms:
            edate = "{}{}-{}-01{}".format(ms.group(1), ms.group(2), ms.group(3), ms.group(4))
            edate = adddate(edate, 0, 1, -1)
         else:
            ms = re.match(r'^(\W*)(\d+)(\W*)$', edate)
            if ms:
               edate = "{}{}-12-31{}".format(ms.group(1), ms.group(2), ms.group(3))

   return [sdate, edate]

#
# date to datetime range
#
def dtrange(dates):

   date = dates[0]
   if date:
      if not isinstance(date, str): date = str(date)
      dates[0] = date + ' 00:00:00'
   date = dates[1]
   if date:
      if not isinstance(date, str): date = str(date)
      dates[1] = date + ' 23:59:59'

   return dates

#
#  sdate: starting date in format of 'YYYY-MM-DD'
#  edate: ending date
#    fmt: period format, ex. "YYYYMon-YYYMon", default to "YYYYMM-YYYYMM"
# Return: a string of formated period
#
def format_period(sdate, edate, fmt = None):
  
   period = ''

   if not fmt:
      sfmt = efmt = "YYYYMM"
      sep = '-'
   else:
      ms = re.match(r'^(.*)(\s*-\s*)(.*)$', fmt)
      if ms:
         (sfmt, sep, efmt) = ms.groups()
      else:
         sfmt = fmt
         efmt = None
         sep  = ''

   if sdate:
      if not isinstance(sdate, str): sdate = str(sdate)
      ms = re.search(r'(\d+)-(\d+)-(\d+)', sdate)
      if ms:
         (yr, mn, dy) = ms.groups()
         period = fmtdate(int(yr), int(mn), int(dy), sfmt)

   if sep: period += sep

   if efmt:
      if re.search(r'current', efmt, re.I):
         period += efmt
      elif edate:
         if not isinstance(edate, str): edate = str(edate)
         ms = re.search(r'(\d+)-(\d+)-(\d+)', edate)
         if ms:
            (yr, mn, dy) = ms.groups()
            period += fmtdate(int(yr), int(mn), int(dy), efmt)

   return period

#
#  dsid: given dataset id in form of dsNNN(.|)N, NNNN.N or [a-z]NNNNNN
# newid: True to format a new dsid; defaults to False for now
# returns a new or old dsid according to the newid option
#
def format_dataset_id(dsid, newid = PgLOG.PGLOG['NEWDSID'], logact = PgLOG.LGEREX):

   dsid = str(dsid)
   ms = re.match(r'^([a-z])(\d\d\d)(\d\d\d)$', dsid)
   if ms:
      ids = list(ms.groups())
      if ids[0] not in PgLOG.PGLOG['DSIDCHRS']:
         if logact: PgLOG.pglog("{}: dsid leading character must be '{}'".format(dsid, PgLOG.PGLOG['DSIDCHRS']), logact)
         return dsid
      if newid: return dsid
      if ids[2][:2] != '00':
         if logact: PgLOG.pglog(dsid + ": Cannot convert new dsid to old format", logact)
         return dsid
      return 'ds{}.{}'.format(ids[1], ids[2][2])

   ms = re.match(r'^ds(\d\d\d)(\.|)(\d)$', dsid, re.I)
   if not ms: ms = re.match(r'^(\d\d\d)(\.)(\d)$', dsid)
   if ms:
      if newid:
         return "d{}00{}".format(ms.group(1), ms.group(3))
      else:
         return 'ds{}.{}'.format(ms.group(1), ms.group(3))

   if logact: PgLOG.pglog(dsid + ": invalid dataset id", logact)
   return dsid

#
#  dsid: given dataset id in form of dsNNN(.|)N, NNNN.N or [a-z]NNNNNN
# newid: True to format a new dsid; defaults to False for now
# returns a new or old metadata dsid according to the newid option
#
def metadata_dataset_id(dsid, newid = PgLOG.PGLOG['NEWDSID'], logact = PgLOG.LGEREX):

   ms = re.match(r'^([a-z])(\d\d\d)(\d\d\d)$', dsid)
   if ms:
      ids = list(ms.groups())
      if ids[0] not in PgLOG.PGLOG['DSIDCHRS']:
         if logact: PgLOG.pglog("{}: dsid leading character must be '{}'".format(dsid, PgLOG.PGLOG['DSIDCHRS']), logact)
         return dsid
      if newid: return dsid
      if ids[2][:2] != '00':
         if logact: PgLOG.pglog(dsid + ": Cannot convert new dsid to old format", logact)
         return dsid
      return '{}.{}'.format(ids[1], ids[2][2])

   ms = re.match(r'^ds(\d\d\d)(\.|)(\d)$', dsid)
   if not ms: ms = re.match(r'^(\d\d\d)(\.)(\d)$', dsid)
   if ms:
      if newid:
         return "d{}00{}".format(ms.group(1), ms.group(3))
      else:
         return '{}.{}'.format(ms.group(1), ms.group(3))

   if logact: PgLOG.pglog(dsid + ": invalid dataset id", logact)
   return dsid


#
# idstr: string holding a dsid in form of dsNNN(.|)N, NNNN.N or [a-z]NNNNNN
# and find it according to the flag value O (Old), N (New) or B (Both) formats
# returns dsid if found in given id string; None otherwise
#
def find_dataset_id(idstr, flag = 'B', logact = 0):

   if flag in 'NB':
      ms = re.search(r'(^|\W)(([a-z])\d{6})($|\D)', idstr)
      if ms and ms.group(3) in PgLOG.PGLOG['DSIDCHRS']: return ms.group(2)
   if flag in 'OB':
      ms = re.search(r'(^|\W)(ds\d\d\d(\.|)\d)($|\D)', idstr)
      if not ms: ms = re.search(r'(^|\W)(\d\d\d\.\d)($|\D)', idstr)
      if ms: return ms.group(2)

   if logact: PgLOG.pglog("{} : No valid dsid found for flag {}".format(idstr, flag), logact)
   return None

#
# find and convert all found dsids according to old/new dsids
# for newid = False/True
#
def convert_dataset_ids(idstr, newid = PgLOG.PGLOG['NEWDSID'], logact = 0):

   flag = 'O' if newid else 'N'
   cnt = 0
   if idstr:
      while True:
         dsid = find_dataset_id(idstr, flag = flag)
         if not dsid: break
         ndsid = format_dataset_id(dsid, newid = newid, logact = logact)
         if ndsid != dsid: idstr = idstr.replace(dsid, ndsid)
         cnt += 1

   return (idstr, cnt)

#
# records: dict of mutiple records,
#     idx: index of the records to return
#  Return: a dict to the idx record out of records
#
def onerecord(records, idx):

   record = {}

   for fld in records:
      record[fld] = records[fld][idx]

   return record

#
# records: dict of mutiple records,
#  record: record to add
#     idx: index of the record to add
#  Return: add a record to a dict of lists
#
def addrecord(records, record, idx):

   if records is None: records = {}   # initialize dist of lists structure
   if not records:
      for key in record:
         records[key] = []

   for key in record:
      slen = len(records[key])         
      if idx < slen:
         records[key][idx] = record[key]
      else:
         while idx > slen:
            records[key].append(None)
            slen += 1
         records[key].append(record[key])

   return records

#
# convert a hash with multiple rows from pgmget() to an array of hashes
#
def hash2array(hrecs, hkeys = None):

   if not hkeys: hkeys = list(hrecs)
   acnt = len(hrecs[hkeys[0]]) if hrecs and hkeys[0] in hrecs else 0
   arecs = [None]*acnt
   for i in range(acnt):
      arec = {}
      for hkey in hkeys: arec[hkey] = hrecs[hkey][i]
      arecs[i] = arec
   
   return arecs

#
# convert an array of hashes to a hash with multiple rows for pgmget()
#
def array2hash(arecs, hkeys = None):

   hrecs = {}
   acnt = len(arecs) if arecs else 0
   if acnt > 0:
      if not hkeys: hkeys = list(arecs[0])
      for hkey in hkeys:
         hrecs[hkey] = [None]*acnt
         for i in range(acnt): hrecs[hkey][i] = arecs[i][hkey]

   return hrecs

#
# records: dict of mutiple records,
#     opt: 0 - column count,
#          1 - row count,
#          2 - both
#  Return: a single number or list of two dependend on given opt
#
def hashcount(records, opt = 0):

   ret = [0, 0]

   if records:
      clen = len(records)
      if opt == 0 or opt == 2:
         ret[0] = clen
      if opt == 1 or opt == 2:
         ret[1] = len(next(iter(records.values())))
         
   return ret if opt == 2 else ret[opt]

#
#   adict: dict a
#   bdict: dict b
# default: default values if missed
#  unique: unique join if set
#  Return: the joined dict records with default value for missing ones
#          For unique join, a record in bdict must not be contained in adict already
#
def joinhash(adict, bdict, default = None, unique = None):

   if not bdict: return adict
   if not adict: return bdict

   akeys = list(adict.keys())
   bkeys = list(bdict.keys())
   acnt = len(adict[akeys[0]])
   bcnt = len(bdict[bkeys[0]])
   ckeys = []    # common keys for unique joins

   # check and assign default value for missing keys in adict
   for bkey in bkeys:
      if bkey in akeys:
         if unique and bkey not in ckeys: ckeys.append(bkey)
      else:
         adict[bkey] = [default]*acnt

   # check and assign default value for missing keys in bdict
   for akey in akeys:
      if akey in bkeys:
         if unique and akey not in ckeys: ckeys.append(akey)
      else:
         bdict[akey] = [default]*bcnt

   if unique:    # append bdict
      kcnt = len(ckeys)
      for i in range(bcnt):
         j = 0
         while(j < acnt):
            k = 0
            for ckey in ckeys:
               if pgcmp(adict[ckey][j], bdict[ckey][i]): break
               k += 1
            if k >= kcnt: break
            j += 1

         if j >= acnt:
            for key in adict:
               adict[key].append(bdict[key][i])
   else:
      for key in adict:
         adict[key].extend(bdict[key])

   return adict

#
#   lst1: list 1
#   lst2: list 2
# unique: unique join if set
# Return: the joined list
#
def joinarray(lst1, lst2, unique = None):
   
   if not lst2: return lst1
   if not lst1: return lst2

   cnt1 = len(lst1)
   cnt2 = len(lst2)

   if unique:
      for i in (cnt2):
         for j in (cnt1):
            if pgcmp(lst1[j], lst2[i]) != 0: break
         if j >= cnt1:
           lst1.append(lst2[i])
   else:
      lst1.extend(lst2)

   return lst1

#
# Function: crosshash(ahash, bhash)
#   Return: a reference to the cross-joined hash records 
#
def crosshash(ahash, bhash):

   if not bhash: return ahash
   if not ahash: return bhash

   akeys = list(ahash.keys())
   bkeys = list(bhash.keys())
   acnt = len(ahash[akeys[0]])
   bcnt = len(bhash[bkeys[0]])
   rets = {}
   for key in akeys: rets[key] = []
   for key in bkeys: rets[key] = []
   for i in range(acnt):
      for j in range(bcnt):
         for key in akeys: rets[key].append(ahash[key][i])
         for key in bkeys: rets[key].append(bhash[key][j])

   return rets

#
# strip database and table names for a field name
#
def strip_field(field):
   ms = re.search(r'\.([^\.]+)$', field)
   if ms: field = ms.group(1)
   
   return field

#
#   pgrecs: dict obterned from pgmget()
#     flds: list of single letter fields to be sorted on
#     hash: table dict for pre-defined fields
# patterns: optional list of temporal patterns for order fields
#   Return: a sorted dict list
#
def sorthash(pgrecs, flds, hash, patterns = None):
   
   fcnt = len(flds)    # count of fields to be sorted on

   # set sorting order, descenting (-1) or ascenting (1)
   # get the full field names to be sorted on
   desc = [1]*fcnt
   fields = []
   nums = [1]*fcnt   # initialize each column as numerical
   for i in range(fcnt):
      if flds[i].islower(): desc[i] = -1
      fld = strip_field(hash[flds[i].upper()][1])
      fields.append(fld)

   count = len(pgrecs[fields[0]])    # row count of pgrecs

   if count < 2: return pgrecs       # no need of sording
   pcnt = len(patterns) if patterns else 0

   # prepare the dict list for sortting
   srecs = []
   for i in range(count):
      pgrec = onerecord(pgrecs, i)
      rec = []
      for j in range(fcnt):
         if j < pcnt and patterns[j]:
            # get the temporal part of each value matching the pattern
            val = format_date(pgrec[fields[j]], "YYYYMMDDHH", patterns[j])
         else:
            # sort on the whole value if no pattern given
            val = pgrec[fields[j]]

         if nums[j]: nums[j] = pgnum(val)
         rec.append(val)
      rec.append(i)   # add column to cache the row index
      srecs.append(rec)

   srecs = quicksort(srecs, 0, count-1, desc, fcnt, nums)

   # sort pgrecs according the cached row index column in ordered srecs
   rets = {}
   for fld in pgrecs:
      rets[fld] = []

   for i in range(count):
      pgrec = onerecord(pgrecs, srecs[i][fcnt])
      for fld in pgrecs:
         rets[fld].append(pgrec[fld])

   return rets

#
# Return: the number of days bewteen date1 and date2
#
def diffdate(date1, date2):

   ut1 = ut2 = 0
   if date1: ut1 = unixtime(date1)
   if date2: ut2 = unixtime(date2)
   return round((ut1 - ut2)/86400)   # 24*60*60 

#
# Return: the number of seconds bewteen time1 and time2
#
def difftime(time1, time2):

   ut1 = ut2 = 0
   if time1: ut1 = unixtime(time1)
   if time2: ut2 = unixtime(time2)
   return round(ut1 - ut2)

diffdatetime = difftime

#
# Return: the number of days between date and '1970-01-01 00:00:00'
#
def get_days(cdate):

   return diffdate(str(cdate), '1970-01-01')

#
# Function: get_month_days(date)
# 
#   Return: the number of days in given month
#
def get_month_days(cdate):

   ms = re.match(r'^(\d+)-(\d+)', str(cdate))
   if ms:
      yr = int(ms.group(1))
      mn = int(ms.group(2))
      return calendar.monthrange(yr, mn)[1]
   else:
      return 0

#
# Function: validate_date(date)
# 
#   Return: a date in format of YYYY-MM-DD thar all year/month/day are validated
#
def validate_date(cdate):

   ms = re.match(r'^(\d+)-(\d+)-(\d+)', str(cdate))
   if ms:
      (yr, mn, dy) = (int(m) for m in ms.groups())
      if yr < 1000:
         yr += 2000
      elif yr > 9999:
         yr %= 10000
      if mn < 1:
         mn = 1
      elif mn > 12:
         mn = 12
      md = calendar.monthrange(yr, mn)[1]
      if dy < 1:
         dy = 1
      elif dy > md:
         dy = md
      cdate = '{}-{:02d}-{:02d}'.format(yr, mn, dy)

   return cdate

#
# Function: get_date(days)
# 
#   Return: the date in format of "YYYY-MM-DD" for given number of days
#   from '1970-01-01 00:00:00'
#
def get_date(days):

   return adddate('1970-01-01', 0, 0, int(days))

#
# compare date/hour and return the different hours
#
def diffdatehour(date1, hour1, date2, hour2):

   if hour1 is None: hour1 = 23
   if hour2 is None: hour2 = 23
   return (hour1 - hour2) + 24*diffdate(date1, date2)

# 
# hour difference between GMT and local time 
#
def diffgmthour():

   tg = time.gmtime()
   tl = time.localtime()
   dg = fmtdate(tg[0], tg[1], tg[2])
   dl = fmtdate(tl[0], tl[1], tl[2])
   hg = tg[3]
   hl = tl[3]

   return diffdatehour(dg, hg, dl, hl)

#
# compare date and time (if given) and return 1, 0 and -1
#
def cmptime(date1, time1, date2, time2):

   stime1 = join_datetime(date1, time1)
   stime2 = join_datetime(date2, time2)

   return pgcmp(stime1, stime2)

#
#   date: the original date in format of 'YYYY-MM-DD',
#     mf: the number of month fractions to add
#     nf: number of fractions of a month
# Return: new date
#
def addmonth(cdate, mf, nf = 1):

   if not mf: return cdate
   if not nf or nf < 2: return adddate(cdate, 0, mf, 0)

   ms = re.match(r'^(\d+)-(\d+)-(\d+)$', cdate)
   if ms:
      (syr, smn, sdy) = ms.groups()
      yr = int(syr)
      mn = int(smn)
      ody = int(sdy)
      dy = 0            # set to end of previous month
      ndy = int(30/nf)  # number of days in each fraction
      while ody > ndy:
         dy += ndy
         ody -= ndy

      dy += mf * ndy
      if mf > 0:
         while dy >= 30:
            dy -= 30
            mn += 1
      else:
         while dy < 0:
            dy += 30
            mn -= 1

      dy += ody
      cdate = fmtdate(yr, mn, dy)      

   return cdate

# add yr years & mn months to yearmonth ym in format YYYYMM
def addyearmonth(ym, yr, mn):

   if yr == None: yr = 0
   if mn == None: mn = 0

   ms =re.match(r'^(\d\d\d\d)(\d\d)$', ym)
   if ms:
      (syr, smn) = ms.groups()
      yr = int(syr)
      mn = int(smn)
      if mn < 0:
         while mn < 0:
            yr -= 1
            mn += 12
      else:
         while mn > 12:
            yr += 1
            mn -= 12

      ym = "{:04}{:02}".format(yr, mn)      

   return ym

#
#  a wrapper to adddate()
#
def addNoLeapDate(cdate, yr, mn, dy): return adddate(cdate, yr, mn, dy)

#
# set number of days in Beburary for Leap year according PgLOG.PGLOG['NOLEAP']
#
def set_leap_mdays(year):

   if not PgLOG.PGLOG['NOLEAP'] and calendar.isleap(year):
      MDAYS[0] = 366
      MDAYS[2] = 29
      ret = 1
   else:
      MDAYS[0] = 365
      MDAYS[2] = 28
      ret = 0
   return ret

#
# wrap on calendar.isleap()
#
def is_leapyear(year): return calendar.isleap(year)
 
#
# reutn 1 if is end of month
#
def is_end_month(yr, mn, dy):

   set_leap_mdays(yr)
   return 1 if dy == MDAYS[mn] else 0

#
# adust the year, month and day values that are out of ranges
#
def adjust_ymd(yr, mn, dy):

   if yr is None: yr = 1970
   if mn is None: mn = 1
   if dy is None: dy = 1

   while True:
      if mn > 12:
         yr += 1
         mn -= 12
         continue
      elif mn < 1:
         yr -= 1
         mn += 12
         continue

      set_leap_mdays(yr)

      if dy < 1:
         if(dy < -MDAYS[0]):
            yr -= 1
            dy += MDAYS[0]
         else:
            mn -= 1
            if mn < 1:
              yr -= 1
              mn += 12
            dy += MDAYS[mn]
         continue
      elif dy > MDAYS[mn]:
         if(dy > MDAYS[0]):
            dy -= MDAYS[0]
            yr += 1
         else:
            dy -= MDAYS[mn]
            mn += 1
         continue

      break

   return [yr, mn, dy]

#
#   date: the original date in format of 'YYYY-MM-DD',
#     yr: the number of years to add/subtract from the odate for positive/negative value,
#     mn: the number of months to add/subtract from the odate for positive/negative value,
#     dy: the number of days to add/subtract from the odate for positive/negative value)
# 
# Return: new date
#
def adddate(cdate, yr, mn = 0, dy = 0, tofmt = None):

   if not cdate: return cdate
   if not isinstance(cdate, str): cdate = str(cdate)
   if yr is None:
      yr = 0
   elif isinstance(yr, str):
      yr = int(yr)
   if mn is None:
      mn = 0
   elif isinstance(mn, str):
      mn = int(mn)
   if dy is None:
      dy = 0
   elif isinstance(dy, str):
      dy = int(dy)

   ms = re.search(r'(\d+)-(\d+)-(\d+)', cdate)
   if not ms: return cdate    # non-standard date format
   (nyr, nmn, ndy) = (int(m) for m in ms.groups())
   mend = 0
   if mn and ndy > 27: mend = is_end_month(nyr, nmn, ndy)
   if yr: nyr += yr
   if mn:
      (nyr, nmn, tdy) = adjust_ymd(nyr, nmn+mn+1, 0)
      if mend: ndy = tdy
   if dy: ndy += dy

   return fmtdate(nyr, nmn, ndy, tofmt)

#
# add given hours to the initial date and time
#
def addhour(sdate, stime, nhour):

   if nhour and isinstance(nhour, str): nhour = int(nhour)
   if sdate and not isinstance(sdate, str): sdate = str(sdate)
   if stime and not isinstance(stime, str): stime = str(stime)
   if not nhour: return [sdate, stime]

   hr = dy = 0
   ms = re.match(r'^(\d+)', stime)
   if ms:
      shr = ms.group(1)
      hr = int(shr) + nhour
      if hr < 0:
         while hr < 0:
            dy -= 1
            hr += 24
      else:
         while hr > 23:
            dy += 1
            hr -= 24

   shour = "{:02}".format(hr)
   if shr != shour: stime = re.sub(shr, shour, stime, 1)
   if dy: sdate = adddate(sdate, 0, 0, dy)

   return [sdate, stime]

#
# add given years, months, days and hours to the initial date and hour
#
def adddatehour(sdate, nhour, yr, mn, dy, hr = 0):

   if sdate and not isinstance(sdate, str): sdate = str(sdate)
   if hr:
      if nhour != None:
         if isinstance(nhour, str): nhour = int(nhour)
         hr += nhour
      if hr < 0:
         while hr < 0:
            dy -= 1
            hr += 24
      else:
         while hr > 23:
            dy += 1
            hr -= 24
      if nhour != None: nhour = hr

   if yr or mn or dy: sdate = adddate(sdate, yr, mn, dy)
   
   return [sdate, nhour]

#
# add given yyyy, mm, dd, hh, nn, ss to sdatetime
# if nf, add fraction of month only
#
def adddatetime(sdatetime, yy, mm, dd, hh, nn, ss, nf = 0):

   if sdatetime and not isinstance(sdatetime, str): sdatetime = str(sdatetime)
   (sdate, stime) = re.split(' ', sdatetime)

   if hh or nn or ss: (sdate, stime) = addtime(sdate, stime, hh, nn, ss)
   if nf:
      sdate = addmonth(sdate, mm, nf)
      mm = 0
   if yy or mm or dd: sdate = adddate(sdate, yy, mm, dd)

   return "{} {}".format(sdate, stime)

#
# add given yyyy, mm, dd, hh, nn, ss to sdatetime
# if nf, add fraction of month only
#
def adddatetime(sdatetime, yy, mm, dd, hh, nn, ss, nf = 0):

   if sdatetime and not isinstance(sdatetime, str): sdatetime = str(sdatetime)
   (sdate, stime) = re.split(' ', sdatetime)

   if hh or nn or ss: (sdate, stime) = addtime(sdate, stime, hh, nn, ss)
   if nf:
      sdate = addmonth(sdate, mm, nf)
      mm = 0
   if yy or mm or dd: sdate = adddate(sdate, yy, mm, dd)

   return "{} {}".format(sdate, stime)

#
# add given hours, minutes and seconds to the initial date and time
#
def addtime(sdate, stime, h, m, s):

   if sdate and not isinstance(sdate, str): sdate = str(sdate)
   if stime and not isinstance(stime, str): sdate = str(stime)
   ups = (60, 60, 24)
   tms = [0, 0, 0, 0]   # (sec, min, hour, day)

   if s: tms[0] += s
   if m: tms[1] += m
   if h: tms[2] += h
   if stime:
      ms = re.match(r'^(\d+):(\d+):(\d+)$', stime)
      if ms:
         tms[2] += int(ms.group(1))
         tms[1] += int(ms.group(2))
         tms[0] += int(ms.group(3))

   for i in range(3):
      if tms[i] < 0:
         while tms[i] < 0:
            tms[i] += ups[i]
            tms[i+1] -= 1
      elif tms[i] >= ups[i]:
         while tms[i] >= ups[i]:
            tms[i] -= ups[i]
            tms[i+1] += 1

   stime = "{:02}:{:02}:{:02}".format(tms[2], tms[1], tms[0])
   if tms[3]: sdate = adddate(sdate, 0, 0, tms[3])

   return [sdate, stime]

#
# add time interval array to datetime
# opt = -1 - minus, 0 - begin time, 1 - add (default)
#
def addintervals(sdatetime, intv, opt = 1):

   if not isinstance(sdatetime, str): sdatetime = str(sdatetime)
   if not intv: return sdatetime
   tv = [0]*7
   i = 0
   for v in intv:
      tv[i] = v
      i += 1
      
   # assume the given datetime is end of the current interval;
   # add one second to set it to beginning of the next one
   if opt == 0: sdatetime = adddatetime(sdatetime, 0, 0, 0 ,0, 0, 1)

   if opt < 1: # negative intervals for minus
      for i in range(6):
         if tv[i]: tv[i] = -tv[i]

   return adddatetime(sdatetime, tv[0], tv[1], tv[2], tv[3], tv[4], tv[5], tv[6])

#
# adjust end date to the specified day days for frequency of year/month/week
# end of period if days == 0
# nf - number of fractions of a month, for unit of 'M' only
#
def enddate(sdate, days, unit, nf = 0):

   if sdate and not isinstance(sdate, str): sdate = str(sdate)
   if days and isinstance(days, str): days = int(days)
   if not (unit and unit in 'YMW'): return sdate

   if unit == 'Y':
      ms = re.match(r'^(\d+)', sdate)
      if ms:
         yr = int(ms.group(1))
         if days:
            mn = 1
            dy = days
         else:
            mn = 12
            dy = 31
         sdate = fmtdate(yr, mn, dy)
   elif unit == 'M':
      ms = re.match(r'^(\d+)-(\d+)-(\d+)', sdate)
      if ms:
         (yr, mn, dy) = (int(m) for m in ms.groups())
      else:
         ms = re.match(r'^(\d+)-(\d+)', sdate)
         if ms:
            (yr, mn) = (int(m) for m in ms.groups())
            dy = 1
         else:
            return sdate
         
      if not nf or nf == 1:
         nd = days if days else calendar.monthrange(yr, mn)[1]
         if nd != dy: sdate = fmtdate(yr, mn, nd)
      else:
         val = int(30/nf)
         if dy >= 28:
            mf = nf
         else:
            mf = int(dy/val)
            if (mf*val) < dy: mf += 1
         if days:
            dy = (mf-1)*val + days
         elif mf < nf:
            dy = mf*val
         else:
            mn += 1
            dy = 0
         sdate = fmtdate(yr, mn, dy)
   elif unit == 'W':
      val = get_weekday(sdate)
      if days != val: sdate = adddate(sdate, 0, 0, days-val)

   return sdate

#
# adjust end time to the specified h/n/s for frequency of hour/mimute/second
#
def endtime(stime, unit):

   if stime and not isinstance(stime, str): stime = str(stime)
   if not (unit and unit in 'HNS'): return stime

   if stime:
      tm = split_datetime(stime, 'T')
   else:
      tm = [0, 0, 0]

   if unit == 'H':
      tm[1] = tm[2] = 59
   elif unit == 'N':
      tm[2] = 59
   elif unit != 'S':
      tm[0] = 23
      tm[1] = tm[2] = 59

   return "{:02}:{:02}:{:02}".format(tm[0], tm[1]. tm[2])

#
# adjust end time to the specified h/n/s for frequency of year/month/week/day/hour/mimute/second
#
def enddatetime(sdatetime, unit, days = 0, nf = 0):

   if sdatetime and not isinstance(sdatetime, str): sdatetime = str(sdatetime)
   if not (unit and unit in 'YMWDHNS'): return sdatetime
   (sdate, stime) = re.split(' ', sdatetime)

   if unit in 'HNS':
      stime = endtime(stime, unit)
   else:
      sdate = enddate(sdate, days, unit, nf)
   return "{} {}".format(sdate, stime)

#
# get the string length dynamically
#
def get_column_length(colname, values):

   clen = len(colname) if colname else 2  # initial column length as the length of column title

   for val in values:
      if val is None: continue
      sval = str(val)
      if sval and not re.search(r'\n', sval):
         slen = len(sval)
         if slen > clen: clen = slen

   return clen

#
# Function: hour2time()
#   Return: time string in format of date HH:MM:SS
#
def hour2time(sdate, nhour, endtime = 0):

   if sdate and not isinstance(sdate, str): sdate = str(sdate)
   stime = "{:02}:".format(nhour)
   if endtime:
      stime += "59:59"
   else:
      stime += "00:00"

   if sdate:
      return "{} {}".format(sdate, stime)
   else:
      return stime

#
# Function: time2hour()
#   Return: list of date and hour
#
def time2hour(stime):
   
   sdate = nhour = None
   times = stime.split(' ')

   if len(times) == 2:
      sdate = times[0]
      stime = times[1]

   ms = re.match(r'^(\d+)', stime)
   if ms: nhour = int(ms.group(1))

   return [sdate, nhour]

#
# get the all column widths
#
def all_column_widths(pgrecs, flds, tdict):

   colcnt = len(flds)
   lens = [0]*colcnt
   for i in range(colcnt):
      fld = flds[i]
      if fld not in tdict: continue
      field = strip_field(tdict[fld][1])
      lens[i] = get_column_length(None, pgrecs[field])

   return lens

#
# check a give value, return 1 if numeric, 0 therwise
#
def pgnum(val):

   if not isinstance(val, str): val = str(val)
   ms = re.match(r'^\-{0,1}(\d+|\d+\.\d*|d*\.\d+)([eE]\-{0,1}\d+)*$', val)
   return 1 if ms else 0

#
# Function: pgcmp(val1, val2)
#   Return: 0 if both empty or two values are identilcal; -1 if val1 < val2; otherwise 1
#
def pgcmp(val1, val2, ignorecase = 0, num = 0):

   if val1 is None:
      if val2 is None:
         return 0
      else:
         return -1
   elif val2 is None:
      return 1
   typ1 = type(val1)
   typ2 = type(val2)
   if typ1 != typ2:
      if num:
         if typ1 is str:
            typ1 = int
            val1 = int(val1)
         if typ2 is str:
            typ2 = int
            val2 = int(val2)  
      else:
         if typ1 != str:
            typ1 = str
            val1 = str(val1)  
         if typ2 != str:
            typ2 = str
            val2 = str(val2)  

   if typ1 is str:
      if num:
         if typ1 is str and pgnum(val1) and pgnum(val2):
            val1 = int(val1)
            val2 = int(val2)
      elif ignorecase:
         val1 = val1.lower()
         val2 = val2.lower()

   if val1 > val2:
      return 1
   elif val1 < val2:
      return -1
   else:
      return 0

#
# infiles: initial file list
#  Return: final file list with all the subdirectories expanded
#
def recursive_files(infiles):

   ofiles = []

   for file in infiles:
      if op.isdir(file):
         ofiles.extend(recursive_files(glob.glob(file + "/*")))
      else:
         ofiles.append(file)

   return ofiles

#
#   lidx: lower index limit  (including)
#   hidx: higher index limit (excluding)
#    key: string value to be searched,
#   list: reference to a sorted list where the key is searched)
# Return: index if found; -1 otherwise
#
def asearch(lidx, hidx, key, list):

   ret = -1
   if (hidx - lidx) < 11:   # use linear search for less than 11 items
      for midx in range(lidx, hidx):
         if key == list[midx]:
            ret = midx
            break
   else:
      midx = (lidx + hidx)/2
      if key == list[midx]:
         ret = midx
      elif key < list[midx]:
         ret = asearch(lidx, midx, key, list)
      else:
         ret = asearch(midx + 1, hidx, key, list)

   return ret

#
#   lidx: lower index limit  (including)
#   hidx: higher index limit (excluding)
#    key: string value to be searched,
#   list: reference to a sorted list where the key is searched)
# Return: index if found; -1 otherwise
#
def psearch(lidx, hidx, key, list):

   ret = -1
   if (hidx - lidx) < 11: # use linear search for less than 11 items
      for midx in range(lidx, hidx):
         if re.search(list[midx], key):
            ret = midx
            break
   else:
      midx = int((lidx + hidx)/2)
      if re.search(list[midx], key):
         ret = midx
      elif key < list[midx]:
         ret = psearch(lidx, midx, key, list)
      else:
         ret = psearch(midx + 1, hidx, key, list)

   return ret

#
# quicksort for pattern
#
def quicksort(srecs, lo, hi, desc, cnt, nums = None):
   
   i = lo
   j = hi
   mrec = srecs[int((lo+hi)/2)]

   while True:
      while cmp_records(srecs[i], mrec, desc, cnt, nums) < 0: i += 1 
      while cmp_records(srecs[j], mrec, desc, cnt, nums) > 0: j -= 1
      if i <= j:
         if i < j:
            tmp = srecs[i]
            srecs[i] = srecs[j]
            srecs[j] = tmp
         i += 1
         j -= 1
      if i > j: break

   #recursion
   if lo < j: srecs = quicksort(srecs, lo, j, desc, cnt, nums)
   if i < hi: srecs = quicksort(srecs, i, hi, desc, cnt, nums)
   
   return srecs

def cmp_records(arec, brec, desc, cnt, nums):
   
   for i in range(cnt):
      num = nums[i] if nums else 0
      ret = pgcmp(arec[i], brec[i], 0, num)
      if ret != 0:
         return (ret*desc[i])

   return 0   # identical records

#
# format one floating point value
#
def format_float_value(val, precision = 2):

   units = ('B', 'KB', 'MB', 'GB', 'TB', 'PB')

   if val is None:
      return ''
   elif not isinstance(val, int):
      val = int(val)

   idx = 0
   while val >= 1000 and idx < 5:
      val /= 1000
      idx += 1

   return "{:.{}f}{}".format(val, precision, units[idx])

#
# check a file is a ASCII text one
# return 1 if yes, 0 if not; or -1 if file not exists
#
def is_text_file(fname):

   ret = -1
   if op.isfile(fname):
      buf = PgLOG.pgsystem("file -b " + fname, PgLOG.LOGWRN, 20)
      ret = 1 if buf and re.search(r'(^|\s)(text|script|data)', buf) else 0

   return ret
