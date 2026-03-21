###############################################################################
#     Title: pg_util.py  -- module for misc utilities.
#    Author: Zaihua Ji,  zji@ucar.edu
#      Date: 07/27/2020
#             2025-01-10 transferred to package rda_python_common from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-11-20 convert to class PgUtil
#   Purpose: python library module for global misc utilities
#    Github: https://github.com/NCAR/rda-python-common.git
###############################################################################
import os
import re
import time
import datetime
import calendar
import glob
from os import path as op
from .pg_log import PgLOG

class PgUtil(PgLOG):

   """Miscellaneous date/time, dataset ID, and record-manipulation utilities.

   Extends PgLOG with helpers for date arithmetic, formatting, temporal pattern
   parsing, column-oriented record manipulation, sorting, searching, and file
   classification. Inherits all logging utilities from PgLOG.

   Instance Attributes:
      DATEFMTS (dict): Regex fragments for each temporal unit (C, Y, Q, M, W, D, H, N, S).
      MONTHS (list[str]): Full lowercase month names, index 0 = January.
      MNS (list[str]): Three-letter lowercase month abbreviations, index 0 = 'jan'.
      WDAYS (list[str]): Full lowercase weekday names, index 0 = 'sunday'.
      WDS (list[str]): Three-letter lowercase weekday abbreviations, index 0 = 'sun'.
      MDAYS (list[int]): Days per month; index 0 = days in year (365/366),
                          indices 1-12 = days in each month (Feb updated for leap years).
   """

   def __init__(self):
      """Initialise PgUtil with date/time lookup tables.

      Calls PgLOG.__init__(), then populates DATEFMTS (temporal format regex fragments),
      MONTHS, MNS, WDAYS, WDS (month/weekday name lists), and MDAYS (days-per-month array).
      """
      super().__init__()  # initialize parent class
      self.DATEFMTS = {
         'C': '(CC|C)',                   # century
         'Y': '(YYYY|YY00|YYY|YY|YEAR|YR|Y)',  # YYY means decade
         'Q': '(QQ|Q)',                   # quarter
         'M': '(Month|Mon|MM|M)',         # numeric or string month
         'W': '(Week|Www|W)',             # string or numeric weedday
         'D': '(DDD|DD|D)',               # days in year or month
         'H': '(HHH|HH|H)',               # hours in month or day
         'N': '(NNNN|NN|N)',              # minutes in day or hour
         'S': '(SSSS|SS|S)'               # seconds in hour or minute
      }
      self.MONTHS = [
         "january", "february", "march",     "april",   "may",      "june",
         "july",    "august",   "september", "october", "november", "december"
      ]
      self.MNS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
      self.WDAYS = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
      self.WDS = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]
      self.MDAYS = [365, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

   # dt: optional given date in format of "YYYY-MM-DD"
   # return weekday: 0 - Sunday, 1 - Monday, ..., 6 - Saturday
   def get_weekday(self, date = None):
      """Return the weekday number for a given date, using Sunday=0 convention.

      Args:
         date (str | None): Date string in 'YYYY-MM-DD' format; uses today when None.

      Returns:
         int: Weekday number where 0 = Sunday, 1 = Monday, …, 6 = Saturday.
      """
      if date is None:
         ct = time.gmtime() if self.PGLOG['GMTZ'] else time.localtime()
      else:
         ct = time.strptime(str(date), "%Y-%m-%d")
      return (ct[6]+1)%7

   #  mn: given month string like "Jan" or "January", or numeric number 1 to 12
   # Return: numeric Month if not fmt (default); three-charater or full month names for given fmt
   def get_month(self, mn, fmt = None):
      """Convert a month value to a numeric index or a formatted name string.

      Args:
         mn (int | str): Month as an integer (1-12), numeric string, or name/abbreviation.
         fmt (str | None): Output format token (e.g. 'MM', 'Mon', 'Month'); returns the
                           numeric month when None.

      Returns:
         int | str: Numeric month (1-12) when fmt is None; formatted string otherwise.
      """
      if not isinstance(mn, int):
         if re.match(r'^\d+$', mn):
            mn = int(mn)
         else:
            for m in range(12):
               if re.match(mn, self.MONTHS[m], re.I):
                  mn = m + 1
                  break
      if fmt and mn > 0 and mn < 13:
         slen = len(fmt)
         if slen == 2:
            smn = "{:02}".format(mn)
         elif re.match(r'^mon', fmt, re.I):
            smn = self.MNS[mn-1] if slen == 3 else self.MONTHS[mn-1]
            if re.match(r'^Mon', fmt):
               smn = smn.capitalize()
            elif re.match(r'^MON', fmt):
               smn = smn.upper()
         else:
            smn = str(mn)
         return smn
      else:
         return mn

   # wday: given weekday string like "Sun" or "Sunday", or numeric number 0 to 6
   # Return: numeric Weekday if !fmt (default); three-charater or full week name for given fmt
   def get_wday(self, wday, fmt = None):
      """Convert a weekday value to a numeric index or a formatted name string.

      Args:
         wday (int | str): Weekday as 0-6 integer, numeric string, or name/abbreviation.
         fmt (str | None): Output format token (e.g. 'W', 'Www', 'Week'); returns the
                           numeric weekday when None.

      Returns:
         int | str: Numeric weekday (0=Sunday … 6=Saturday) when fmt is None;
                    formatted string otherwise.
      """
      if not isinstance(wday, int):
         if re.match(r'^\d+$', wday):
            wday = int(wday)
         else:
            for w in range(7):
               if re.match(wday, self.WDAYS[w], re.I):
                  wday = w
                  break
      if fmt and wday >= 0 and wday <= 6:
         slen = len(fmt)
         if slen == 4:
            swday = self.WDAYS[wday]
            if re.match(r'^We', fmt):
               swday = swday.capitalize()
            elif re.match(r'^WE', fmt):
               swday = swday.upper()
         elif slen == 3:
            swday = self.WDS[wday]
            if re.match(r'^Ww', fmt):
               swday = swday.capitalize()
            elif re.match(r'^WW', fmt):
               swday = swday.upper()
         else:
            swday = str(wday)
         return swday
      else:
         return wday

   #   file: given file name
   # Return: type if given file name is a valid online file; '' otherwise
   @staticmethod
   def valid_online_file(file, type = None, exists = None):
      """Determine whether a file path is a valid, publicly-servable data file.

      Rejects files that do not exist (unless exists is False), hidden files (basename
      starts with a comma), index HTML files, and files with special extensions
      (.doc, .php, .html, .shtml).

      Args:
         file (str): File path to check.
         type (str | None): Caller-supplied file type; returned unchanged when not 'D'.
         exists (bool | None): When False, skips the filesystem existence check.

      Returns:
         str: The file type (defaulting to 'D') on success, or '' when rejected.
      """
      if exists is None or exists:
         if not op.exists(file): return ''    # file does not exist
      bname = op.basename(file)
      if re.match(r'^,.*', bname): return ''       # hidden file
      if re.search(r'index\.(htm|html|shtml)$', bname, re.I): return ''   # index file
      if  type and type != 'D': return type
      if re.search(r'\.(doc|php|html|shtml)(\.|$)', bname, re.I): return ''    # file with special extention
      return 'D'

   # Return: current time string in format of HH:MM:SS
   def curtime(self, getdate = False):
      """Return the current time (or datetime) as a formatted string.

      Args:
         getdate (bool): When True returns 'YYYY-MM-DD HH:MM:SS'; otherwise 'HH:MM:SS'.

      Returns:
         str: Formatted current time string.
      """
      ct = time.gmtime() if self.PGLOG['GMTZ'] else time.localtime()
      fmt = "%Y-%m-%d %H:%M:%S" if getdate else "%H:%M:%S"
      return time.strftime(fmt, ct)

   # wrapper function of curtime(True) to get datetime in form of YYYY-MM-DD HH:NN:SS
   def curdatetime(self):
      """Return the current date and time as 'YYYY-MM-DD HH:MM:SS'.

      Returns:
         str: Current datetime string.
      """
      return self.curtime(True)

   #    fmt: optional date format, defaults to YYYY-MM-DD
   # Return: current (date, hour)
   def curdatehour(self, fmt = None):
      """Return the current date and hour as a two-element list.

      Args:
         fmt (str | None): Date format string passed to fmtdate(); defaults to 'YYYY-MM-DD'.

      Returns:
         list: [date_str, hour_int] where hour is 0-23.
      """
      ct = time.gmtime() if self.PGLOG['GMTZ'] else time.localtime()
      dt =  self.fmtdate(ct[0], ct[1], ct[2], fmt) if fmt else time.strftime("%Y-%m-%d", ct)
      return [dt, ct[3]]

   #     tm: optional time in seconds since the Epoch
   # Return: current date and time strings
   def get_date_time(self, tm = None):
      """Split a time value into [date_str, time_str] components.

      Accepts multiple input types and normalises them to a two-element list.

      Args:
         tm: Input value — None (→ now), str ('YYYY-MM-DD HH:MM:SS'), int/float
             (Unix epoch), datetime.datetime, datetime.date, or datetime.time.

      Returns:
         list | None: [date_str, time_str] on success, or None when tm is unrecognised.
      """
      act = ct = None
      if tm is None:
         ct = time.gmtime() if self.PGLOG['GMTZ'] else time.localtime()
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
      if ct is None:
         return act if act else None
      else:
         return [time.strftime("%Y-%m-%d", ct), time.strftime("%H:%M:%S", ct)]

   #     tm: optional time in seconds since the Epoch
   # Return: current datetime strings
   def get_datetime(self, tm = None):
      """Return a datetime value normalised to a 'YYYY-MM-DD HH:MM:SS' string.

      Args:
         tm: Input — None (→ now), str (returned as-is), int/float (Unix epoch),
             datetime.datetime, or datetime.date.

      Returns:
         str: Datetime string, or the original value when the type is unrecognised.
      """
      if tm is None:
         ct = time.gmtime() if self.PGLOG['GMTZ'] else time.localtime()
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

   #   file: file name, get curent timestamp if missed
   # Return: timestsmp string in format of 'YYYYMMDDHHMMSS
   def timestamp(self, file = None):
      """Return a compact timestamp string in 'YYYYMMDDHHMMSS' format.

      Args:
         file (str | None): Path to a file whose mtime is used; uses current time when None.

      Returns:
         str: 14-character timestamp string.
      """
      if file is None:
         ct = time.gmtime() if self.PGLOG['GMTZ'] else time.localtime()
      else:
         mt = os.stat(file).st_mtime    # file last modified time
         ct = time.gmtime(mt) if self.PGLOG['GMTZ'] else time.localtime(mt)
      return time.strftime("%Y%m%d%H%M%S", ct)

   #  dt: datetime string
   # check date/time and set to default one if empty date
   @staticmethod
   def check_datetime(date, default):
      """Return date if non-empty and non-zero, otherwise return the default.

      Args:
         date (str | any): Date value to check; coerced to str when not already.
         default (str): Fallback value returned when date is falsy or starts with '0000'.

      Returns:
         str: Validated date string or default.
      """
      if not date: return default
      if not isinstance(date, str): date = str(date)
      if re.match(r'^0000', date): return default
      return date

   #    fmt: date format, default to "YYYY-MM-DD"
   # Return: new formated current date string
   def curdate(self, fmt = None):
      """Return the current date as a formatted string.

      Args:
         fmt (str | None): Date format string for fmtdate(); defaults to 'YYYY-MM-DD'.

      Returns:
         str: Formatted current date string.
      """
      ct = time.gmtime() if self.PGLOG['GMTZ'] else time.localtime()
      return self.fmtdate(ct[0], ct[1], ct[2], fmt) if fmt else time.strftime("%Y-%m-%d", ct)

   # check given string to identify temporal pattern and their units
   # defined in (keys self.DATEFMTS)
   def temporal_pattern_units(self, string, seps):
      """Parse a string for temporal format tokens and return their unit mappings.

      Extracts patterns enclosed by seps delimiters, ignores generic ('P…') and
      current-time ('C…C') patterns, and maps each found DATEFMTS key to its unit
      multiplier (quarter→3, century→100, others→1).

      Args:
         string (str): Input string containing delimited temporal patterns.
         seps (str): Two-character string where seps[0] is the opening delimiter
                     and seps[1] the closing delimiter.

      Returns:
         dict: Mapping of DATEFMTS key (e.g. 'Y', 'M', 'D') to unit multiplier.
      """
      mkeys = ['D', 'Q', 'M', 'C', 'Y', 'H', 'N', 'S']
      units = {}
      match = seps[0] + "([^" + seps[1] + "]+)" + seps[1]
      patterns = re.findall(match, string)
      for pattern in patterns:
         # skip generic pattern and current time
         if re.match(r'^(P\d*|C.+C)$', pattern, re.I): continue
         for mkey in mkeys:
            ms = re.findall(self.DATEFMTS[mkey], pattern, re.I)
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

   # format output for given date and hour
   def format_datehour(self, date, hour, tofmt = None, fromfmt = None):
      """Format a date and hour value into a string using an optional format template.

      When tofmt is given, substitutes the hour token in the formatted date string.
      When tofmt is absent, appends the zero-padded hour with a space separator.

      Args:
         date (str | any): Date value; formatted via format_date() when truthy.
         hour (int | None): Hour value (0-23); appended/substituted when not None.
         tofmt (str | None): Output date+hour format string.
         fromfmt (str | None): Input date format string passed to format_date().

      Returns:
         str: Formatted date-hour string.
      """
      if date:
         datehour = self.format_date(str(date), tofmt, fromfmt)
      elif tofmt:
         datehour = tofmt
      else:
         datehour = ''
      if hour != None:
         if tofmt:
            fmts = re.findall(self.DATEFMTS['H'], datehour, re.I)
            for fmt in fmts:
               if len(fmt) > 1:
                  shr = "{:02}".format(int(hour))
               else:
                  shr = str(hour)
               datehour = re.sub(fmt, shr, datehour, 1)
         else:
            datehour += " {:02}".format(int(hour))
      return datehour

   # split a date, time or datetime into an array according to
   # the sep value; str to int for digital values
   @staticmethod
   def split_datetime(sdt, sep = r'\D'):
      """Split a date, time, or datetime string into a list of integer/string parts.

      Splits on the regex sep pattern and converts purely numeric parts to int.

      Args:
         sdt (str | any): Datetime value; coerced to str when not already.
         sep (str): Regex separator pattern; defaults to any non-digit character.

      Returns:
         list: Mixed int/str parts of the split datetime.
      """
      if not isinstance(sdt, str): sdt = str(sdt)
      adt = re.split(sep, sdt)
      acnt = len(adt)
      for i in range(acnt):
         if re.match(r'^\d+$', adt[i]): adt[i] = int(adt[i])
      return adt

   #    date: given date in format of fromfmt
   #   tofmt: date formats; ex. "Month D, YYYY"
   # fromfmt: date formats, default to YYYY-MM-DD
   #  Return: new formated date string according to tofmt
   def format_date(self, cdate, tofmt = None, fromfmt = None):
      """Reformat a date string from one format to another.

      Parses cdate according to fromfmt (auto-detected when omitted) and renders it
      using tofmt. Supports year, century, quarter, month (numeric and name), and
      day tokens defined in DATEFMTS.

      Args:
         cdate (str | any): Input date value; coerced to str when needed.
         tofmt (str | None): Output format string (e.g. 'Month D, YYYY'); when None
                             returns 'YYYY-MM-DD'.
         fromfmt (str | None): Input format string; auto-detected from cdate when None.

      Returns:
         str | None: Reformatted date string, or the original value when cdate is falsy.
      """
      if not cdate: return cdate
      if not isinstance(cdate, str): cdate = str(cdate)
      dates = [None, None, None]
      sep = '|'
      mns = sep.join(self.MNS)
      months = sep.join(self.MONTHS)
      mkeys = ['D', 'M', 'Q', 'Y', 'C', 'H']
      PATTERNS = [r'(\d\d\d\d)', r'(\d+)', r'(\d\d)',
                  r'(\d\d\d)', '(' + mns + ')', '(' + months + ')']
      if not fromfmt:
         if not tofmt:
            if re.match(r'^\d\d\d\d-\d\d-\d\d$', cdate): return cdate   # no need formatting
         ms = re.match(r'^\d+(\W)\d+(\W)\d+', cdate)
         if ms:
            fromfmt = "Y" + ms.group(1) + "M" + ms.group(2) + "D"
         else:
            self.pglog(cdate + ": Invalid date, should be in format YYYY-MM-DD", self.LGEREX)
      pattern = fromfmt
      fmts = {}
      formats = {}
      for mkey in mkeys:
         ms = re.search(self.DATEFMTS[mkey], pattern, re.I)
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
               dates[1] = self.get_month(val)
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
         cdate = self.fmtdatehour(dates[0], dates[1], dates[2], dates[3], tofmt)
      else:
         cdate = self.fmtdate(dates[0], dates[1], dates[2], tofmt)
      return cdate

   #     yr: year value
   #     mn: month value, 1-12
   #     dy: day of the month
   #     hr: hour of the day
   #     nn: minute of the hour
   #     ss: second of the minute
   #  tofmt: date format, ex. "Month D, YYYY", default to "YYYY-MM-DD HH:NN:SS"
   # Return: new formated datehour string
   def fmtdatetime(self, yr, mn, dy, hr = None, nn = None, ss = None, tofmt = None):
      """Format year/month/day/hour/minute/second components into a datetime string.

      Carries over-range values (e.g. seconds ≥ 60) into the next unit automatically
      before formatting. Delegates date formatting to fmtdate().

      Args:
         yr (int): Year.
         mn (int): Month (1-12).
         dy (int): Day of month.
         hr (int | None): Hour (0-23).
         nn (int | None): Minute (0-59).
         ss (int | None): Second (0-59).
         tofmt (str | None): Output format; defaults to 'YYYY-MM-DD HH:NN:SS'.

      Returns:
         str: Formatted datetime string.
      """
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
      sdt = self.fmtdate(yr, mn, dy, tofmt)
      # format second/minute/hour values
      for i in range(3):
         if tms[i] != None:
            ms = re.search(self.DATEFMTS[fks[i]], sdt, re.I)
            if ms:
               fmt = ms.group(1)
               if len(fmt) == 2:
                  sval = "{:02}".format(tms[i])
               else:
                  sval = str(tms[i])
            sdt = re.sub(fmt, sval, sdt, 1)   
      return sdt

   #     yr: year value
   #     mn: month value, 1-12
   #     dy: day of the month
   #     hr: hour of the day
   #  tofmt: date format, ex. "Month D, YYYY", default to "YYYY-MM-DD:HH"
   # Return: new formated datehour string
   def fmtdatehour(self, yr, mn, dy, hr, tofmt = None):
      """Format year/month/day/hour components into a date-hour string.

      Normalises out-of-range hour values (negative or ≥ 24) by adjusting the day.

      Args:
         yr (int): Year.
         mn (int): Month (1-12).
         dy (int): Day of month.
         hr (int | None): Hour (0-23); may be negative or ≥ 24 (adjusted automatically).
         tofmt (str | None): Output format; defaults to 'YYYY-MM-DD:HH'.

      Returns:
         str: Formatted date-hour string.
      """
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
      datehour = self.fmtdate(yr, mn, dy, tofmt)
      if hr != None:
         ms = re.search(self.DATEFMTS['H'], datehour, re.I)
         if ms:
            fmt = ms.group(1)
            if len(fmt) == 2:
               shr = "{:02}".format(hr)
            else:
               shr = str(hr)
            datehour = re.sub(fmt, shr, datehour, 1)
      return datehour

   #     yr: year value
   #     mn: month value, 1-12
   #     dy: day of the month
   #  tofmt: date format, ex. "Month D, YYYY", default to "YYYY-MM-DD"
   # Return: new formated date string
   def fmtdate(self, yr, mn, dy, tofmt = None):
      """Format year, month, and day components into a date string.

      Applies adjust_ymd() to normalise out-of-range values, then substitutes day,
      month (numeric or name), quarter, year, and century tokens from tofmt.

      Args:
         yr (int | None): Year component.
         mn (int | None): Month component (1-12).
         dy (int | None): Day of month.
         tofmt (str | None): Output format string; defaults to 'YYYY-MM-DD'.

      Returns:
         str: Formatted date string.
      """
      (y, m, d) = self.adjust_ymd(yr, mn, dy)
      if not tofmt or tofmt == 'YYYY-MM-DD': return "{}-{:02}-{:02}".format(y, m, d)
      if dy != None:
         md = re.search(self.DATEFMTS['D'], tofmt, re.I)
         if md:
            fmt = md.group(1)   # day
            slen = len(fmt)
            if slen > 2:    # days of the year
               for i in range(1, m): d += self.MDAYS[i]
               sdy = "{:03}".format(d)
            elif slen == 2:
               sdy = "{:02}".format(d)
            else:
               sdy = str(d)
            tofmt = re.sub(fmt, sdy, tofmt, 1)
      if mn != None:
         md = re.search(self.DATEFMTS['M'], tofmt, re.I)
         if md:
            fmt = md.group(1)   # month
            slen = len(fmt)
            if slen == 2:
               smn = "{:02}".format(m)
            elif re.match(r'^mon', fmt, re.I):
               smn = self.MNS[m-1] if slen == 3 else self.MONTHS[m-1]
               if re.match(r'^Mo', fmt):
                  smn = smn.capitalize()
               elif re.match(r'^MO', fmt):
                  smn = smn.upper()
            else:
               smn = str(m)
            tofmt = re.sub(fmt, smn, tofmt, 1)
         else:
            md = re.search(self.DATEFMTS['Q'], tofmt, re.I)
            if md:
               fmt = md.group(1)   # quarter
               m = int((m+2)/3)
               smn = "{:02}".format(m) if len(fmt) == 2 else str(m)
               tofmt = re.sub(fmt, smn, tofmt, 1)
      if yr != None:
         md = re.search(self.DATEFMTS['Y'], tofmt, re.I)
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
            md = re.search(self.DATEFMTS['C'], tofmt, re.I)
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

   # format given date and time into standard timestamp
   @staticmethod
   def join_datetime(sdate, stime):
      """Combine separate date and time strings into a single datetime string.

      Args:
         sdate (str | any): Date portion; coerced to str when not already.
                            Returns None when falsy.
         stime (str | any): Time portion; defaults to '00:00:00' when falsy.
                            A leading single digit is zero-padded.

      Returns:
         str | None: Combined 'YYYY-MM-DD HH:MM:SS' string, or None when sdate is falsy.
      """
      if not sdate: return None
      if not stime: stime = "00:00:00"
      if not isinstance(sdate, str): sdate = str(sdate)
      if not isinstance(stime, str): stime = str(stime)
      if re.match(r'^\d:', stime): stime = '0' + stime
      return "{} {}".format(sdate, stime)
   fmttime = join_datetime

   # split a date or datetime into an array of [date, time]
   @staticmethod
   def date_and_time(sdt):
      """Split a datetime string into [date, time] parts.

      Args:
         sdt (str | any): Datetime value; coerced to str when not already.

      Returns:
         list: [date_str, time_str]; time_str defaults to '00:00:00' when absent;
               [None, None] when sdt is falsy.
      """
      if not sdt: return [None, None]
      if not isinstance(sdt, str): sdt = str(sdt)
      adt = re.split(' ', sdt)
      acnt = len(adt)
      if acnt == 1: adt.append('00:00:00')
      return adt

   # convert given date/time to unix epoch time; -1 if cannot
   @staticmethod
   def unixtime(stime):
      """Convert a date/time string to a Unix epoch timestamp.

      Parses the date portion (YYYY-MM-DD) and optional time portion (HH:MM:SS)
      from stime and returns the corresponding local epoch seconds.

      Args:
         stime (str | any): Date or datetime string; coerced to str when needed.

      Returns:
         float: Unix epoch timestamp via time.mktime().
      """
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

   #  sdate: start date in form of 'YYYY' or 'YYYY-MM' or 'YYYY-MM-DD'
   #  edate: end date in form of 'YYYY' or 'YYYY-MM' or 'YYYY-MM-DD'
   # Return: list of start and end dates in format of YYYY-MM-DD
   def daterange(self, sdate, edate):
      """Expand partial dates in a [sdate, edate] pair to full 'YYYY-MM-DD' strings.

      Partial dates:
      - 'YYYY' → sdate becomes 'YYYY-01-01', edate becomes 'YYYY-12-31'.
      - 'YYYY-MM' → sdate becomes 'YYYY-MM-01', edate becomes last day of that month.

      Args:
         sdate (str | any | None): Start date (partial or full).
         edate (str | any | None): End date (partial or full).

      Returns:
         list: [sdate_str, edate_str] both in 'YYYY-MM-DD' format.
      """
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
               edate = self.adddate(edate, 0, 1, -1)
            else:
               ms = re.match(r'^(\W*)(\d+)(\W*)$', edate)
               if ms:
                  edate = "{}{}-12-31{}".format(ms.group(1), ms.group(2), ms.group(3))
      return [sdate, edate]

   # date to datetime range
   @staticmethod
   def dtrange(dates):
      """Extend a [date, date] pair to a [datetime, datetime] pair covering the full days.

      Appends ' 00:00:00' to dates[0] and ' 23:59:59' to dates[1].

      Args:
         dates (list): Two-element list [start_date, end_date]; modified in-place.

      Returns:
         list: The same list with datetime strings.
      """
      date = dates[0]
      if date:
         if not isinstance(date, str): date = str(date)
         dates[0] = date + ' 00:00:00'
      date = dates[1]
      if date:
         if not isinstance(date, str): date = str(date)
         dates[1] = date + ' 23:59:59'
      return dates

   #  sdate: starting date in format of 'YYYY-MM-DD'
   #  edate: ending date
   #    fmt: period format, ex. "YYYYMon-YYYMon", default to "YYYYMM-YYYYMM"
   # Return: a string of formated period
   def format_period(self, sdate, edate, fmt = None):
      """Format a date range as a period string using start and end format tokens.

      When fmt is None, produces 'YYYYMM-YYYYMM'. The format string may contain a
      hyphen separator dividing the start and end sub-formats.

      Args:
         sdate (str | any | None): Start date in 'YYYY-MM-DD' or similar format.
         edate (str | any | None): End date in 'YYYY-MM-DD' or similar format.
         fmt (str | None): Period format like 'YYYYMon-YYYYMon'; the literal word
                           'current' in the end sub-format is kept verbatim.

      Returns:
         str: Formatted period string.
      """
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
            period = self.fmtdate(int(yr), int(mn), int(dy), sfmt)
      if sep: period += sep   
      if efmt:
         if re.search(r'current', efmt, re.I):
            period += efmt
         elif edate:
            if not isinstance(edate, str): edate = str(edate)
            ms = re.search(r'(\d+)-(\d+)-(\d+)', edate)
            if ms:
               (yr, mn, dy) = ms.groups()
               period += self.fmtdate(int(yr), int(mn), int(dy), efmt)
      return period

   #  dsid: given dataset id in form of dsNNN(.|)N, NNNN.N or [a-z]NNNNNN
   # newid: True to format a new dsid; defaults to False for now
   # returns a new or old dsid according to the newid option
   def format_dataset_id(self, dsid, newid = None, logact = None):
      """Normalise a dataset ID to old-style ('ds###.#') or new-style ('[a-z]######') format.

      Args:
         dsid (str | any): Input dataset ID in any recognised format.
         newid (bool | None): True → return new-style ID; False → old-style;
                              None → uses PGLOG['NEWDSID'].
         logact (int | None): Logging action for invalid IDs; defaults to LGEREX.

      Returns:
         str: Normalised dataset ID string.
      """
      if newid is None: newid = self.PGLOG['NEWDSID']
      if logact is None: logact = self.LGEREX
      dsid = str(dsid)
      ms = re.match(r'^([a-z])(\d\d\d)(\d\d\d)$', dsid)
      if ms:
         ids = list(ms.groups())
         if ids[0] not in self.PGLOG['DSIDCHRS']:
            if logact: self.pglog("{}: dsid leading character must be '{}'".format(dsid, self.PGLOG['DSIDCHRS']), logact)
            return dsid
         if newid: return dsid
         if ids[2][:2] != '00':
            if logact: self.pglog(dsid + ": Cannot convert new dsid to old format", logact)
            return dsid
         return 'ds{}.{}'.format(ids[1], ids[2][2])
      ms = re.match(r'^ds(\d\d\d)(\.|)(\d)$', dsid, re.I)
      if not ms: ms = re.match(r'^(\d\d\d)(\.)(\d)$', dsid)
      if ms:
         if newid:
            return "d{}00{}".format(ms.group(1), ms.group(3))
         else:
            return 'ds{}.{}'.format(ms.group(1), ms.group(3))
      if logact: self.pglog(dsid + ": invalid dataset id", logact)
      return dsid

   #  dsid: given dataset id in form of dsNNN(.|)N, NNNN.N or [a-z]NNNNNN
   # newid: True to format a new dsid; defaults to False for now
   # returns a new or old metadata dsid according to the newid option
   def metadata_dataset_id(self, dsid, newid = None, logact = None):
      """Normalise a dataset ID to metadata format (no 'ds' prefix for old style).

      Like format_dataset_id() but old-style output is '###.#' instead of 'ds###.#'.

      Args:
         dsid (str | any): Input dataset ID.
         newid (bool | None): True → new-style; False → metadata old-style;
                              None → uses PGLOG['NEWDSID'].
         logact (int | None): Logging action for invalid IDs; defaults to LGEREX.

      Returns:
         str: Normalised metadata dataset ID string.
      """
      if newid is None: newid = self.PGLOG['NEWDSID']
      if logact is None: logact = self.LGEREX
      ms = re.match(r'^([a-z])(\d\d\d)(\d\d\d)$', dsid)
      if ms:
         ids = list(ms.groups())
         if ids[0] not in self.PGLOG['DSIDCHRS']:
            if logact: self.pglog("{}: dsid leading character must be '{}'".format(dsid, self.PGLOG['DSIDCHRS']), logact)
            return dsid
         if newid: return dsid
         if ids[2][:2] != '00':
            if logact: self.pglog(dsid + ": Cannot convert new dsid to old format", logact)
            return dsid
         return '{}.{}'.format(ids[1], ids[2][2])
      ms = re.match(r'^ds(\d\d\d)(\.|)(\d)$', dsid)
      if not ms: ms = re.match(r'^(\d\d\d)(\.)(\d)$', dsid)
      if ms:
         if newid:
            return "d{}00{}".format(ms.group(1), ms.group(3))
         else:
            return '{}.{}'.format(ms.group(1), ms.group(3))
      if logact: self.pglog(dsid + ": invalid dataset id", logact)
      return dsid

   # idstr: string holding a dsid in form of dsNNN(.|)N, NNNN.N or [a-z]NNNNNN
   # and find it according to the flag value O (Old), N (New) or B (Both) formats
   # returns dsid if found in given id string; None otherwise
   def find_dataset_id(self, idstr, flag = 'B', logact = 0):
      """Search a string for a dataset ID in old, new, or both formats.

      Args:
         idstr (str): String to search.
         flag (str): 'N' = new-style only, 'O' = old-style only, 'B' = both (default).
         logact (int): Logging action when no ID is found; 0 = silent.

      Returns:
         str | None: The first matching dataset ID string, or None when not found.
      """
      if flag in 'NB':
         ms = re.search(r'(^|\W)(([a-z])\d{6})($|\D)', idstr)
         if ms and ms.group(3) in self.PGLOG['DSIDCHRS']: return ms.group(2)
      if flag in 'OB':
         ms = re.search(r'(^|\W)(ds\d\d\d(\.|)\d)($|\D)', idstr)
         if not ms: ms = re.search(r'(^|\W)(\d\d\d\.\d)($|\D)', idstr)
         if ms: return ms.group(2)
      if logact: self.pglog("{}: No valid dsid found for flag {}".format(idstr, flag), logact)
      return None

   # find and convert all found dsids according to old/new dsids
   # for newid = False/True
   def convert_dataset_ids(self, idstr, newid = None, logact = 0):
      """Find and convert all dataset IDs in a string between old and new formats.

      Args:
         idstr (str | None): Input string possibly containing dataset IDs.
         newid (bool | None): True → convert old→new; False → new→old;
                              None → uses PGLOG['NEWDSID'].
         logact (int): Logging action flags; default 0.

      Returns:
         tuple: (converted_str, count) where count is the number of IDs converted.
      """
      if newid is None: newid = self.PGLOG['NEWDSID']
      flag = 'O' if newid else 'N'
      cnt = 0
      if idstr:
         while True:
            dsid = self.find_dataset_id(idstr, flag = flag)
            if not dsid: break
            ndsid = self.format_dataset_id(dsid, newid = newid, logact = logact)
            if ndsid != dsid: idstr = idstr.replace(dsid, ndsid)
            cnt += 1
      return (idstr, cnt)

   # records: dict of mutiple records,
   #     idx: index of the records to return
   #  Return: a dict to the idx record out of records
   @staticmethod
   def onerecord(records, idx):
      """Extract a single row from a column-oriented multi-record dict.

      Args:
         records (dict): Column-oriented dict (field → list-of-values) from pgmget().
         idx (int): Row index to extract.

      Returns:
         dict: Row dict with field → scalar value for the given index.
      """
      record = {}
      for fld in records:
         record[fld] = records[fld][idx]
      return record

   # records: dict of mutiple records,
   #  record: record to add
   #     idx: index of the record to add
   #  Return: add a record to a dict of lists
   @staticmethod
   def addrecord(records, record, idx):
      """Insert or replace a row in a column-oriented multi-record dict.

      Appends None padding when idx exceeds the current length of a column list.
      Initialises records to an empty dict when None is passed.

      Args:
         records (dict | None): Column-oriented dict to update, or None to create one.
         record (dict): Single-row dict to insert.
         idx (int): Target row index.

      Returns:
         dict: The updated (or newly created) column-oriented dict.
      """
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

   # convert a hash with multiple rows from pgmget() to an array of hashes
   @staticmethod
   def hash2array(hrecs, hkeys = None):
      """Convert a column-oriented dict (from pgmget) to a list of row dicts.

      Args:
         hrecs (dict): Column-oriented dict mapping field_name → list_of_values.
         hkeys (list | None): Keys to include; uses all keys when None.

      Returns:
         list[dict]: List of row dicts, one per row.
      """
      if not hkeys: hkeys = list(hrecs)
      acnt = len(hrecs[hkeys[0]]) if hrecs and hkeys[0] in hrecs else 0
      arecs = [None]*acnt
      for i in range(acnt):
         arec = {}
         for hkey in hkeys: arec[hkey] = hrecs[hkey][i]
         arecs[i] = arec
      return arecs

   # convert an array of hashes to a hash with multiple rows for pgmget()
   @staticmethod
   def array2hash(arecs, hkeys = None):
      """Convert a list of row dicts to a column-oriented dict (for pgmget-style use).

      Args:
         arecs (list[dict]): List of row dicts.
         hkeys (list | None): Keys to include; uses all keys from arecs[0] when None.

      Returns:
         dict: Column-oriented dict mapping field_name → list_of_values.
      """
      hrecs = {}
      acnt = len(arecs) if arecs else 0
      if acnt > 0:
         if not hkeys: hkeys = list(arecs[0])
         for hkey in hkeys:
            hrecs[hkey] = [None]*acnt
            for i in range(acnt): hrecs[hkey][i] = arecs[i][hkey]
      return hrecs

   # records: dict of mutiple records,
   #     opt: 0 - column count,
   #          1 - row count,
   #          2 - both
   #  Return: a single number or list of two dependend on given opt
   @staticmethod
   def hashcount(records, opt = 0):
      """Return the column count, row count, or both for a column-oriented dict.

      Args:
         records (dict): Column-oriented dict from pgmget().
         opt (int): 0 = column count (default), 1 = row count, 2 = [col_count, row_count].

      Returns:
         int | list: Single count for opt 0/1, or [col_count, row_count] for opt 2.
      """
      ret = [0, 0]
      if records:
         clen = len(records)
         if opt == 0 or opt == 2:
            ret[0] = clen
         if opt == 1 or opt == 2:
            ret[1] = len(next(iter(records.values())))
      return ret if opt == 2 else ret[opt]

   #   adict: dict a
   #   bdict: dict b
   # default: default values if missed
   #  unique: unique join if set
   #  Return: the joined dict records with default value for missing ones
   #          For unique join, a record in bdict must not be contained in adict already
   @staticmethod
   def joinhash(adict, bdict, default = None, unique = None):
      """Concatenate two column-oriented dicts, filling missing keys with a default.

      For a unique join, a row from bdict is only appended when no row in adict
      has identical values for all common keys.

      Args:
         adict (dict | None): Base column-oriented dict; returned unchanged when bdict is falsy.
         bdict (dict | None): Dict to append; returned when adict is falsy.
         default: Fill value for keys absent in one of the dicts; default None.
         unique: When truthy, skip bdict rows already present in adict.

      Returns:
         dict: Merged column-oriented dict.
      """
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
                  if PgUtil.pgcmp(adict[ckey][j], bdict[ckey][i]): break
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

   #   lst1: list 1
   #   lst2: list 2
   # unique: unique join if set
   # Return: the joined list
   @staticmethod
   def joinarray(lst1, lst2, unique = None):
      """Concatenate two lists, optionally skipping duplicates.

      Args:
         lst1 (list | None): Base list; returned unchanged when lst2 is falsy.
         lst2 (list | None): List to append; returned when lst1 is falsy.
         unique: When truthy, only appends elements from lst2 not already in lst1.

      Returns:
         list: Merged list.
      """
      if not lst2: return lst1
      if not lst1: return lst2
      cnt1 = len(lst1)
      cnt2 = len(lst2)
      if unique:
         for i in range(cnt2):
            for j in range(cnt1):
               if PgUtil.pgcmp(lst1[j], lst2[i]) != 0: break
            if j >= cnt1:
              lst1.append(lst2[i])
      else:
         lst1.extend(lst2)
      return lst1

   # Function: crosshash(ahash, bhash)
   #   Return: a reference to the cross-joined hash records
   @staticmethod
   def crosshash(ahash, bhash):
      """Produce a Cartesian product of two column-oriented dicts.

      Every row in ahash is combined with every row in bhash, producing
      acnt × bcnt rows in the result.

      Args:
         ahash (dict | None): First column-oriented dict.
         bhash (dict | None): Second column-oriented dict.

      Returns:
         dict: Cross-joined column-oriented dict with all keys from both inputs.
      """
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

   # strip database and table names for a field name
   @staticmethod
   def strip_field(field):
      """Strip schema and table prefixes from a dot-qualified field name.

      Args:
         field (str): Possibly qualified field name like 'schema.table.column'.

      Returns:
         str: The bare column name after the last dot, or field unchanged when no dot.
      """
      ms = re.search(r'\.([^\.]+)$', field)
      if ms: field = ms.group(1)
      return field

   #   pgrecs: dict obterned from pgmget()
   #     flds: list of single letter fields to be sorted on
   #     hash: table dict for pre-defined fields
   # patterns: optional list of temporal patterns for order fields
   #   Return: a sorted dict list
   def sorthash(self, pgrecs, flds, hash, patterns = None):
      """Sort a column-oriented dict on one or more fields using quicksort.

      Lowercase field letters in flds indicate descending order; uppercase = ascending.
      Temporal patterns in patterns are used to extract a comparable key for each value.

      Args:
         pgrecs (dict): Column-oriented dict to sort.
         flds (str | list): Sort-field letter codes; each must be a key in hash.
         hash (dict): Field-code → (label, full_field_name, …) mapping.
         patterns (list | None): Optional temporal format patterns, one per sort field.

      Returns:
         dict: New column-oriented dict with rows in sorted order.
      """
      fcnt = len(flds)    # count of fields to be sorted on
      # set sorting order, descenting (-1) or ascenting (1)
      # get the full field names to be sorted on
      desc = [1]*fcnt
      fields = []
      nums = [1]*fcnt   # initialize each column as numerical
      for i in range(fcnt):
         if flds[i].islower(): desc[i] = -1
         fld = self.strip_field(hash[flds[i].upper()][1])
         fields.append(fld)
      count = len(pgrecs[fields[0]])    # row count of pgrecs
      if count < 2: return pgrecs       # no need of sording
      pcnt = len(patterns) if patterns else 0
      # prepare the dict list for sortting
      srecs = []
      for i in range(count):
         pgrec = self.onerecord(pgrecs, i)
         rec = []
         for j in range(fcnt):
            if j < pcnt and patterns[j]:
               # get the temporal part of each value matching the pattern
               val = self.format_date(pgrec[fields[j]], "YYYYMMDDHH", patterns[j])
            else:
               # sort on the whole value if no pattern given
               val = pgrec[fields[j]]
            if nums[j]: nums[j] = self.pgnum(val)
            rec.append(val)
         rec.append(i)   # add column to cache the row index
         srecs.append(rec)
      srecs = self.quicksort(srecs, 0, count-1, desc, fcnt, nums)
      # sort pgrecs according the cached row index column in ordered srecs
      rets = {}
      for fld in pgrecs:
         rets[fld] = []
      for i in range(count):
         pgrec = self.onerecord(pgrecs, srecs[i][fcnt])
         for fld in pgrecs:
            rets[fld].append(pgrec[fld])
      return rets

   # Return: the number of days bewteen date1 and date2
   @staticmethod
   def diffdate(date1, date2):
      """Return the signed number of days between two date strings (date1 − date2).

      Args:
         date1 (str | None): Later date in 'YYYY-MM-DD' format.
         date2 (str | None): Earlier date in 'YYYY-MM-DD' format.

      Returns:
         int: Positive when date1 > date2, negative when date1 < date2.
      """
      ut1 = ut2 = 0
      if date1: ut1 = PgUtil.unixtime(date1)
      if date2: ut2 = PgUtil.unixtime(date2)
      return round((ut1 - ut2)/86400)   # 24*60*60

   # Return: the number of seconds bewteen time1 and time2
   @staticmethod
   def difftime(time1, time2):
      """Return the signed number of seconds between two datetime strings (time1 − time2).

      Args:
         time1 (str | None): Later datetime string.
         time2 (str | None): Earlier datetime string.

      Returns:
         int: Signed second difference.
      """
      ut1 = ut2 = 0
      if time1: ut1 = PgUtil.unixtime(time1)
      if time2: ut2 = PgUtil.unixtime(time2)
      return round(ut1 - ut2)
   diffdatetime = difftime

   # Return: the number of days between date and '1970-01-01 00:00:00'
   @staticmethod
   def get_days(cdate):
      """Return the number of days elapsed since the Unix epoch (1970-01-01).

      Args:
         cdate (str | any): Date value; coerced to str when not already.

      Returns:
         int: Day count since 1970-01-01.
      """
      return PgUtil.diffdate(str(cdate), '1970-01-01')

   # Function: get_month_days(date)
   #   Return: the number of days in given month
   @staticmethod
   def get_month_days(cdate):
      """Return the number of days in the month of a given date.

      Args:
         cdate (str | any): Date value in 'YYYY-MM-…' format.

      Returns:
         int: Days in the month (28-31), or 0 when the date cannot be parsed.
      """
      ms = re.match(r'^(\d+)-(\d+)', str(cdate))
      if ms:
         yr = int(ms.group(1))
         mn = int(ms.group(2))
         return calendar.monthrange(yr, mn)[1]
      else:
         return 0

   # Function: validate_date(date)
   #   Return: a date in format of YYYY-MM-DD thar all year/month/day are validated
   @staticmethod
   def validate_date(cdate):
      """Clamp year, month, and day components of a date to valid ranges.

      Years below 1000 are assumed to be in the 2000s; years above 9999 are taken
      mod 10000. Month and day are clamped to [1, 12] and [1, last_day_of_month].

      Args:
         cdate (str | any): Date string in 'YYYY-MM-DD' format.

      Returns:
         str: Validated date string in 'YYYY-MM-DD' format, or cdate unchanged
              when it cannot be parsed.
      """
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

   # Function: get_date(days)
   #   Return: the date in format of "YYYY-MM-DD" for given number of days
   #   from '1970-01-01 00:00:00'
   def get_date(self, days):
      """Return the date that is a given number of days after the Unix epoch.

      Args:
         days (int | str): Number of days since 1970-01-01.

      Returns:
         str: Date string in 'YYYY-MM-DD' format.
      """
      return self.adddate('1970-01-01', 0, 0, int(days))

   # compare date/hour and return the different hours
   @staticmethod
   def diffdatehour(date1, hour1, date2, hour2):
      """Return the signed hour difference between two date+hour pairs.

      Missing hour values default to 23 (end-of-day).

      Args:
         date1 (str): Date string for the first point.
         hour1 (int | None): Hour (0-23) for the first point; defaults to 23.
         date2 (str): Date string for the second point.
         hour2 (int | None): Hour for the second point; defaults to 23.

      Returns:
         int: (hour1 − hour2) + 24 × (date1 − date2 in days).
      """
      if hour1 is None: hour1 = 23
      if hour2 is None: hour2 = 23
      return (hour1 - hour2) + 24*PgUtil.diffdate(date1, date2)

   # hour difference between GMT and local time
   def diffgmthour(self):
      """Return the hour difference between GMT and local time.

      Returns:
         int: Local_hour − GMT_hour (positive east of UTC, negative west).
      """
      tg = time.gmtime()
      tl = time.localtime()
      dg = self.fmtdate(tg[0], tg[1], tg[2])
      dl = self.fmtdate(tl[0], tl[1], tl[2])
      hg = tg[3]
      hl = tl[3]
      return self.diffdatehour(dg, hg, dl, hl)

   # compare date and time (if given) and return 1, 0 and -1
   @staticmethod
   def cmptime(date1, time1, date2, time2):
      """Compare two date+time pairs and return a three-way comparison result.

      Args:
         date1 (str): First date string.
         time1 (str | None): First time string; defaults to '00:00:00' when None.
         date2 (str): Second date string.
         time2 (str | None): Second time string.

      Returns:
         int: 1 if first > second, -1 if first < second, 0 if equal.
      """
      stime1 = PgUtil.join_datetime(date1, time1)
      stime2 = PgUtil.join_datetime(date2, time2)
      return PgUtil.pgcmp(stime1, stime2)

   #   date: the original date in format of 'YYYY-MM-DD',
   #     mf: the number of month fractions to add
   #     nf: number of fractions of a month
   # Return: new date
   def addmonth(self, cdate, mf, nf = 1):
      """Add a fractional number of months to a date.

      When nf < 2, delegates to adddate(). Otherwise uses 30-day month fractions
      to compute the new date.

      Args:
         cdate (str): Starting date in 'YYYY-MM-DD' format.
         mf (int): Number of month fractions to add (negative to subtract).
         nf (int): Number of fractions per month (1 = whole months, 2 = half, etc.).

      Returns:
         str: Resulting date in 'YYYY-MM-DD' format.
      """
      if not mf: return cdate
      if not nf or nf < 2: return self.adddate(cdate, 0, mf, 0)
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
         cdate = self.fmtdate(yr, mn, dy)
      return cdate
   
   # add yr years & mn months to yearmonth ym in format YYYYMM
   @staticmethod
   def addyearmonth(ym, yr, mn):
      """Add years and months to a compact year-month string (YYYYMM).

      Args:
         ym (str): Base year-month in 'YYYYMM' format.
         yr (int | None): Years to add; treated as 0 when None.
         mn (int | None): Months to add; treated as 0 when None.

      Returns:
         str: Resulting year-month in 'YYYYMM' format, or ym unchanged when it
              cannot be parsed.
      """
      if yr is None: yr = 0
      if mn is None: mn = 0
      ms =re.match(r'^(\d\d\d\d)(\d\d)$', ym)
      if ms:
         (syr, smn) = ms.groups()
         nyr = int(syr) + yr
         nmn = int(smn) + mn
         if nmn < 0:
            while nmn < 0:
               nyr -= 1
               nmn += 12
         else:
            while nmn > 12:
               nyr += 1
               nmn -= 12
         ym = "{:04}{:02}".format(nyr, nmn)
      return ym

   # set number of days in Beburary for Leap year according PGLOG['NOLEAP']
   def set_leap_mdays(self, year):
      """Update MDAYS[0] (year length) and MDAYS[2] (February) for a given year.

      Honours PGLOG['NOLEAP']: when set, February always has 28 days.

      Args:
         year (int): Year to evaluate for leap-year status.

      Returns:
         int: 1 if year is a leap year (and NOLEAP is not set), 0 otherwise.
      """
      if not self.PGLOG['NOLEAP'] and calendar.isleap(year):
         self.MDAYS[0] = 366
         self.MDAYS[2] = 29
         ret = 1
      else:
         self.MDAYS[0] = 365
         self.MDAYS[2] = 28
         ret = 0
      return ret

   # wrap on calendar.isleap()
   is_leapyear = calendar.isleap

   # reutn 1 if is end of month
   def is_end_month(self, yr, mn, dy):
      """Return 1 if the given day is the last day of its month, 0 otherwise.

      Args:
         yr (int): Year.
         mn (int): Month (1-12).
         dy (int): Day of month.

      Returns:
         int: 1 when dy equals the last day of month mn in year yr, else 0.
      """
      self.set_leap_mdays(yr)
      return 1 if dy == self.MDAYS[mn] else 0

   # adust the year, month and day values that are out of ranges
   def adjust_ymd(self, yr, mn, dy):
      """Normalise year, month, and day values that are out of calendar range.

      Carries months into/from years and days into/from months iteratively until
      all three components are within valid ranges. Updates MDAYS for leap years.

      Args:
         yr (int | None): Year component; defaults to 1970 when None.
         mn (int | None): Month component (1-12); defaults to 1 when None.
         dy (int | None): Day component; defaults to 1 when None.

      Returns:
         list: [yr, mn, dy] all within valid calendar ranges.
      """
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
         self.set_leap_mdays(yr)
         if dy < 1:
            if(dy < -self.MDAYS[0]):
               yr -= 1
               dy += self.MDAYS[0]
            else:
               mn -= 1
               if mn < 1:
                 yr -= 1
                 mn += 12
               dy += self.MDAYS[mn]
            continue
         elif dy > self.MDAYS[mn]:
            if(dy > self.MDAYS[0]):
               dy -= self.MDAYS[0]
               yr += 1
            else:
               dy -= self.MDAYS[mn]
               mn += 1
            continue
         break
      return [yr, mn, dy]

   #   date: the original date in format of 'YYYY-MM-DD',
   #     yr: the number of years to add/subtract from the odate for positive/negative value,
   #     mn: the number of months to add/subtract from the odate for positive/negative value,
   #     dy: the number of days to add/subtract from the odate for positive/negative value)
   # Return: new date
   def adddate(self, cdate, yr, mn = 0, dy = 0, tofmt = None):
      """Add years, months, and/or days to a date string.

      Handles month-end preservation: when the starting day is the last day of its
      month and months are being added, the result lands on the last day of the
      target month.

      Args:
         cdate (str | any): Starting date in 'YYYY-MM-DD' format.
         yr (int | str | None): Years to add (negative to subtract).
         mn (int | str | None): Months to add; default 0.
         dy (int | str | None): Days to add; default 0.
         tofmt (str | None): Output date format; defaults to 'YYYY-MM-DD'.

      Returns:
         str: Resulting date string, or cdate unchanged when it cannot be parsed.
      """
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
      if mn and ndy > 27: mend = self.is_end_month(nyr, nmn, ndy)
      if yr: nyr += yr
      if mn:
         (nyr, nmn, tdy) = self.adjust_ymd(nyr, nmn+mn+1, 0)
         if mend: ndy = tdy
      if dy: ndy += dy
      return self.fmtdate(nyr, nmn, ndy, tofmt)
   addNoLeapDate = adddate

   # add given hours to the initial date and time
   def addhour(self, sdate, stime, nhour):
      """Add a number of hours to a date+time pair, adjusting the date when needed.

      Args:
         sdate (str | any | None): Starting date string.
         stime (str | any | None): Starting time string in 'HH:…' format.
         nhour (int | str): Hours to add (may be negative).

      Returns:
         list: [new_date_str, new_time_str].
      """
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
      if dy: sdate = self.adddate(sdate, 0, 0, dy)
      return [sdate, stime]

   # add given years, months, days and hours to the initial date and hour
   def adddatehour(self, sdate, nhour, yr, mn, dy, hr = 0):
      """Add years, months, days, and hours to a date+hour pair.

      The hour increment hr is combined with nhour, then overflow/underflow is
      carried into dy before calling adddate().

      Args:
         sdate (str | any | None): Starting date string.
         nhour (int | str | None): Starting hour value.
         yr (int): Years to add.
         mn (int): Months to add.
         dy (int): Days to add.
         hr (int): Hours to add; default 0.

      Returns:
         list: [new_date_str, new_hour_int].
      """
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
      if yr or mn or dy: sdate = self.adddate(sdate, yr, mn, dy)
      return [sdate, nhour]

   # add given yyyy, mm, dd, hh, nn, ss to sdatetime
   # if nf, add fraction of month only
   def adddatetime(self, sdatetime, yy, mm, dd, hh, nn, ss, nf = 0):
      """Add year/month/day/hour/minute/second offsets to a datetime string.

      When nf > 1, the month increment is applied as fractional months via
      addmonth() before any remaining date arithmetic.

      Args:
         sdatetime (str | any): Starting datetime in 'YYYY-MM-DD HH:MM:SS' format.
         yy (int): Years to add.
         mm (int): Months to add (or fractional months when nf > 1).
         dd (int): Days to add.
         hh (int): Hours to add.
         nn (int): Minutes to add.
         ss (int): Seconds to add.
         nf (int): Month fraction denominator; 0 or 1 = whole months.

      Returns:
         str: Resulting datetime string in 'YYYY-MM-DD HH:MM:SS' format.
      """
      if sdatetime and not isinstance(sdatetime, str): sdatetime = str(sdatetime)
      (sdate, stime) = re.split(' ', sdatetime)
      if hh or nn or ss: (sdate, stime) = self.addtime(sdate, stime, hh, nn, ss)
      if nf:
         sdate = self.addmonth(sdate, mm, nf)
         mm = 0
      if yy or mm or dd: sdate = self.adddate(sdate, yy, mm, dd)
      return "{} {}".format(sdate, stime)

   # add given hours, minutes and seconds to the initial date and time
   def addtime(self, sdate, stime, h, m, s):
      """Add hour, minute, and second offsets to a date+time pair.

      Normalises overflow/underflow across seconds → minutes → hours → days.

      Args:
         sdate (str | any | None): Starting date string.
         stime (str | any | None): Starting time in 'HH:MM:SS' format.
         h (int): Hours to add.
         m (int): Minutes to add.
         s (int): Seconds to add.

      Returns:
         list: [new_date_str, new_time_str] in 'YYYY-MM-DD' and 'HH:MM:SS' format.
      """
      if sdate and not isinstance(sdate, str): sdate = str(sdate)
      if stime and not isinstance(stime, str): stime = str(stime)
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
      if tms[3]: sdate = self.adddate(sdate, 0, 0, tms[3])
      return [sdate, stime]

   # add time interval array to datetime
   # opt = -1 - minus, 0 - begin time, 1 - add (default)
   def addintervals(self, sdatetime, intv, opt = 1):
      """Apply a time-interval array to a datetime string.

      Args:
         sdatetime (str | any): Starting datetime in 'YYYY-MM-DD HH:MM:SS' format.
         intv (list | None): Interval values [yy, mm, dd, hh, nn, ss, nf]; missing
                             positions default to 0.
         opt (int): 1 = add (default), -1 = subtract, 0 = advance one second first
                    (to move from end of current period to start of next).

      Returns:
         str: Resulting datetime string.
      """
      if not isinstance(sdatetime, str): sdatetime = str(sdatetime)
      if not intv: return sdatetime
      tv = [0]*7
      i = 0
      for v in intv:
         tv[i] = v
         i += 1
      # assume the given datetime is end of the current interval;
      # add one second to set it to beginning of the next one
      if opt == 0: sdatetime = self.adddatetime(sdatetime, 0, 0, 0 ,0, 0, 1)
      if opt < 1: # negative intervals for minus
         for i in range(6):
            if tv[i]: tv[i] = -tv[i]
      return self.adddatetime(sdatetime, tv[0], tv[1], tv[2], tv[3], tv[4], tv[5], tv[6])

   # adjust end date to the specified day days for frequency of year/month/week
   # end of period if days == 0
   # nf - number of fractions of a month, for unit of 'M' only
   def enddate(self, sdate, days, unit, nf = 0):
      """Adjust a date to the end (or a specified day) of its year/month/week period.

      Args:
         sdate (str | any | None): Input date string.
         days (int | str | None): Target day within the period; 0 = last day of period.
         unit (str): Period unit — 'Y' (year), 'M' (month), or 'W' (week).
         nf (int): Month fraction denominator for unit='M'; 0 or 1 = whole months.

      Returns:
         str: Adjusted date string in 'YYYY-MM-DD' format.
      """
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
            sdate = self.fmtdate(yr, mn, dy)
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
            if nd != dy: sdate = self.fmtdate(yr, mn, nd)
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
            sdate = self.fmtdate(yr, mn, dy)
      elif unit == 'W':
         val = self.get_weekday(sdate)
         if days != val: sdate = self.adddate(sdate, 0, 0, days-val)
      return sdate

   # adjust end time to the specified h/n/s for frequency of hour/mimute/second
   def endtime(self, stime, unit):
      """Adjust a time string to the end of its hour, minute, or second period.

      Args:
         stime (str | any | None): Input time string; defaults to '00:00:00' when falsy.
         unit (str): Period unit — 'H' (hour), 'N' (minute), or 'S' (second, no-op).

      Returns:
         str: Adjusted time string in 'HH:MM:SS' format.
      """
      if stime and not isinstance(stime, str): stime = str(stime)
      if not (unit and unit in 'HNS'): return stime  
      if stime:
         tm = self.split_datetime(stime, 'T')
      else:
         tm = [0, 0, 0]
      if unit == 'H':
         tm[1] = tm[2] = 59
      elif unit == 'N':
         tm[2] = 59
      elif unit != 'S':
         tm[0] = 23
         tm[1] = tm[2] = 59
      return "{:02}:{:02}:{:02}".format(tm[0], tm[1], tm[2])

   # adjust end time to the specified h/n/s for frequency of year/month/week/day/hour/mimute/second
   def enddatetime(self, sdatetime, unit, days = 0, nf = 0):
      """Adjust a datetime string to the end of the given calendar/time period.

      Delegates to enddate() for date units (Y/M/W) and endtime() for time units (H/N/S).

      Args:
         sdatetime (str | any | None): Input datetime in 'YYYY-MM-DD HH:MM:SS' format.
         unit (str): Period unit — Y, M, W, D, H, N, or S.
         days (int): Target day for date period adjustment; 0 = last day.
         nf (int): Month fraction denominator; 0 or 1 = whole months.

      Returns:
         str: Adjusted datetime string in 'YYYY-MM-DD HH:MM:SS' format.
      """
      if sdatetime and not isinstance(sdatetime, str): sdatetime = str(sdatetime)
      if not (unit and unit in 'YMWDHNS'): return sdatetime
      (sdate, stime) = re.split(' ', sdatetime)
      if unit in 'HNS':
         stime = self.endtime(stime, unit)
      else:
         sdate = self.enddate(sdate, days, unit, nf)
      return "{} {}".format(sdate, stime)

   # get the string length dynamically
   @staticmethod
   def get_column_length(colname, values):
      """Return the display width needed for a column, based on its values.

      Starts from the length of the column title (or 2 when colname is None) and
      expands to accommodate the longest non-newline value string.

      Args:
         colname (str | None): Column header label.
         values (iterable): Column values to measure.

      Returns:
         int: Maximum display width for the column.
      """
      clen = len(colname) if colname else 2  # initial column length as the length of column title
      for val in values:
         if val is None: continue
         sval = str(val)
         if sval and not re.search(r'\n', sval):
            slen = len(sval)
            if slen > clen: clen = slen
      return clen

   # Function: hour2time()
   #   Return: time string in format of date HH:MM:SS
   @staticmethod
   def hour2time(sdate, nhour, endtime = 0):
      """Build a time string (and optional datetime string) from a date and hour.

      Args:
         sdate (str | any | None): Date portion; when truthy, prepended to the time.
         nhour (int): Hour of day (0-23).
         endtime (int): When non-zero, sets minutes and seconds to 59; else 00.

      Returns:
         str: 'YYYY-MM-DD HH:MM:SS' when sdate is given, or 'HH:MM:SS' otherwise.
      """
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

   # Function: time2hour()
   #   Return: list of date and hour
   @staticmethod
   def time2hour(stime):
      """Split a datetime or time string into a [date, hour] pair.

      Args:
         stime (str): Time or datetime string; 'YYYY-MM-DD HH:…' or 'HH:…'.

      Returns:
         list: [date_str_or_None, hour_int_or_None].
      """
      sdate = nhour = None
      times = stime.split(' ')
      if len(times) == 2:
         sdate = times[0]
         stime = times[1]
      ms = re.match(r'^(\d+)', stime)
      if ms: nhour = int(ms.group(1))
      return [sdate, nhour]

   # get the all column widths
   @staticmethod
   def all_column_widths(pgrecs, flds, tdict):
      """Return display widths for a list of field code columns in a result dict.

      Args:
         pgrecs (dict): Column-oriented dict from pgmget().
         flds (list): Ordered field code letters matching keys in tdict.
         tdict (dict): Field-code → (label, full_field_name, …) mapping.

      Returns:
         list[int]: Display width for each field in flds (0 when not in tdict).
      """
      colcnt = len(flds)
      lens = [0]*colcnt
      for i in range(colcnt):
         fld = flds[i]
         if fld not in tdict: continue
         field = PgUtil.strip_field(tdict[fld][1])
         lens[i] = PgUtil.get_column_length(None, pgrecs[field])
      return lens

   # check a give value, return 1 if numeric, 0 therwise
   @staticmethod
   def pgnum(val):
      """Return 1 when val is a valid numeric string, 0 otherwise.

      Recognises integers, decimals, and scientific notation.

      Args:
         val: Value to test; coerced to str when not already.

      Returns:
         int: 1 if numeric, 0 otherwise.
      """
      if not isinstance(val, str): val = str(val)
      ms = re.match(r'^\-{0,1}(\d+|\d+\.\d*|d*\.\d+)([eE]\-{0,1}\d+)*$', val)
      return 1 if ms else 0

   # Function: pgcmp(val1, val2)
   #   Return: 0 if both empty or two values are identilcal; -1 if val1 < val2; otherwise 1
   @staticmethod
   def pgcmp(val1, val2, ignorecase = 0, num = 0):
      """Three-way comparison of two values with optional type normalisation.

      None is considered less than any non-None value. Mismatched types are coerced
      to str (default) or int (when num is set). String comparison can be
      case-insensitive.

      Args:
         val1: First value.
         val2: Second value.
         ignorecase (int): When non-zero, lowercases strings before comparing.
         num (int): When non-zero, coerces strings to int for numeric comparison.

      Returns:
         int: 1 if val1 > val2, -1 if val1 < val2, 0 if equal.
      """
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
            if typ1 is str and PgUtil.pgnum(val1) and PgUtil.pgnum(val2):
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

   # infiles: initial file list
   #  Return: final file list with all the subdirectories expanded
   @staticmethod
   def recursive_files(infiles):
      """Expand a list of file paths, recursively replacing directories with their contents.

      Args:
         infiles (list[str]): Input file/directory paths.

      Returns:
         list[str]: Flat list of file paths with all directories expanded.
      """
      ofiles = []
      for file in infiles:
         if op.isdir(file):
            ofiles.extend(PgUtil.recursive_files(glob.glob(file + "/*")))
         else:
            ofiles.append(file)
      return ofiles

   #   lidx: lower index limit  (including)
   #   hidx: higher index limit (excluding)
   #    key: string value to be searched,
   #   list: reference to a sorted list where the key is searched)
   # Return: index if found; -1 otherwise
   @staticmethod
   def asearch(lidx, hidx, key, list):
      """Binary (or linear) search for an exact key in a sorted list.

      Uses linear search for ranges ≤ 10 elements, binary search otherwise.

      Args:
         lidx (int): Inclusive lower index.
         hidx (int): Exclusive upper index.
         key: Value to search for.
         list (list): Sorted list to search within.

      Returns:
         int: Index of the matching element, or -1 when not found.
      """
      ret = -1
      if (hidx - lidx) < 11:   # use linear search for less than 11 items
         for midx in range(lidx, hidx):
            if key == list[midx]:
               ret = midx
               break
      else:
         midx = (lidx + hidx) // 2
         if key == list[midx]:
            ret = midx
         elif key < list[midx]:
            ret = PgUtil.asearch(lidx, midx, key, list)
         else:
            ret = PgUtil.asearch(midx + 1, hidx, key, list)
      return ret

   #   lidx: lower index limit  (including)
   #   hidx: higher index limit (excluding)
   #    key: string value to be searched,
   #   list: reference to a sorted list where the key is searched)
   # Return: index if found; -1 otherwise
   @staticmethod
   def psearch(lidx, hidx, key, list):
      """Binary (or linear) search matching key against regex patterns in a sorted list.

      Uses linear search for ranges ≤ 10 elements, binary search otherwise.
      Comparisons use re.search(list[midx], key) for matching.

      Args:
         lidx (int): Inclusive lower index.
         hidx (int): Exclusive upper index.
         key (str): Value to match against patterns.
         list (list[str]): Sorted list of regex patterns.

      Returns:
         int: Index of the first matching pattern, or -1 when none match.
      """
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
            ret = PgUtil.psearch(lidx, midx, key, list)
         else:
            ret = PgUtil.psearch(midx + 1, hidx, key, list)
      return ret

   # quicksort for pattern
   @staticmethod
   def quicksort(srecs, lo, hi, desc, cnt, nums = None):
      """In-place quicksort for a list of record lists.

      Uses the middle element as pivot and recursively sorts sub-ranges.

      Args:
         srecs (list): List of row lists; each row ends with a cached original index.
         lo (int): Inclusive lower bound.
         hi (int): Inclusive upper bound.
         desc (list[int]): Per-column sort direction: 1 = ascending, -1 = descending.
         cnt (int): Number of sort-key columns (not counting the index column).
         nums (list[int] | None): Per-column numeric flag; 1 = numeric comparison.

      Returns:
         list: The sorted srecs list.
      """
      i = lo
      j = hi
      mrec = srecs[int((lo+hi)/2)]
      while True:
         while PgUtil.cmp_records(srecs[i], mrec, desc, cnt, nums) < 0: i += 1
         while PgUtil.cmp_records(srecs[j], mrec, desc, cnt, nums) > 0: j -= 1
         if i <= j:
            if i < j:
               tmp = srecs[i]
               srecs[i] = srecs[j]
               srecs[j] = tmp
            i += 1
            j -= 1
         if i > j: break   
      #recursion
      if lo < j: srecs = PgUtil.quicksort(srecs, lo, j, desc, cnt, nums)
      if i < hi: srecs = PgUtil.quicksort(srecs, i, hi, desc, cnt, nums)
      return srecs

   # compare two arrays
   @staticmethod
   def cmp_records(arec, brec, desc, cnt, nums):
      """Compare two record lists on the first cnt columns using pgcmp().

      Args:
         arec (list): First record list.
         brec (list): Second record list.
         desc (list[int]): Per-column direction multipliers (1 or -1).
         cnt (int): Number of columns to compare.
         nums (list[int] | None): Per-column numeric flag.

      Returns:
         int: Negative, zero, or positive comparison result.
      """
      for i in range(cnt):
         num = nums[i] if nums else 0
         ret = PgUtil.pgcmp(arec[i], brec[i], 0, num)
         if ret != 0:
            return (ret*desc[i])   
      return 0   # identical records

   # format one floating point value
   @staticmethod
   def format_float_value(val, precision = 2):
      """Format a byte count as a human-readable string with unit suffix.

      Scales through B, KB, MB, GB, TB, PB with the specified decimal precision.

      Args:
         val (int | float | None): Byte count; returns '' when None.
         precision (int): Decimal places in the formatted number; default 2.

      Returns:
         str: Formatted string like '1.23GB', or '' when val is None.
      """
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

   # check a file is a ASCII text one
   # return 1 if yes, 0 if not; or -1 if file not checkable
   @staticmethod
   def is_text_file(fname, blocksize = 256, threshhold = 0.1):
      """Determine whether a file is an ASCII text file by sampling its content.

      Reads up to blocksize bytes and rejects the file if it contains null bytes or
      if the proportion of non-printable-ASCII characters exceeds threshhold.

      Args:
         fname (str): Path to the file to inspect.
         blocksize (int): Number of bytes to sample; default 256.
         threshhold (float): Maximum allowed fraction of non-text bytes; default 0.1.

      Returns:
         int: 1 = text, 0 = binary, -1 = file does not exist or is not a regular file.
      """
      # File doesn't exist or is not a regular file
      if not op.exists(fname) or not op.isfile(fname): return -1
      if op.getsize(fname) == 0: return 1  # Empty files are considered text
      try:
         buffer = None
         with open(fname, 'rb') as f:
            buffer = f.read(blocksize)
         # Check for null bytes (a strong indicator of a binary file)
         if not buffer or b'\0' in buffer: return 0
         text_characters = (
            b'\t\n\r\f\v' +        # Whitespace characters
            bytes(range(32, 127))  # Printable ASCII characters
         )
         non_text_count = 0
         for byte in buffer:
            if byte not in text_characters:
               non_text_count += 1  # Count non-text characters
         # If a significant portion of the buffer consists of non-text characters,
         # it's likely a binary file.
         return 1 if((non_text_count/len(buffer)) < threshhold) else 0
      except IOError:
         return -1   # Handle cases where the file cannot be opened or read
