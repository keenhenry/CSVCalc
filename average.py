#!/usr/bin/python
#

from __future__ import with_statement
from google.appengine.api import files
from google.appengine.ext import blobstore
import csv

class CSVCalc(object):
  '''
  '''
  
  def __init__(self, blob_key=None):
    '''
    '''
    self.src = blob_key
    self.month_rpt = None
    self.compy_rpt = None
    self.blob_reader = None
    self.reader = None
    self.rows_month = ''
    self.rows_compy = ''

    # input file parsing control variables
    self.old_gvkey, self.old_conm, self.old_Month = None, None, None
    self.m_cnt, self.c_cnt, self.m_total, self.c_total = 0, 0, 0, 0

  def prepare_reader(self):
    ""

    if self.src:
      self.blob_reader = blobstore.BlobReader(self.src)
      self.reader = csv.DictReader(self.blob_reader)

  def _init_frome_first_row(self, o_gvkey, o_month, o_conm, o_illiq):
    ""
    self.old_gvkey = o_gvkey
    self.old_Month = o_month
    self.old_conm = o_conm
    self.m_cnt = 1
    self.m_total = 0 if o_illiq=='' or o_illiq=='#DIV/0!' else float(o_illiq)

  def _print_to_month_rpt(self):
    ""
    #self.writer_m.writerow([int(self.old_gvkey), self.old_conm, int(self.old_Month), self.m_total/self.m_cnt])
    self.rows_month += self.old_gvkey + ',\"' + \
    	   	       self.old_conm + '\",' + \
	   	       self.old_Month + ',' + \
	   	       str(self.m_total/self.m_cnt) + '\r\n'

  def _print_to_compy_rpt(self):
    ""
    #self.writer_c.writerow([int(self.old_gvkey), self.old_conm, self.c_total/self.c_cnt])
    self.rows_compy += self.old_gvkey + ',\"' + \
    		       self.old_conm + '\",' + \
	   	       str(self.c_total/self.c_cnt) + '\r\n'

  def _update_flags_within_month(self, row):
    self.m_cnt += 1
    self.m_total += 0 if row['ILLIQ']=='' or row['ILLIQ']=='#DIV/0!' else float(row['ILLIQ'])

  def _update_flags_after_month(self, row):
    self.old_Month = row['Month']
    self.m_cnt = 1
    self.m_total = 0 if row['ILLIQ']=='' or row['ILLIQ']=='#DIV/0!' else float(row['ILLIQ'])
  
  def _update_flags_after_compy(self, row):
    self.old_gvkey = row['gvkey']
    self.old_Month = row['Month']
    self.old_conm = row['conm']

    self.m_cnt, self.c_cnt, self.c_total = 1, 0, 0
    self.m_total = 0 if row['ILLIQ']=='' or row['ILLIQ']=='#DIV/0!' else float(row['ILLIQ'])

  def _increment_company_flags(self):
    self.c_cnt += self.m_cnt
    self.c_total += self.m_total

  def _print_last_rows(self):
    ""
    self._increment_company_flags()

    # print the last rows to two reports
    self._print_to_month_rpt()
    self._print_to_compy_rpt()

  def close_files(self):
    self.blob_reader.close()
    files.finalize(self.month_rpt)
    files.finalize(self.compy_rpt)
  
  def parse(self):
    ""
    
    try:
      # write the header rows into target report files first
      #self.writer_m.writerow([ 'gvkey', 'conm', 'Month', 'ILLIQ_average'])
      #self.writer_c.writerow([ 'gvkey', 'conm', 'ILLIQ_average'])
      self.rows_month += '\"gvkey\",'+'\"conm\",'+'\"Month\",'+'\"ILLIQ_average\"\r\n'
      self.rows_compy += '\"gvkey\",'+'\"conm\",'+'\"ILLIQ_average\"\r\n'

      # produce the rest of the reports
      for row in self.reader:
	if not (self.old_gvkey or self.old_Month): 
  	  self._init_frome_first_row(row['gvkey'], row['Month'], row['conm'], row['ILLIQ'])
	  continue
	    
	# gvkey not changed - still in the records belong to the same company
	if self.old_gvkey==row['gvkey']:
	  if self.old_Month==row['Month']:
  	    self._update_flags_within_month(row)
	  else:
  	    self._increment_company_flags()
	    self._print_to_month_rpt()
	    self._update_flags_after_month(row)
	# gvkey changed - data for a new company is fetched
	else:
  	  self._increment_company_flags()
	  self._print_to_month_rpt()
	  self._print_to_compy_rpt()
	  self._update_flags_after_compy(row)
      self._print_last_rows()
    finally:
      # making up reports' file names
      infile_blob_info = self.blob_reader.blob_info
      month_rpt_filename = 'rpt-month-' + infile_blob_info.filename
      compy_rpt_filename = 'rpt-compy-' + infile_blob_info.filename

      # creating report blobs in blobstore
      self.month_rpt = files.blobstore.create(mime_type='application/octet-stream', _blobinfo_uploaded_filename=month_rpt_filename)
      self.compy_rpt = files.blobstore.create(mime_type='application/octet-stream', _blobinfo_uploaded_filename=compy_rpt_filename)

      # writing data (in csv format) into blobs
      with files.open(self.month_rpt, 'a') as f:
        f.write(self.rows_month)
      
      with files.open(self.compy_rpt, 'a') as f:
        f.write(self.rows_compy)
