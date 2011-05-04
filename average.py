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
    #self.tgt_m = None
    #self.tgt_c = None
    self.reader = None
    #self.writer_m = None
    #self.writer_c = None
    self.rows_month = ''
    self.rows_compy = ''

    self.old_gvkey, self.old_conm, self.old_Month = None, None, None
    self.m_cnt, self.c_cnt, self.m_total, self.c_total = 0, 0, 0, 0

  def prepare_reader(self):
    ""

    #if not self.src: self.src = open('data-mini.csv','rb')
    blob_reader = blobstore.BlobReader(self.src)
    self.reader = csv.DictReader(blob_reader)

  def prepare_writers(self):
    ""
    return
    # create new blobstore files for writing
    #self.month_rpt = files.blobstore.create(mime_type='application/octet-stream')
    #self.compy_rpt = files.blobstore.create(mime_type='application/octet-stream')
    #self.tgt_m = files.open(self.month_rpt,'a')
    #self.tgt_c = files.open(self.compy_rpt,'a')

    #self.writer_m = csv.writer(self.tgt_m, quoting=csv.QUOTE_NONNUMERIC)
    #self.writer_c = csv.writer(self.tgt_c, quoting=csv.QUOTE_NONNUMERIC)

  def get_rpt_blobkeys(self):
    rpt_month_blobkey = files.blobstore.get_blob_key(self.month_rpt)
    rpt_compy_blobkey = files.blobstore.get_blob_key(self.compy_rpt)
    return (rpt_month_blobkey, rpt_compy_blobkey)

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
    files.finalize(self.month_rpt)
    files.finalize(self.compy_rpt)
    #self.tgt_c.close()
    #self.tgt_m.close()
    #self.src.close()
  
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
      self.month_rpt = files.blobstore.create(mime_type='application/octet-stream')
      self.compy_rpt = files.blobstore.create(mime_type='application/octet-stream')

      with files.open(self.month_rpt, 'a') as f:
      	#self.tgt_m = files.open(self.month_rpt,'a')
      	#self.tgt_c = files.open(self.compy_rpt,'a')
        f.write(self.rows_month)
      
      with files.open(self.compy_rpt, 'a') as f:
      	#self.tgt_m = files.open(self.month_rpt,'a')
      	#self.tgt_c = files.open(self.compy_rpt,'a')
        f.write(self.rows_compy)
      #self.tgt_c.write(self.rows_compy)
      #self._close_files()

def main():

    parser = CSVCalc(open('data-mini.csv', 'rb'))

    parser.prepare_reader()
    parser.prepare_writers()
    parser.parse()

if __name__ == '__main__':
  main()
