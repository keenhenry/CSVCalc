#!/usr/bin/python
#

import csv

class CSVCalc(object):
  '''
  '''
  
  def __init__(self, src_file=None):
    '''
    '''
    self.src = src_file
    self.tgt_m = None
    self.tgt_c = None
    self.reader = None
    self.writer_m = None
    self.writer_c = None

    self.old_gvkey, self.old_conm, self.old_Month = None, None, None
    self.m_cnt, self.c_cnt, self.m_total, self.c_total = 0, 0, 0, 0

  def prepare_reader(self):
    ""

    if not self.src: self.src = open('data-mini.csv','rb')
    self.reader = csv.DictReader(self.src)

  def prepare_writers(self):
    ""
    self.tgt_m = open('reports/rpt-month.csv','wb')
    self.tgt_c = open('reports/rpt-company.csv','wb')

    self.writer_m = csv.writer(self.tgt_m, quoting=csv.QUOTE_NONNUMERIC)
    self.writer_c = csv.writer(self.tgt_c, quoting=csv.QUOTE_NONNUMERIC)

  def _init_frome_first_row(self, o_gvkey, o_month, o_conm, o_illiq):
    ""
    self.old_gvkey = o_gvkey
    self.old_Month = o_month
    self.old_conm = o_conm
    self.m_cnt = 1
    self.m_total = 0 if o_illiq=='' or o_illiq=='#DIV/0!' else float(o_illiq)

  def _print_to_month_rpt(self):
    ""
    self.writer_m.writerow([int(self.old_gvkey), self.old_conm, int(self.old_Month), self.m_total/self.m_cnt])


  def _print_to_compy_rpt(self):
    ""
    self.writer_c.writerow([int(self.old_gvkey), self.old_conm, self.c_total/self.c_cnt])

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

  def _write_last_row(self):
    ""
    self._increment_company_flags()

    # print the last rows to two reports
    self._print_to_month_rpt()
    self._print_to_compy_rpt()

  def _close_files(self):
    self.tgt_c.close()
    self.tgt_m.close()
    self.src.close()
  
  def parse(self):
    ""

    try:
      # write the header rows into target report files first
      self.writer_m.writerow([ 'gvkey', 'conm', 'Month', 'ILLIQ_average'])
      self.writer_c.writerow([ 'gvkey', 'conm', 'ILLIQ_average'])

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
      self._write_last_row()
    finally:
      self._close_files()

def main():

    # First thing is to get the blob key of the file you want to calculate.
    #blob_key =

    # Construct BlobReader object
    #blob_reader = blobstore.BlobReader(blob_key, buffer_size=1048576)

    # Parse the content read from the blob
    parser = CSVCalc(open('data-mini.csv', 'rb'))

    parser.prepare_reader()
    parser.prepare_writers()
    parser.parse()

if __name__ == '__main__':
  main()
