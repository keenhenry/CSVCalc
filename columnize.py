#!/usr/bin/python

#
# This script tries to transform a multi-column csv doc into a 
# uni-column csv doc.
# @author Henry Huang
# @date 06/10/2011
#

from __future__ import with_statement
from google.appengine.api import files
from google.appengine.ext import blobstore
import csv
import itertools

class CSVOneColumn(object):
  '''
  '''
  
  def __init__(self, blob_key=None):
    '''
    '''
    self.src = blob_key
    self.blob_reader = None
    self.reader = None
    self.rpt = None

    # An empty column stores the resulting csv report: a string
    self.col = ''

  def prepare_reader(self):
    ""

    if self.src:
      self.blob_reader = blobstore.BlobReader(self.src)
      self.reader = csv.reader(self.blob_reader)
    #self.reader = csv.reader(open('Price.csv'))

  def _unitize_columns(self, columns=None):
    ""

    if columns:
      l = [row for row in itertools.chain(*columns)]
      self.col += '\r\n'.join(l)
      #print self.col

  def close_files(self):
    self.blob_reader.close()
    files.finalize(self.rpt)
  
  def parse(self):
    ""

    # Extract columns and aggregate them into a list: zipped_columns
    zipped_columns = zip(*self.reader)
    
    # Group columns into ONE final column
    self._unitize_columns(zipped_columns)

    # making up reports' file name
    infile_blob_info = self.blob_reader.blob_info
    rpt_filename = 'rpt-1col-' + infile_blob_info.filename

    # creating report blobs in blobstore
    self.rpt = files.blobstore.create(mime_type='application/octet-stream', _blobinfo_uploaded_filename=rpt_filename)

    # writing data (in csv format) into blobs
    with files.open(self.rpt, 'a') as f:
      f.write(self.col)

#if __name__ == '__main__':
#  parser = CSVOneColumn()
#  parser.prepare_reader()
#  parser.parse()
#  parser.close_files()
