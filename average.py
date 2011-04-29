#!/usr/bin/python
#

import csv
#import os
#import urllib

#from google.appengine.ext import blobstore
#from google.appengine.ext import webapp
#from google.appengine.ext.webapp import blobstore_handlers
#from google.appengine.ext.webapp import template
#from google.appengine.ext.webapp.util import run_wsgi_app

def main():

    # First thing is to get the blob key of the file you want to calculate.
    #blob_key =

    # Construct BlobReader object
    #blob_reader = blobstore.BlobReader(blob_key, buffer_size=1048576)
   
    # Parse the content read from the blob
    #for line in blob_reader:
    with open('data.csv', 'rb') as f:
    	old_gvkey, old_conm, old_Month = None, None, None
	m_cnt, c_cnt, m_total, c_total = 0, 0, 0, 0
        rpt_month = open('reports/rpt-month.csv', 'wb')
	rpt_compy = open('reports/rpt-company.csv', 'wb')

    	for row in csv.DictReader(f):
	    # first record in csv file
	    if old_gvkey==None and old_Month==None: 
	    	old_gvkey, old_Month, old_conm = row['gvkey'], row['Month'], row['conm']
		m_cnt = 1
		m_total = 0 if row['ILLIQ']=='' or row['ILLIQ']=='#DIV/0!' else float(row['ILLIQ'])
		continue

	    # gvkey not changed - still in the records belong to the same company
	    if old_gvkey==row['gvkey']:
	    	if old_Month==row['Month']:
		    m_cnt += 1
		    m_total += 0 if row['ILLIQ']=='#DIV/0!' else float(row['ILLIQ'])
		else:
		    c_cnt += m_cnt
		    c_total += m_total
            	    print >>rpt_month, row['gvkey'],'|',row['conm'],'|', old_Month,'|', m_total / m_cnt
		    old_Month = row['Month']
		    m_cnt = 1
		    m_total = 0 if row['ILLIQ']=='#DIV/0!' else float(row['ILLIQ'])
	    # gvkey changed - data for a new company is fetched
	    else:
	    	c_cnt += m_cnt
		c_total += m_total
            	print >>rpt_compy, old_gvkey,'|', old_conm,'|', c_total / c_cnt
	    	old_gvkey, old_Month, old_conm = row['gvkey'], row['Month'], row['conm']
		m_cnt, c_cnt, c_total = 1, 0, 0
		m_total = 0 if row['ILLIQ']=='#DIV/0!' else float(row['ILLIQ'])

	# Need to think about a way to deal with the last row of csv file
	# 
        print >>rpt_month, old_gvkey,'|', old_conm,'|', old_Month,'|', m_total / m_cnt
        print >>rpt_compy, old_gvkey,'|', old_conm,'|', c_total / c_cnt

	rpt_month.close()
	rpt_compy.close()
    f.close()

if __name__ == '__main__':
  main()
