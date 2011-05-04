#!/usr/bin/python
#

import os
import urllib

from google.appengine.ext import blobstore
from google.appengine.ext import webapp
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app
from average import CSVCalc

rpt_blobkey_pairs = []

class MainHandler(webapp.RequestHandler):
    def get(self):
	upload_url = blobstore.create_upload_url('/upload')
	blobs = blobstore.BlobInfo.all()
	csvparser_url = '/parse'
	file_blobkey_pairs = [[str(blob.filename), str(blob.key())] for blob in blobs]
	#month_rpt_urls = ['/download/'+str(pair[0]) for pair in rpt_blobkey_pairs]
	#compy_rpt_urls = ['/download/'+str(pair[1]) for pair in rpt_blobkey_pairs]
	rpt_pair_urls = [('/download/'+str(pair[0]), '/download/'+str(pair[1])) for pair in rpt_blobkey_pairs]

	template_values = {
		'upload_url': upload_url,
		'filekey_pairs': file_blobkey_pairs,
		'csvparser_url': csvparser_url,
		'rpt_pair_urls': rpt_pair_urls
		#'month_rpt_urls': month_rpt_urls,
		#'compy_rpt_urls': compy_rpt_urls
	}

	path = os.path.join(os.path.dirname(__file__), 'index.html')
        self.response.out.write(template.render(path, template_values))

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        #upload_files = self.get_uploads('filename')  # 'filename' is file upload field in the form
        #blob_info = upload_files[0]
        self.redirect('/')

class CSVParser(webapp.RequestHandler):
    def post(self):
        blob_key = self.request.get('dropdown') 	# the blob key in 'string' type!

	if blob_key != 'default':
    	    parser = CSVCalc(blob_key)
	    parser.prepare_reader()
    	    parser.prepare_writers()
    	    parser.parse()
	    parser.close_files()
	    #(rpt_month, rpt_compy) = parser.get_rpt_blobkeys()
	    rpt_blobkey_pairs.append(parser.get_rpt_blobkeys())
        self.redirect('/')
	#path = os.path.join(os.path.dirname(__file__), 'index.html')
        #self.response.out.write(template.render(path, template_values))

class DownloadHandler(blobstore_handlers.BlobstoreDownloadHandler):
    def get(self, blob_key):
        blob_key = str(urllib.unquote(blob_key))
	
	if not blobstore.get(blob_key):
	    self.error(404)
	else:
            self.send_blob(blobstore.BlobInfo.get(blob_key), save_as=True)

def main():
    application = webapp.WSGIApplication(
          [('/', MainHandler),
           ('/upload', UploadHandler),
           ('/download/([^/]+)?', DownloadHandler),
	   ('/parse', CSVParser)	# will be changed to CSVParser later
          ], debug=True)
    run_wsgi_app(application)

if __name__ == '__main__':
  main()
