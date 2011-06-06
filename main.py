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

class MainHandler(webapp.RequestHandler):
    def get(self):
	upload_url = blobstore.create_upload_url('/upload')
	blobs = blobstore.BlobInfo.all()
	csvparser_url = '/parse'
	uploaded_blobinfos, rpt_blobinfos = [], []

	for blob in blobs:
	  filename = str(blob.filename)
	  if not filename[0:3]=='rpt':
	    uploaded_blobinfos.append( (filename, str(blob.key())) )
	  else:
	    create_time = blob.creation
	    rpt_blobinfos.append( (filename, '/download/'+str(blob.key()), create_time.strftime("%Y-%m-%d %H:%M:%S"), create_time) )
	rpt_blobinfos.sort(key=lambda t: t[3])	# sort by creation time
	
	template_values = {
		'upload_url': upload_url,
		'csvparser_url': csvparser_url,
		'uploaded_blobs': uploaded_blobinfos,
		'rpt_blobs': rpt_blobinfos,
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
        blob_key = self.request.get('dropdown') 	# the blob key in 'str' type!

	if blob_key != 'default':
    	    parser = CSVCalc(blob_key)
	    parser.prepare_reader()
    	    parser.parse()
	    parser.close_files()
        self.redirect('/')

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
	   ('/parse', CSVParser)	
          ], debug=True)
    run_wsgi_app(application)

if __name__ == '__main__':
  main()
