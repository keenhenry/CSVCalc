#!/usr/bin/python
#

import os
import urllib

from google.appengine.ext import blobstore
from google.appengine.ext import webapp
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app

class MainHandler(webapp.RequestHandler):
   
    def __init__(self):
    	self.template_values = {}

    def get(self):
	upload_url = blobstore.create_upload_url('/upload')
	blobs = blobstore.BlobInfo.all()
	file_downloadurl_pairs = [[str(blob.filename), '/download/'+str(blob.key())] for blob in blobs]
	csvparser_url = '/parse'
	#csvfiles = [str(blob.filename) for blob in blobs]	
	#download_urls = ['/download/'+str(blob.key()) for blob in blobs]

	self.template_values['upload_url'] = upload_url
	self.template_values['fileurl_pairs'] = file_downloadurl_pairs
	self.template_values['csvparser_url'] = csvparser_url

	path = os.path.join(os.path.dirname(__file__), 'index.html')
        self.response.out.write(template.render(path, self.template_values))

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        #upload_files = self.get_uploads('filename')  # 'filename' is file upload field in the form
        #blob_info = upload_files[0]
        self.redirect('/')

class CSVParser(webapp.RequestHandler):
    def post(self):
        download_url = self.request.get('dropdown') 	# the blob key in 'string' type!
	#blob_reader = blobstore.BlobReader(blob_key)

        self.redirect(download_url)
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
