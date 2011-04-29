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
    def get(self):
	blobs = blobstore.BlobInfo.all()
	upload_url = blobstore.create_upload_url('/upload')
	file_downloadurl_pairs = [[str(blob.filename), '/download/'+str(blob.key())] for blob in blobs]
	#csvfiles = [str(blob.filename) for blob in blobs]	
	#download_urls = ['/download/'+str(blob.key()) for blob in blobs]

	template_values = { 
	    #'csvfiles': csvfiles, 
	    #'download_urls': download_urls,
	    'upload_url': upload_url,
	    'fileurl_pairs': file_downloadurl_pairs
	}
        
	path = os.path.join(os.path.dirname(__file__), 'index.html')
        self.response.out.write(template.render(path, template_values))	

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    def post(self):
        upload_files = self.get_uploads('filename')  # 'filename' is file upload field in the form
        blob_info = upload_files[0]
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
          ], debug=True)
    run_wsgi_app(application)

if __name__ == '__main__':
  main()
