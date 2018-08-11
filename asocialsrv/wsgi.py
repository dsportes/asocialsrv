"""
Amorce d'un serveur : 
a) lance éventuellement le serveur local
b) redirige les appels de application vers le module root
"""
import os, sys
from wsgiref.simple_server import make_server
from root import application as app

pyp = os.path.dirname(__file__)
sys.path.insert(0, pyp)

def application(environ, start_response):
    result = app(environ, start_response)
    start_response(result.status(), result.headers())
    return [result.bytes]

if True:
    httpd = make_server('localhost', 8000, application)
    print("Local server listening 8000 ...", file=sys.stderr)
    httpd.serve_forever()
