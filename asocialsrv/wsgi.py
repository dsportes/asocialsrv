"""
Amorce d'un serveur : 
a) lance éventuellement le serveur local
b) redirige les appels de application vers le module root
"""
import os, sys
from wsgiref.simple_server import make_server

pyp = os.path.dirname(__file__)
sys.path.insert(0, pyp)
from root import application as app

def application(environ, start_response):
    result = app(environ, start_response)
    try:
        start_response(result.status(), result.headers())
        return [result.bytes]
    except Exception as e:
        print("Déconnexion 1 : " + str(e), file=sys.stderr)

httpd = make_server('localhost', 8000, application)
print("Local server listening 8000 ...", file=sys.stderr)
try:
    httpd.serve_forever()
except Exception as e:
    print("Déconnexion 2 : " + str(e), file=sys.stderr)

