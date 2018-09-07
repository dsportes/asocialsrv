"""
Amorce d'un serveur : 
a) lance éventuellement le serveur local
b) redirige les appels de application vers le module root
"""
import os, sys

pyp = os.path.dirname(__file__)
sys.path.insert(0, pyp)
from root import application as app
print("wsgi1.py Started", file=sys.stderr)

def application(environ, start_response):
    result = app(environ, start_response)
    try:
        start_response(result.status(), result.headers())
        return [result.bytes]
    except Exception as e:
        print("Déconnexion 1 : " + str(e), file=sys.stderr)
