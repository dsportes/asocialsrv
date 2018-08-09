"""
Amorce du serveur op : 
a) innclut le vrai serveur 1.1 dans son path
b) pour chaque request, créé un ExecCtx et invoque sa méthode go() et retourne son résultat
"""
import os, sys
from wsgiref.simple_server import make_server

pyp = os.path.dirname(__file__)
sys.path.insert(0, pyp)
from config import cfg

os.environ['TZ'] = cfg.timezone

if (pyp.endswith("/0.0")):
    # on est dans la racine : il faut mettre la version réelle dans le path
    opb = str(cfg.inb) + "." + str(cfg.opb[0])
    pyp2 = pyp[:(len(pyp) - 3)] + opb
    sys.path.insert(0, pyp2)
    
import execCtx

def application(environ, start_response):
    p = environ["PATH_INFO"]
    if p == "/favicon.ico":
        res = "favicon.ico not found".encode("utf-8")
        start_response('400 Bad Request', [('Content-type', "text/plain"), ('Content-length', str(len(res)))])
        return [res]
    ec = execCtx.ExecCtx(environ)
    result = ec.error if ec.error is not None else ec.go()
    start_response(result.status(), result.headers())
    return [result.bytes]

if cfg.debugserver:
    httpd = make_server('localhost', 8001, application)
    httpd.serve_forever()