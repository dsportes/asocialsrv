import os, sys, time
#from wsgiref.simple_server import make_server

pyp = os.path.dirname(__file__)
sys.path.insert(0, pyp)

def application(environ,start_response):
    t = time.asctime(time.localtime(time.time()))
    txt = ("Bonjour : " + str(t)).encode("utf-8")
    response_header = [('Content-type','text/plain'), ('Content-length', str(len(txt)))]
    start_response("200 OK",response_header)
    return [txt]

#httpd = make_server('localhost', 8000, application)
#httpd.serve_forever()