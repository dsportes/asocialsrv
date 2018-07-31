import time
import os
import sys, traceback, json, cgi
from wsgiref.util import setup_testing_defaults, request_uri, application_uri, shift_path_info
from wsgiref.simple_server import make_server

pyp = os.path.dirname(__file__)
sys.path.insert(0, pyp)

class AL:
    LEVEL = 2
    def setInfo():
        AL.LEVEL = 0
    def setWarn():
        LEVEL = 0
    def setError():
        LEVEL = 0
    def info(text):
        if AL.LEVEL >= 0:
            print(text, file=sys.stderr)
    def warn(text):
        if AL.LEVEL >= 1:
            print(text, file=sys.stderr)
    def error(text):
        print(text, file=sys.stderr)

class AppException(Exception):
    def __init__(self, code, args):
        self.msg = "app exception : code:{0} args:{1}"  # recherche dans un dictionnaire ici
        Exception.__init__(self, self.msg.format(code, args))
        self.args = args
        self.code = code
        c = code[0]
        self.toRetry = (c == 'B') or (c == 'X') or (c == 'C')

class ExecContext:
    def __init__(self):
        self.phase = 1

    def go(self, environ, inputData):
        n = 0
        while True:
            try:
                #raise AppException("BUG", ["a1", "a2", 3])
                #cx = 1/0
                text = json.dumps({'key1':inputData['mdp'], 'key2':str(inputData['fic1']['val'])})
                return Result(True).setText(text)
            except AppException as ex:
                if n == 2 or not ex.toRetry:
                    err = {'code':ex.code, 'info':ex.msg, 'args':ex.args, 'phase':self.phase, 'tb':traceback.format_exc()}
                    return Result(False).setText(json.dumps(err))
                else:
                    n += 1
            except:
                exctype, value = sys.exc_info()[:2]
                err = json.dumps({'code':'U', 'info':str(exctype) + ' - ' + str(value), 'phase':self.phase, 'tb':traceback.format_exc()})
                AL.error(err)
                return Result(False).setText(err)


class Result:
    def __init__(self, ok=True, mime="application/json", encoding="utf-8"):
        self.ok = ok
        self.mime = mime
        self.encoding = encoding
    
    def setText(self, text):
        self.bytes = text.encode(self.encoding)
        return self

    def setBytes(self, bytes):
        self.bytes = bytes
        return self

    def headers(self):
        return  [('Content-type', self.mime + "; charset=" + self.encoding), ('Content-length', str(len(self.bytes)))]

    def status(self):
        return '200 OK' if self.ok else '400 Bad Request'

def inputData(environ):
    ip = {}
    form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ, keep_blank_values=1)
    for x in form:
        f = form[x]
        filename = f.filename
        if filename:
            ip[x] ={'name':x, 'filename':filename, 'type':f.type, 'val':f.file.read()}
        else:
            ip[x] ={'name':x, 'type':'arg', 'val':f.value}
    return ip

def app(environ, start_response):
    result = ExecContext().go(environ, inputData(environ))
    AL.info(request_uri(environ, include_query=1))
    AL.info(application_uri(environ))
    if AL.LEVEL == 0: 
        AL.info('********** result **********') 
        AL.info(result.status())
        AL.info(result.headers())
        AL.info(result.bytes)
    start_response(result.status(), result.headers())
    return [result.bytes]

AL.setInfo()
httpd = make_server('localhost', 8000, app)
AL.warn("Serving on port 8000...")
httpd.serve_forever()