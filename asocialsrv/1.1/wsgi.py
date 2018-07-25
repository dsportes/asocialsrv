import time
import os
import sys, traceback, json, threading

pyp = os.path.dirname(__file__)
print('************************ wsgi start 1 ***************', file=sys.stderr)
sys.path.insert(0, pyp)


if 'c1' in globals():
    c1 += 1
else:
    c1 = 0
    c2 = 0

# import ptvsd
# if c2 == 0:
#     ptvsd.enable_attach('secret', address=('localhost', 36000))
#     ptvsd.wait_for_attach()
#     time.sleep(5)

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

    def go(self, environ):
        n = 0
        while True:
            try:
                raise AppException("BUG", ["a1", "a2", 3])
                cx = 1/0
                text = json.dumps({'key1':'val1', 'key2':'val2'})
                return Result(True).setText(text)
            except AppException as ex:
                if n == 2 or not ex.toRetry:
                    err = {'code':ex.code, 'info':ex.msg, 'args':ex.args, 'phase':self.phase, 'tb':traceback.format_exc()}
                    return Result(False).setText(json.dumps(err))
                else:
                    n += 1
            except:
                exctype, value = sys.exc_info()[:2]
                err = {'code':'U', 'info':str(exctype) + ' - ' + str(value), 'phase':self.phase, 'tb':traceback.format_exc()}
                return Result(False).setText(json.dumps(err))


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
        return  [('Content-type', self.mime + "; charset=" + self.encoding), ('Content-length', len(self.bytes))]

    def status(self):
        return '200' if self.ok else '400'

def application(environ, start_response):
    x = threading.local()
    ec = ExecContext()
    x.toto = 'toto'
    result = ec.go(environ)
    print('********** result **********', file=sys.stderr)
    print(result.status(), file=sys.stderr)
    print(result.headers(), file=sys.stderr)
    print(result.bytes, file=sys.stderr)
#   start_response(result.status(), result.headers())
    return [result.bytes]

res = application(None, None)
x = threading.local()
ec = x.toto
n = 0
