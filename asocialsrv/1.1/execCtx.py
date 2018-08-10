import os, sys, time, traceback, json, cgi, importlib
from wsgiref.util import request_uri, application_uri, shift_path_info
from config import cfg

class AL:
    def __init__(self, lvl):
        self.level = lvl
    def setInfo(self):
        self.level = 0
    def setWarn(self):
        self.level = 1
    def setError(self):
        self.level = 2
    def info(self, text):
        if self.level < 1:
            print(text, file=sys.stderr)
    def warn(self, text):
        if self.level < 2:
            print(text, file=sys.stderr)
    def error(self, text):
        print(text, file=sys.stderr)
al = AL(1)

class Dic: #
    def __init__(self):
        self.dic = {}
        for x in cfg.langs:
            self.dic[x] = {}
            
    def set(self, lang, code, msg):
        if code is None or msg is None:
            return
        l = lang if lang in cfg.langs else cfg.lang
        self.dic[l][code] = msg
        
    def get(self, code, lang="?"):
        if code is None:
            return "?" + Dic.d1
        l = lang if lang in cfg.langs else cfg.lang
        m = self.dic[l].get(code, None)
        if m is None and l != cfg.lang:
            m = self.dic[cfg.lang].get(code, None)
        return m if m is not None else code
    def format(self, code, args, lang="?"):
        c = self.get(code, lang)
        try:
            return c.format(*args)
        except:
            x = ["0?", "1?", "2?", "3?", "4?", "5?", "6?", "7?", "8?", "9?"]
            for idx, val in enumerate(args):
                x[idx] = val
            return c.format(*x)

dics = Dic()
dics.set("fr", "bonjour", "Bonjour {0} {1} !")
dics.set("en", "bonjour", "Hello {0} {1} !")
al.info(dics.format("bonjour", ["Daniel", "Sportes"]))

dics.set("fr", "BPARAMJSON", "param [{0}]: syntaxe JSON incorrecte {1}")
dics.set("fr", "BURL", "url mal formée : [{0}]")
dics.set("fr", "BOPNAME", "opération inconnue : [{0}]")
dics.set("fr", "BOPINIT", "opération non instantiable : [{0}]")

class AppExc(Exception):
    def __init__(self, code, args):
        self.msg = dics.format(code, args)
        Exception.__init__(self, self.msg)
        self.args = args
        self.code = code
        c = code[0]
        self.toRetry = (c == 'B') or (c == 'X') or (c == 'C')

class Result:
    def __init__(self, ok=True, mime="application/json", encoding="utf-8"):
        self.ok = ok
        self.mime = mime
        self.encoding = encoding
    
    def setText(self, text):
        self.bytes = text.encode(self.encoding)
        return self

    def setJson(self, dict):
        self.bytes = json.dumps(dict).encode(self.encoding)
        return self

    def setBytes(self, bytes):
        self.bytes = bytes
        return self

    def headers(self):
        return  [('Content-type', self.mime + "; charset=" + self.encoding), 
                 ('Content-length', str(len(self.bytes))), 
                 ('Access-Control-Allow-Origin', '*'),
                 ('Access-Control-Allow-Headers', 'X-Custom-Header')]

    def status(self):
        return '200 OK' if self.ok else '400 Bad Request'

class Operation:
    def __init__(self, execCtx):
        self.execCtx = execCtx
        self.param = execCtx.param
        self.inputData = execCtx.inputData
        self.opName = execCtx.opName
    
    def work(self):
        return Result().setJson({operation:self.operation})

class ExecCtx:
    def __init__(self, environ):
        self.options = environ["REQUEST_METHOD"] == "OPTIONS"
        self.error = None
        if self.options:
            return
        try:
            self.phase = 1
            self.inputData = {}
            self.param = {}            
            form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ, keep_blank_values=1)
            for x in form:
                f = form[x]
                filename = f.filename
                if filename:
                    self.inputData[x] ={'name':x, 'type':f.type,'filename':filename, 'val':f.file.read()}
                else:
                    v = f.value
                    if x == "param":
                        try:
                            self.param = json.loads(v)
                        except Exception as e:
                            raise AppExc("BPARAMJSON", [e.msg])
                    else:
                        self.inputData[x] ={'name':x, 'val':v}
            # URL à faire et à mettre dans inputData['url']
            p = environ["PATH_INFO"] # /cp/$op/operation/...
            pfx = "/" + cfg.cp + "/$op/"
            if not p.startswith(pfx):
                raise AppExc("BURL", [p])
            p1 = p[len(pfx):]
            i = p1.find("/")
            if i == -1:
                raise AppExc("BURL", [p])
            p1 = p1[i+1:]
            i = p1.find("/")
            u = "" if i == -1 else p1[i+1:]
            self.opName = p1 if i == -1 else p1[:i]
            self.inputData['url'] = [] if len(u) == 0 else u.split("/")
            i = self.opName.rfind(".")
            mod = "base" if i == -1 else self.opName[:i]
            cl = self.opName if i == -1 else self.opName[i+1:]
            try:
                self.operation = getattr(importlib.import_module(mod), cl)(self)
            except:
                raise AppExc("BOPNAME", [self.opName])
                
        except AppExc as ex:
            err = {'code':ex.code, 'info':ex.msg, 'args':ex.args, 'phase':self.phase, 'tb':traceback.format_exc()}
            al.warn(err)
            self.error = Result(False).setJson(err)
        except Exception as e:
            exctype, value = sys.exc_info()[:2]
            err = {'code':'U', 'info':str(exctype) + ' - ' + str(value), 'phase':self.phase, 'tb':traceback.format_exc()}
            al.error(err)
            self.error = Result(False).setJson(err)
        
    def go(self):
        if self.options:
            return Result(True).setText("");
        if self.error is not None:
            return self.error
        n = 0
        while True:
            try:
                return self.operation.work()
            except AppExc as ex:
                if n == 2 or not ex.toRetry:
                    err = {'code':ex.code, 'info':ex.msg, 'args':ex.args, 'phase':self.phase, 'tb':traceback.format_exc()}
                    al.warn(err)
                    return Result(False).setJson(err)
                else:
                    n += 1
            except:
                exctype, value = sys.exc_info()[:2]
                err = {'code':'U', 'info':str(exctype) + ' - ' + str(value), 'phase':self.phase, 'tb':traceback.format_exc()}
                al.error(err)
                return Result(False).setJson(err)
