import os, sys, traceback, json, cgi, importlib
from config import cfg
from threading import Lock
from util import Result, AppExc, al, Stamp

pyp = os.path.dirname(__file__)
if cfg.OPSRV:   # mettre dans le path le répertoire qui héberge la build courante du serveur OP
    sys.path.insert(0, pyp + "/" + str(cfg.inb) + "." + str(cfg.opb[0]))

from settings import sqlselector

class Operation:
#    sqlselector = None
    def __init__(self, execCtx):
#        if Operation.sqlselector is None:
#            Operation.sqlselector = getattr(importlib.__import__("sqlselector"), "sqlselector")
        self.execCtx = execCtx
        self.origin = execCtx.origin
        self.param = execCtx.param
        self.inputData = execCtx.inputData
        self.org = execCtx.org
        self.opName = execCtx.opName
        self.reqXCH = execCtx.reqXCH
        self.stamp = execCtx.stamp
        self.respXCH = None
        self.provider = sqlselector.get(self)
        self.checkOnOff()
        al.info("Operation ==> " + self.opName + " " + self.stamp.toString())

    def work(self):
        return Result(200, self).setJson({'operation':self.operation})
    
    def checkOnOff(self):
        z, y = self.provider.onoff()
        st = 1 if z.ison > 0 and y.ison > 0 else 0
        self.respXCH = {"inb":cfg.inb, 'uiba':self.execCtx.uiba, 'onoff':st, 'infoGen':z.info, 'infoOrg':y.info}
        if self.opName == 'base.InfoOP':
            return
        if z.ison != 1:
            raise AppExc("OFF2")
        if y.ison != 1:
            raise AppExc("OFF3")
    
    def close(self):
        if self.provider is not None:
            self.provider.close()

class ExecCtx:
    modules = {}
    modLock = Lock()
        
    def getClass(mod, cl):  # self.operation = getattr(module, cl)(self)
        with ExecCtx.modLock:
            e = ExecCtx.modules.get(mod, None)
            if e is None:
                module = importlib.import_module(mod)
                e = {'module':module, 'classes':{}}
                ExecCtx.modules[mod] = e
            else:
                module = e['module']
            c = e['classes'].get(cl, None)
            if c is None:
                c = getattr(module, cl)
                e['classes'][cl] = c
            return c        
    
    def __init__(self, environ, url): # opPath : path après /$op/4.7/org/base.InfoOP
        self.reqXCH = url.reqXCH
        self.stamp = url.stamp
        self.origin = url.origin
        self.app = None
        self.uiba = None    
        self.inputData = {}
        self.param = {}
        self.opName = ""
        self.org = None
        self.operationClass = None
        
        self.operation = None
        self.result = None
        self.error = None
        self.phase = 1

        try:
            self.app = self.reqXCH.get('app', None)
            self.uiba = cfg.uiba.get(self.app, None)
            if self.uiba is None:
                raise AppExc("OFF0")
            majeur = self.reqXCH.get("inb", 0)
            mineur = self.reqXCH.get("uib", 0)
            if majeur != cfg.inb or mineur < self.uiba[0] or mineur in self.uiba[1:]:
                xx = [cfg.inb, cfg.opb[0]]
                for y in self.uiba:
                    xx.append(y)
                raise AppExc("DBUILD", xx)
            
            form = cgi.FieldStorage(fp=environ['wsgi.input'], environ=environ, keep_blank_values=1)
            if form.list is not None:
                cl = form.headers.get("content-length", "0")
                self.contentLength = int(cl)
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

            i = url.opPath.find("/") # mod.Op/org/a/b...
            u = "" if i == -1 else url.opPath[i+1:]
            self.opName = url.opPath if i == -1 else url.opPath[:i]
            self.inputData['url'] = [] if len(u) == 0 else u.split("/")
            self.org = self.inputData['url'][0] if len(self.inputData['url']) != 0 else ""
            i = self.opName.rfind(".")
            mod = "base" if i == -1 else self.opName[:i]
            cl = self.opName if i == -1 else self.opName[i+1:]
            try:
                self.operationClass = ExecCtx.getClass(mod, cl)
            except Exception as e:
                raise AppExc("BOPNAME", [self.opName, str(e)])
                
        except AppExc as ex:
            err = {'err':ex.err, 'info':ex.msg, 'args':ex.args, 'phase':self.phase, 'tb':traceback.format_exc()}
            al.warn(err)
            self.error = Result(self).setJson(err)
        except Exception as e:
            exctype, value = sys.exc_info()[:2]
            err = {'err':'BU1', 'info':str(exctype) + ' - ' + str(value), 'phase':self.phase, 'tb':traceback.format_exc()}
            al.error(err)
            self.error = Result(self).setJson(err)
        
    def go(self):
        if self.error is not None:
            return self.error
        n = 0
        while True:
            try:
                if n > 0:
                    self.stamp = Stamp.fromEpoch(Stamp.epochNow())
                self.phase = 1
                self.operation = self.operationClass(self)
                result = self.operation.work()
                if result is None:
                    result = Result(self.operation)
                self.operation.close()
                return result
            except AppExc as ex:
                if self.operation is not None:
                    self.operation.close()
                if n == 2 or not ex.toRetry:
                    err = {'err':ex.err, 'info':ex.msg, 'args':ex.args, 'phase':self.phase} # , 'tb':traceback.format_exc()
                    al.warn(err)
                    return Result(self).setJson(err)
                else:
                    n += 1
            except Exception:
                if self.operation is not None:
                    self.operation.close()
                exctype, value = sys.exc_info()[:2]
                err = {'err':'BU2', 'info':str(exctype) + ' - ' + str(value), 'phase':self.phase, 'tb':traceback.format_exc()}
                al.error(err)
                return Result(self).setJson(err)                

def homeShortcut(sc = None):
    return cfg.homeShortcuts["?"] if sc is None else cfg.homeShortcuts.get(sc, None)

class Url:
    def xch(self, environ):
        xch = environ.get("HTTP_X_CUSTOM_HEADER", None)
        if xch is None:
            raise AppExc("SECORIG1")
        try:
            self.reqXCH = json.loads(xch)
        except:
            self.reqXCH = None
        if self.reqXCH is None:
            raise AppExc("SECORIG1")
        if self.origin not in cfg.origins:
            raise AppExc("SECORIG2", [self.origin])

    def __init__(self, origin):
        self.origin = origin
    
    def doit(self, environ):
        self.stamp = Stamp.fromEpoch(Stamp.epochNow())
        environ.setdefault('QUERY_STRING', '')
        p = environ["PATH_INFO"]    # /cp/prod-home
        self.pathInfo = p
        self.cp = cfg.cp
        l = len(self.cp)
        if l != 0 and p.startswith("/" + self.cp + "/"):
            p = p[l + 1:]
        al.warn("Origin:" + self.origin + " Path_Info:" + p)

        if p == "/favicon.ico":
            return self.getRes(p)

        if p == "/$ping":             # service worker script
            return Result(self).setText(self.stamp.toString())
        
        if p == "/$swjs":           # service worker script
            return self.getSwjs()
        
        if p == "/$sw.html":        # sqlselector build courante sw
            build = str(cfg.inb) + "." + str(cfg.uib[0])
            p = self.uiPath(build, "$sw.html")
            try:
                f = open(p, "rb")
                return Result(self).setBytes(f.read(), "html")
            except Exception as e:
                raise AppExc("notFound", [p, str(e)])

        if p == "/$infoSW":         # DEVRAIT être intercepté par le sscript du service worker
            return Result(self).setJson({"err":"SW not active"})
        
        if p.startswith("/$info/"): # info à propos d'une organisation
            org = p[7:]
            # self.xch(environ)
            return Result(self).setJson({"inb":cfg.inb, "uib":cfg.uib, "opsite":cfg.opsites(org)})
        
        if p.startswith("/$op/"):   # Appel d'une opération /$op/4.7/mod.Op/org/a/b
            x = p[6:]
            i = x.find("/")
            self.opPath = "" if i == -1 else x[i + 1:]  # mod.Op/org/a/b
            self.xch(environ)
            return ExecCtx(environ, self).go()
        
        u = environ['wsgi.url_scheme']+'://'
        if environ.get('HTTP_HOST'):
            u += environ['HTTP_HOST']
        else:
            u += environ['SERVER_NAME']
            if environ['wsgi.url_scheme'] == 'https':
                if environ['SERVER_PORT'] != '443':
                    u += ':' + environ['SERVER_PORT']
            else:
                if environ['SERVER_PORT'] != '80':
                    u += ':' + environ['SERVER_PORT']
        al.warn(u) 
        return self.getHome(p, environ["QUERY_STRING"], u)                          # c'est une page d'accueil

    def getRes(self, p):
        #build = str(cfg.inb) + "." + str(cfg.uib[0])
        #path = self.uiPath(build, p[1:])
        path = pyp + p
        i = p.rfind(".")
        ext = "txt" if i == -1 else p[i + 1:]
        try:
            with open(path, "rb") as f:
                return Result(self).setBytes(f.read(), ext)
        except Exception as e:
            raise AppExc("notFound", [p, str(e)])

    def uiPath(self, resbuild, resname):
        return cfg.uipath + "/" + resbuild + "/" + resname if cfg.BUILD else cfg.uipath + "/" + resname
           
    def getSwjs(self):
        build = str(cfg.inb) + "." + str(cfg.uib[0])
        swjspath = pyp + "/sw.js"
        dx = self.uiPath(build, "")
        lx = len(dx)
        try:
            lst = []
            y = "/$ui/" if len(cfg.cp) == 0 else "/" + cfg.cp + "/$ui/"
            x = y + build +'/'
            for subdir, dirs, files in os.walk(dx):
                subd = subdir.replace("\\", "/")
                for file in files:
                    if subd.endswith("/"):
                        if file.startswith("."):
                            continue
                        filepath = (subd + file)[lx:]
                    else:
                        filepath = (subd + "/" + file)[lx:]
                    lst.append("\"" + x + filepath + "\"")
            d1 = "const shortcuts = " + json.dumps(cfg.homeShortcuts) + ";\n"
            d2 = "const inb = " + str(cfg.inb) + ";\nconst uib = " +  str(cfg.uib) + ";\nconst cp = \"" + cfg.cp + "\";\n"
            d3 = "const dyn_appstore = \"" + cfg.dyn_appstore + "\";\nconst static_appstore = \"" + cfg.static_appstore + "\";\nconst lres = [\n"
            f = open(swjspath, "rb")
            t = f.read().decode("utf-8")
            text = d1 + d2 + d3 + ",\n".join(lst) + "\n];\n" + t
            return Result(self).setText(text, "js")
        except Exception as e:
            raise AppExc("swjs", [swjspath, str(e)])

    def getHome(self, p, qs, u):
        # al.error("path1 : " + p)
        ext = ""
        mode = 1
        breq = None        

        # extension
        i = p.rfind(".")
        if i != -1:
            ext = p[i + 1:]
            if ext.startswith("a"):
                mode = 2
            if ext.startswith("i") or ext == "html":
                mode = 0

        orgHome = p if len(ext) == 0 else p[:len(p) - len(ext) - 1]
        if len(orgHome) == 0:
            orgHome = homeShortcut()
        else:
            i = orgHome.find("-")
            if i == -1:
                x = homeShortcut(orgHome)
                orgHome = x if x is not None else orgHome + "-index"
        i = orgHome.find("-")
        org = orgHome[1:i]
        home = orgHome[i+1:]
        
        if len(qs) != 0:
            i = qs.find("lang=")
            if i != -1:
                j = qs.find("&", i + 5)
                lang = qs[i + 5:j] if j != -1 else qs[i + 5:]
            else:
                lang = cfg.lang

            i = qs.find("build=")
            if i != -1:
                j = qs.index("&", i + 6)
                x = qs[i + 6:j] if j != -1 else qs[i + 6:]
                if len(x) != 0:
                    breq = [0, 0, 0]
                    y = x.split["."]
                    try:
                        breq[0] = int(y[0]) if len(y) >= 1 else 0
                        breq[1] = int(y[1]) if len(y) >= 2 else 0
                        breq[2] = int(y[2]) if len(y) >= 3 else 0
                        if breq[0] < 1 or breq[1] < 0 or breq[2] < 0 :
                            breq = None
                    except:
                        breq = None

        build = str(cfg.inb) + "." + str(cfg.uib[0])
        build2 = (str(cfg.inb) if breq is None else str(breq[0])) + "." + (str(cfg.uib[0]) if breq is None else str(breq[1])) 
        # cpui = cfg.cp + "/$ui" if len(cfg.cp) != 0 else "$ui"
        if breq is not None:
            if breq[0] != cfg.inb:
                raise AppExc("majeur", [cfg.inb, breq[0]], lang)
            if breq[0] == 0:
                build = str(cfg.inb) + "." + cfg.uib[0]
            else:
                if breq[1] in cfg.uib:
                    build = str(cfg.inb) + "." +  breq[1]
                else:
                    raise AppExc("mineur", [cfg.inb, str(cfg.uib), breq[1]], lang)
                
        # Build :  127.0.0.1/cp/$ui/    1.1/index.html?$org=prod&$home=index&$build=1.1&$mode=0&$cp=cp&$appstore=http://127.0.0.1:8000/     
        # Dev : 127.0.0.1:8081/         index.html?$org=prod&$home=index&$build=1.1&$mode=0&$cp=cp&$appstore=http://127.0.0.1:8000/cp/     
        x = "$build=" + build + "&$org=" + org + "&$home=" + home + "&$mode=" + str(mode) + "&$cp=" + cfg.cp + "&$appstore=" + cfg.dyn_appstore + "&$maker=" + cfg.dyn_appstore 
        redir = cfg.static_appstore + (build2 + "/" if cfg.BUILD else "") + home + ".html" + (qs + "&" if len(qs) != 0 else "?") + x
        
        page = "<html><head><meta http-equiv='refresh' content='0;URL=" + redir + "'></head><body></body></html>"
        al.info(page)
        return Result(self).setText(page, "html")

def application(environ, start_response):
    method = environ.get("REQUEST_METHOD", "")
    origin = environ.get("HTTP_ORIGIN", "?")
    if method == "" or method == "OPTIONS":
        al.warn("OPTIONS : " + environ["PATH_INFO"] + " " + origin)
        return Result().setOptions(origin)     # c'est une OPTIONS : origin = "*"

    u = Url(origin)
    try:
        return u.doit(environ)
    except AppExc as e:
        al.warn(e.msg)
        return Result(u, e.err == "notFound").setText(str(e.msg))
    except Exception as e:
        exctype, value = sys.exc_info()[:2]
        err = {'err':'BU3', 'info':str(exctype) + ' - ' + str(value), 'phase':0, 'tb':traceback.format_exc()}
        al.error(err)
        return Result(u).setJson(err)

