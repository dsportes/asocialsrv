import os, sys, traceback, json, cgi, importlib
from config import cfg
from datetime import datetime
from threading import Lock

class AL:
    def __init__(self):
        self.level = cfg.loglevel
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
al = AL()

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

class AppExc(Exception):
    def __init__(self, err, args=[], lang="?"):
        self.msg = dics.format(err, args, lang)
        Exception.__init__(self, self.msg)
        self.args = args
        self.err = err
        c = err[0]
        self.toRetry = (c == 'B') or (c == 'X') or (c == 'C')

class MimeTypes:
    def __init__(self):
        self.types =  {
                "html":"text/html",
                "htm":"text/html",
                "ahtm":"text/html",
                "ihtm":"text/html",
                "a":"text/html",
                "css":"text/css",
                "json":"application/json",
                "js":"text/javascript",
                "md":"text/markdown",
                "txt":"text/plain",
                "xml":"application/xml",
                "appcache":"text/cache-manifest",
                "pem":"application/x-pem-file",
                "woff":"application/font-woff",
                "woff2":"application/font-woff2",
                "svg":"image/svg+xml",
                "gif":"image/gif",
                "ico":"image/x-icon",
                "png":"image/png",
                "jpg":"image/jpeg",
                "jpeg":"image/jpeg"
        }
    def type(self, code):
        if code is None:
            return "application/octet-stream"
        return self.types.get(code, "application/octet-stream") if code.find("/") == -1 else code
  
mime = MimeTypes()        

class Result:
    def __init__(self, op=None, notFound=False):    # op : Operation OU ExecCtc
        self.origin = op.origin if op is not None else None
        self.respXCH = op.respXCH if op is not None and hasattr(op, "respXCH") else None
        self.notFound = notFound
        self.mime = "application/octet-stream"
        self.bytes = b''
        self.noCache = False
        self.json = None
    
    def getJson(self):
        return self.json;
    
    def setOptions(self, reqOrigin):
        # self.origin = "*"
        self.origin = reqOrigin
        return self
    
    def setText(self, text, ext="text/plain"):
        self.mime = mime.type(ext)
        self.bytes = text.encode("utf-8")
        return self

    def setJson(self, dictResp):
        self.json = dictResp
        return self

    def setBytes(self, bytesArg, ext="application/octet-stream"):
        self.mime = mime.type(ext)
        self.bytes = bytesArg
        return self
    
    def setNoCache(self):
        self.noCache = True
        return self

    def finalLength(self):
        if self.json is not None:
            self.bytes = json.dumps(self.json).encode("utf-8")
            self.mime = "application/json"
            self.json = None
        return len(self.bytes)

    def headers(self):
        lst = []
        if self.noCache:
            lst.append(("Cache-control", "no-cache, no-store, must-revalidate"))
        lst.append(('Content-type', self.mime + "; charset=utf-8")) 
        lst.append(('Content-length', str(self.finalLength())))
        if self.origin is not None:
            lst.append(('Access-Control-Allow-Origin', self.origin))
        if self.respXCH is not None:
            lst.append(('X-Custom-Header', json.dumps(self.respXCH)))
        lst.append(('Access-Control-Allow-Headers', 'X-Custom-Header'))
        return lst

    def status(self):
        return '404 Not Found' if self.notFound else '200 OK'

class Operation:
    def __init__(self, execCtx):
        self.execCtx = execCtx
        self.origin = execCtx.origin
        self.param = execCtx.param
        self.inputData = execCtx.inputData
        self.org = execCtx.org
        self.opName = execCtx.opName
        self.reqXCH = execCtx.reqXCH
        self.stamp = execCtx.stamp
        self.respXCH = None
        self.provider = providerClass(self)
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
        al.info(p)

        if p == "/favicon.ico":
            return self.getRes(p)

        if p == "/$ping":             # service worker script
            return Result(self).setText(self.stamp.toString())
        
        if p == "/$swjs":           # service worker script
            return self.getSwjs()
        
        if p == "/$sw.html":        # test build courante sw
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
            self.xch(environ)
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
        sn = environ.get('SCRIPT_NAME', '')
        u += sn
        al.warn(sn) 
        return self.getHome(p, environ["QUERY_STRING"], u)                          # c'est une page d'accueil

    def getRes(self, p):
        build = str(cfg.inb) + "." + str(cfg.uib[0])
        path = self.uiPath(build, p[1:])
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
        p = self.uiPath(build, "base/sw.js")
        dx = self.uiPath(build, "")
        lx = len(dx)
        try:
            lst = []
            y = "/$ui/" if len(cfg.cp) == 0 else "/" + cfg.cp + "/$ui/"
            x = y + build +'/'
            for subdir, dirs, files in os.walk(dx):
                for file in files:
                    if subdir.endswith("/"):
                        if file.startswith("."):
                            continue
                        filepath = (subdir + file)[lx:]
                    else:
                        filepath = (subdir + "/" + file)[lx:]
                    lst.append("\"" + x + filepath + "\"")
            d1 = "const shortcuts = " + json.dumps(cfg.homeShortcuts) + ";\n"
            d2 = "const inb = " + str(cfg.inb) + ";\nconst uib = " +  str(cfg.uib) + ";\nconst cp = \"" + cfg.cp + "\";\n"
            d3 = "const CP = cp ? '/' + cp + '/' : '/';\nconst CPOP = CP + '$op/';\nconst CPUI = CP + '$ui/';\nconst BC = inb + '.' + uib[0];\nconst lres = [\n"
            f = open(p, "rb")
            t = f.read().decode("utf-8")
            text = d1 + d2 + d3 + ",\n".join(lst) + "\n];\n" + t
            return Result(self).setText(text, "js")
        except Exception as e:
            raise AppExc("swjs", [p, str(e)])

    def getHome(self, p, qs, u):
        # al.error("path1 : " + p)
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
        # Dev : 127.0.0.1:8081/         index.html?$org=prod&$home=index&$build=1.1&$mode=0&$cp=cp&$appstore=http://127.0.0.1:8000/     
        x = "$build=" + build + "&$org=" + org + "&$home=" + home + "&$mode=" + str(mode) + "&$cp=" + cfg.cp + "&$appstore=" + u   
        redir = cfg.static_appstore + (build2 + "/" if cfg.BUILD else "") + home + ".html" + (qs + "&" if len(qs) != 0 else "?") + x
        
        page = "<html><head><meta http-equiv='refresh' content='0;URL=" + redir + "'></head><body></body></html>"
        al.info(page)
        return Result(self).setText(page, "html")
                

dics.set("fr", "bonjour", "Bonjour {0} {1} !")
dics.set("en", "bonjour", "Hello {0} {1} !")
al.info(dics.format("bonjour", ["Daniel", "Sportes"]))

dics.set("fr", "BPARAMJSON", "param [{0}]: syntaxe JSON incorrecte {1}")
dics.set("fr", "BURL", "url mal formée : [{0}]")
dics.set("fr", "BOPNAME", "opération inconnue : [{0}] - Cause : [{1}]")
dics.set("fr", "BOPINIT", "opération non instantiable : [{0}]")
dics.set("fr", "SECORIG1", "opération rejetée car provenant d'une origine inconnue")
dics.set("fr", "SECORIG2", "opération rejetée car provenant d'une origine non acceptée : [{0}]")
dics.set("fr", "DBUILD", "la version de l'application graphique n'est pas acceptée par le serveur des opérations")

dics.set("fr", "rni", "Requête non identifiée [{0}]")
dics.set("fr", "notFound", "Ressource non trouvée / lisible [{0}]. Cause : {1}")
dics.set("fr", "swjs", "Ressource sw.js non lisible [{0}]. Cause : {1}")
dics.set("fr", "rb", "Le répertoire de la build [{0}] n'est pas accessible : [{1}]")
dics.set("fr", "home", "La page d'accueil [{0}] pour la build [{1}] (path:[{2}]) n'est pas accessible")
dics.set("fr", "majeur", "La ou les builds déployées sont {0}.x : une {1}.x a été demandée")
dics.set("fr", "mineur", "La ou les builds déployées sont {0}.{1} : {2} a été demandée")
dics.set("fr", "org", "L'organisation {0} n'est pas hébergée")

dics.set("fr", "XSQLCNX", "Incident de connexion à la base de données. Opération:{0} Org:{1} Cause:{2}")

if cfg.OPSRV:   # mettre dans le path le répertoire qui héberge la build courante du serveur OP
    pyp = os.path.dirname(__file__)
    sys.path.insert(0, pyp + "/" + str(cfg.inb) + "." + str(cfg.opb[0]))

from settings import settings

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

class Stamp:
    epoch0 = datetime(1970, 1, 1)
    _minEpoch = 946684800000
    _maxEpoch = 4102444799999
    _minStamp = 101000000000
    _maxStamp = 99123123595959999
    _nbj = [[0,31,28,31,30,31,30,31,31,30,31,30,31], [0,31,29,31,30,31,30,31,31,30,31,30,31]]
    _nbjc = [[0,0,0,0,0,0,0,0,0,0,0,0,0,0], [0,0,0,0,0,0,0,0,0,0,0,0,0,0]]
    i = 0
    while i < 2:
        m = 1
        while m < 14:
            _nbjc[i][m] = 0
            k = 1
            while k < m:
                _nbjc[i][m] += _nbj[i][k]
                k += 1
            m += 1
        i += 1
    _qa = (365 * 4) + 1
    _nbjq = [0, 366, 366 + 365, 366 + 365 + 365, 366 + 365 + 365 + 365]
    # nb jours 2000-01-01 - 1970-01-01 - 30 années dont 7 bissextiles - C'était un Samedi
    _nbj00 = (365 * 30) + 7
    _wd00 = 5
    _nbms00 = _minEpoch % 86400000
    
    def epochNow():
        dt = datetime.utcnow()
        ms = int(dt.microsecond / 1000)
        n = int((dt - Stamp.epoch0).total_seconds()) * 1000
        return n + ms
    
    def nbj(yy, mm):
        return Stamp._nbj[1 if yy % 4 == 0 else 0][mm]
    
    def truncJJ(yy, mm, jj):
        x = Stamp._nbj[1 if yy % 4 == 0 else 0][mm]
        return x if jj > x else jj
    
    def __init__(self):
        self.yy = 0
        self.MM = 1
        self.dd = 1
        self.HH = 0
        self.mm = 0
        self.ss = 0
        self.ms = 0
        self.time = 0
        self.date = 101
        self.stamp = Stamp._minStamp
        self.epoch = Stamp._minEpoch
        self.wd = 5
        self.nbd00 = 0
        self.epoch00 = 0
        self.nbms = Stamp._nbms00 
        
    def fromEpoch(epoch):
        if epoch < Stamp._minEpoch:
            epoch = Stamp._minEpoch
        if epoch > Stamp._maxEpoch:
            epoch = Stamp._maxEpoch
        myself = Stamp()
        myself.epoch = epoch
        myself.nbd00 = (epoch // 86400000) - Stamp._nbj00
        myself.wd = ((myself.nbd00 + Stamp._wd00) % 7) + 1
        myself.nbms = epoch % 86400000
        myself.epoch00 = (myself.nbd00 * 86400000) + myself.nbms
        myself.yy = (myself.nbd00 // Stamp._qa) * 4
        x1 = myself.nbd00 % Stamp._qa
        na = 0
        while True:
            nbjcx = Stamp._nbjc[1 if myself.yy % 4 == 0 else 0]
            if x1 < Stamp._nbjq[na + 1]:
                nj = x1 - Stamp._nbjq[na]
                myself.MM = 1
                while True:
                    if nj < nbjcx[myself.MM+1]:
                        myself.dd = nj - nbjcx[myself.MM] + 1
                        break                      
                    myself.MM += 1
                break
            myself.yy += 1
            na += 1
        myself.date = myself.dd + (myself.MM * 100) + (myself.yy * 10000)
        myself.HH = myself.nbms // 3600000
        x = myself.nbms % 3600000
        myself.mm = x // 60000
        x = myself.nbms % 60000
        myself.ss = x // 1000
        myself.SSS = x % 1000
        myself.time = myself.SSS + (myself.ss * 1000) + (myself.mm * 100000) + (myself.HH * 10000000)
        myself.stamp = (myself.date * 1000000000) + myself.time
        return myself
            
    def fromStamp(stamp):        
        if stamp > Stamp._maxStamp:
            stamp = Stamp._maxStamp
        if stamp < Stamp._minStamp:
            stamp = Stamp._minStamp
        myself = Stamp()
        myself.SSS = stamp % 1000
        x = stamp // 1000
        myself.ss = x % 100
        x = x // 100
        myself.mm = x % 100
        x = x // 100
        myself.HH = x % 100
        x = x // 100
        myself.dd = x % 100
        x = x // 100
        myself.MM = x % 100
        x = x // 100
        myself.yy = x % 100
        return myself.normalize()
            
    def fromDateUTC(yy, MM, dd, hh=0, mm=0, ss=0, SSS=0):
        myself = Stamp()
        myself.yy = yy
        myself.MM = MM
        myself.dd = dd
        myself.hh = hh
        myself.mm = mm
        myself.ss = ss
        myself.SSS = SSS
        return myself.normalize()
        
    def normalize(self):
        if self.yy < 0:
            self.yy = 0
        if self.yy > 99: 
            self.yy = 99
        if self.MM < 1: 
            self.MM = 1
        if self.MM > 12:
            self.MM = 12
        self.dd =  1 if self.dd < 1 else Stamp.truncJJ(self.yy, self.MM, self.dd)
        if self.HH < 0:
            self.HH = 0
        if self.mm < 0:
            self.mm = 0
        if self.ss < 0:
            self.ss = 0
        if self.SSS < 0:
            self.SSS = 0
        if self.HH > 23:
            self.HH = 23
        if self.mm > 59:
            self.mm = 59
        if self.ss > 59:
            self.ss = 59
        if self.SSS > 999:
            self.SSS = 999
        self.time = self.SSS + (self.ss * 1000) + (self.mm * 100000) + (self.HH * 10000000)
        self.date = self.dd + (self.MM * 100) + (self.yy * 10000)
        self.stamp = (self.date * 1000000000) + self.time
        self.q = Stamp._nbjc[1 if self.yy % 4 == 0 else 0][self.MM] + self.dd
        self.nbd00 = ((self.yy // 4) * Stamp._qa) + Stamp._nbjq[(self.yy % 4)] + self.q - 1
        self.nbms = self.SSS + (self.ss * 1000) + (self.mm * 60000) + (self.HH * 3600000)
        self.epoch00 = (self.nbd00 * 86400000) + self.nbms
        self.epoch = ((self.nbd00 + Stamp._nbj00) * 86400000) + self.nbms
        self.wd = ((self.nbd00 + Stamp._wd00) % 7) + 1
        return self
    
    def toString(self):
        s = str(self.stamp)
        return "0000000"[0:15 - len(s)] + s

    def test():
        l1 = Stamp.epochNow()
        st = Stamp.fromEpoch(l1)
        l2 = st.epoch
        l3 = st.stamp
        st2 = Stamp.fromStamp(l2)
        l2b = st2.epoch
        l2c = st2.stamp
        st3 = Stamp.fromStamp(l3)
        l3b = st3.epoch
        l3c = st3.stamp

# Stamp.test()

providerClass = None

try:
    providerModule = importlib.import_module(settings.dbProvider[0])
    providerClass = getattr(providerModule, settings.dbProvider[1])
    if providerClass is None:
        al.error("Provider class NON TROUVEE : " + settings.dbProvider[0] + "." + settings.dbProvider[1])
except Exception as e:
    al.error("Provider class NON TROUVEE : " + settings.dbProvider[0] + "." + settings.dbProvider[1] + str(e))

