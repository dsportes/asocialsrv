import os, sys, traceback, json, cgi, importlib
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

class AppExc(Exception):
    def __init__(self, code, args, lang="?"):
        self.msg = dics.format(code, args, lang)
        Exception.__init__(self, self.msg)
        self.args = args
        self.code = code
        c = code[0]
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
    stmap = {'200':'200 OK', '400':'400 Bad Request', '404':'404 - Not Found'}
    def __init__(self, status=200):
        self.st = status
        self.mime = "application/octet-stream"
        self.encoding = "utf-8"
        self.bytes = u''
    
    def setText(self, text, ext="text/plain", encoding="utf-8"):
        self.mime = mime.type(ext)
        self.bytes = text.encode(self.encoding)
        return self

    def setJson(self, dictArg):
        self.bytes = json.dumps(dictArg).encode(self.encoding)
        self.mime = "application/json"
        return self

    def setBytes(self, bytesArg, ext="application/octet-stream"):
        self.mime = mime.type(ext)
        self.bytes = bytesArg
        return self

    def headers(self):
        return  [('Content-type', self.mime + "; charset=" + self.encoding), 
                 ('Content-length', str(len(self.bytes))), 
                 ('Access-Control-Allow-Origin', cfg.origins),
                 ('Access-Control-Allow-Headers', 'X-Custom-Header')]

    def status(self):
        return Result.stmap.get(str(self.st), '400 Bad Request')

class Operation:
    def __init__(self, execCtx):
        self.execCtx = execCtx
        self.param = execCtx.param
        self.inputData = execCtx.inputData
        self.opName = execCtx.opName
    
    def work(self):
        return Result().setJson({'operation':self.operation})

class ExecCtx:
    modules = {}
    
    def getClass(mod, cl):  # self.operation = getattr(module, cl)(self)
        e = ExecCtx.modules.get(mod, None)
        if e is None:
            module = importlib.import_module(mod);
            e = {'module':module, 'classes':{}}
            ExecCtx.modules[mod] = e
        else:
            module = e['module']
        c = e['classes'].get(cl, None)
        if c is None:
            c = getattr(module, cl)
            e['classes'][cl] = c;
        return c        
    
    def __init__(self, environ, opPath): # opPath : path après /$op/4.7/org/base.InfoOP
        self.error = None
        try:
            self.phase = 1
            self.inputData = {}
            self.param = {}            
            self.xch = environ.get("HTTP_X_CUSTOM_HEADER", None)
            environ.setdefault('QUERY_STRING', '')
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

            i = opPath.find("/") # mod.Op/org/a/b...
            u = "" if i == -1 else opPath[i+1:]
            self.opName = opPath if i == -1 else opPath[:i]
            self.inputData['url'] = [] if len(u) == 0 else u.split("/")
            self.org = self.inputData['url'][0] if len(self.inputData['url']) != 0 else ""
            i = self.opName.rfind(".")
            mod = "base" if i == -1 else self.opName[:i]
            cl = self.opName if i == -1 else self.opName[i+1:]
            try:
                c = ExecCtx.getClass(mod, cl)
                self.operation = c(self)
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

def homeShortcut(sc = None):
    return cfg.homeShortcuts["?"] if sc is None else cfg.homeShortcuts.get(sc, None)

class Url:
    def __init__(self, environ):
        global al
        p = environ["PATH_INFO"]    # /cp/prod-home
        al.info(p)

        i = p.find("/$swjs")        # service worker script
        if i != -1:
            self.type = 1
            self.cp = "" if i == 0 else p[1:i]
            return
        
        i = p.find("$info/")        # info à propos d'une organisation
        if i != -1:
            self.type = 2
            self.org = p[i + 6:]
            return

        i = p.find("/$ui/")         # Accès à une ressource UI /$ui/4.10/...ext
        if i != -1:
            self.type = 4
            x = p[i + 5:]
            i = x.find("/")
            if i == -1:
                self.resbuild = x
                self.resname = ""
            else:
                self.resbuild = x[:i]
                self.resname = x[i + 1:]
            i = self.resname.rfind(".")
            self.ext = "" if i == -1 else self.resname[i + 1:]
            return
        
        i = p.find("/$op/")     # Appel d'une opération /$op/4.7/mod.Op/org/a/b
        if i != -1:
            self.type = 5
            x = p[i + 5:]
            i = x.find("/")
            self.opPath = "" if i == -1 else x[i + 1:]  # mod.Op/org/a/b
            return
        
        # c'est une page d'accueil
        self.type = 3
        self.mode = 1
        self.ext = ""
        self.org = ""
        self.home = ""
        self.lang = cfg.lang
        self.breq = None
        self.cp = ""
        
        p1 = ""
        i = p.rfind("/")
        if i >= 0:
            self.cp = p[1:i]
            p1 = p[i + 1:] 

        # extension
        i = p1.rfind(".")
        if i != -1:
            self.ext = p1[i + 1:]
            if self.ext.startswith("a"):
                self.mode = 2
            if self.ext.startswith("i"):
                self.mode = 0

        orgHome = p1 if len(self.ext) == 0 else p1[:len(p1) - len(self.ext) - 1]
        if len(orgHome) == 0:
            orgHome = homeShortcut()
        else:
            i = orgHome.find("-")
            if i == -1:
                x = homeShortcut(orgHome)
                orgHome = x if x is not None else orgHome + "-index"
        i = orgHome.find("-")
        self.org = orgHome[:i]
        self.home = orgHome[i+1:]
        
        qs = environ["QUERY_STRING"]
        if len(qs) != 0:
            i = qs.find("lang=")
            if i != -1:
                j = qs.find("&", i + 5)
                self.lang = qs[i + 5:j] if j != -1 else qs[i + 5:]
            else:
                self.lang = cfg.lang
            
            i = qs.find("build=")
            if i != -1:
                j = qs.index("&", i + 6)
                x = qs[i + 6:j] if j != -1 else qs[i + 6:]
                if len(x) != 0:
                    self.breq = [0, 0, 0]
                    y = x.split["."]
                    try:
                        self.breq[0] = int(y[0]) if len(y) >= 1 else 0
                        self.breq[1] = int(y[1]) if len(y) >= 2 else 0
                        self.breq[2] = int(y[2]) if len(y) >= 3 else 0
                        if self.breq[0] < 1 or self.breq[1] < 0 or self.breq[2] < 0 :
                            self.breq = None
                    except:
                        self.breq = None

def uiPath(resbuild, resname):
    p = cfg.uipath + "/" + resname
    if cfg.uipath.endswith("/"):
        p = cfg.uipath + resbuild + "/" + resname
    return p
           
def getSwjs(cp):
    build = str(cfg.inb) + "." + str(cfg.uib[0])
    p = uiPath(build, "base/sw.js")
    dx = uiPath(build, "")
    lx = len(dx)
    try:
        lst = []
        for subdir, dirs, files in os.walk(dx):
            for file in files:
                if subdir.endswith("/"):
                    if file.startswith("."):
                        continue
                    filepath = (subdir + file)[lx:]
                else:
                    filepath = (subdir + "/" + file)[lx:]
                lst.append("x + \"" + filepath + "\"")
        d1 = "const shortcuts = " + json.dumps(cfg.homeShortcuts) + ";\n"
        d2 = "const inb = " + str(cfg.inb) + ";\nconst uib = " +  str(cfg.uib) + ";\nconst cp = \"" + cp + "\";\n;"
        d3 = "const CP = cp ? '/' + cp + '/' : '/';\nconst CPOP = CP + '$op/';\nconst CPUI = CP + '$ui/';\nconst BC = inb + '.' + uib[0];\nconst x = CPUI + BC +'/';\nconst lres = [\n"
        f = open(p, "rb")
        t = f.read().decode("utf-8");
        text = d1 + d2 + d3 + ",\n".join(lst) + "\n];\n" + t;
        return Result().setText(text, "js")
    except Exception as e:
        raise AppExc("swjs", [p, str(e)])

def info(org):
    return Result().setJson({"inb":cfg.inb, "uib":cfg.uib, "opsite":cfg.opsites(org)})

def getRes(resbuild, resname, ext):
    p = uiPath(resbuild, resname)
    try:
        f = open(p, "rb")
        return Result().setBytes(f.read(), ext)
    except Exception as e:
        raise AppExc("res", [p, str(e)])

def getHome(url):
    build = str(cfg.inb) + "." + str(cfg.uib[0])
    cpui = url.cp + "/$ui" if len(url.cp) != 0 else "$ui"
    if url.breq is not None:
        if url.breq[0] != cfg.inb:
            raise AppExc("majeur", [cfg.inb, url.breq[0]], url.lang)
        if url.breq[0] == 0:
            build = str(cfg.inb) + "." + cfg.uib[0]
        else:
            if url.breq[1] in cfg.uib:
                build = str(cfg.inb) + "." +  url.breq[1]
            else:
                raise AppExc("mineur", [cfg.inb, str(cfg.uib), url.breq[1]], url.lang)
    p = uiPath(build, url.home + ".html")
    try:
        lst = []
        done = False
        base = "<base href=\"/" + cpui + "/" + build + "/\" data-build=\"" + build + "\">\n"
        with open(p, "r") as ins:
            for line in ins:
                if done:
                    lst.append(line)
                else:
                    i = line.find("<base ")
                    if i != -1:
                        lst.append(base)
                        done = True
                    else:
                        lst.append(line)
        return Result().setText("".join(lst), "html")
    except:
        raise AppExc("home", [url.home, build, p], url.lang)

dics.set("fr", "bonjour", "Bonjour {0} {1} !")
dics.set("en", "bonjour", "Hello {0} {1} !")
al.info(dics.format("bonjour", ["Daniel", "Sportes"]))

dics.set("fr", "BPARAMJSON", "param [{0}]: syntaxe JSON incorrecte {1}")
dics.set("fr", "BURL", "url mal formée : [{0}]")
dics.set("fr", "BOPNAME", "opération inconnue : [{0}]")
dics.set("fr", "BOPINIT", "opération non instantiable : [{0}]")

dics.set("fr", "rni", "Requête non identifiée [{0}]")
dics.set("fr", "res", "Ressource non lisible [{0}]. Cause : {1}")
dics.set("fr", "swjs", "Ressource sw.js non lisible [{0}]. Cause : {1}")
dics.set("fr", "rb", "Le répertoire de la build [{0}] n'est pas accessible : [{1}]")
dics.set("fr", "home", "La page d'accueil [{0}] pour la build [{1}] (path:[{2}]) n'est pas accessible")
dics.set("fr", "majeur", "La ou les builds déployées sont {0}.x : une {1}.x a été demandée")
dics.set("fr", "mineur", "La ou les builds déployées sont {0}.{1} : {2} a été demandée")
dics.set("fr", "org", "L'organisation {0} n'est pas hébergée")

if cfg.mode == 2:   # mettre dans le path le répertoire qui héberge la build courante du serveur OP
    pyp = os.path.dirname(__file__)
    sys.path.insert(0, pyp + "/" + str(cfg.inb) + "." + str(cfg.opb[0]))
    # module = importlib.import_module("base")
    # print("base")
#    for x in sys.path:
#        print(x)

def application(environ, start_response):
    if environ["REQUEST_METHOD"] == "OPTIONS":
        return Result().setText("")
    try:
        url = Url(environ)
        if url.type == 1:
            return getSwjs(url.cp); 
        if url.type == 2:
            return info(url.org)
        if url.type == 3:
            return getHome(url)
        if url.type == 4:
            return getRes(url.resbuild, url.resname, url.ext)
        if url.type == 5:
            return ExecCtx(environ, url.opPath).go()
        raise AppExc("rni", environ["PATH_INFO"])
    except AppExc as e:
        al.warn(e.msg)
        return Result(400 if e.code != "res" else 404).setText(str(e.msg))
    except Exception as e:
        traceback.print_exc()
        return Result(400).setText(str(e) + traceback.format_exc())
    
al.setInfo()
    
    