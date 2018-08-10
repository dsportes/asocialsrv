"""
Trois services : (quatre en test)
cp/ : "" "cp/"
a) génération de home page
https://site1/cp/org-home.i?lang=fr&build=1.1.0&a=xx&b=yy

b) génération du script du service worker
https://site/cp/$sw.js

c) info UI retournant le site du serveur op et ses builds pour l'organisation org
https://site/cp/$sw.z/org

d) service des ressources UI statiques (pour test local)
https://site1/cp/$ui/1.1/bas/app.js
"""

import os, sys, json, traceback
from wsgiref.simple_server import make_server

pyp = os.path.dirname(__file__)
sys.path.insert(0, pyp)
from config import cfg

def defLang(lgx = None):
    return lgx if (lgx is not None or lgx in cfg.langs) else cfg.langs[0]

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
al = AL(2)
     
class Dic:
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
dics.set("fr", "rni", "Requête non identifiée [{0}]")
dics.set("fr", "res", "Ressource non lisible [{0}]. Cause : {1}")
dics.set("fr", "swjs", "Ressource sw.js non lisible [{0}]. Cause : {1}")
dics.set("fr", "rb", "Le répertoire de la build [{0}] n'est pas accessible : [{1}]")
dics.set("fr", "home", "La page d'accueil [{0}] pour la build [{1}] (path:[{2}]) n'est pas accessible")
dics.set("fr", "majeur", "La ou les builds déployées sont {0}.x : une {1}.x a été demandée")
dics.set("fr", "mineur", "La ou les builds déployées sont {0}.{1} : {2} a été demandée")
dics.set("fr", "org", "L'organisation {0} n'est pas hébergée")

def homeShortcut(sc = None):
    return cfg.homeShortcuts["?"] if sc is None else cfg.homeShortcuts.get(sc, None)

class AppEx(Exception):
    def __init__(self, code, args, lang="?"):
        self.msg = dics.format(code, args)
        Exception.__init__(self, self.msg)
        self.args = args
        self.code = code
        c = code[0]
        self.toRetry = (c == 'B') or (c == 'X') or (c == 'C')

class Url:
    def __init__(self, environ):
        global al
        p = environ["PATH_INFO"] # /cp/prod-home
        al.info(p)

        # on enlève le context-path s'il y en a un
        l = len(cfg.cp)
        if l != 0 and p.startswith("/" + cfg.cp + "/"):
            p = p[l + 2:]       
        else:
            p = p[1:]

        if p == "$sw.js":
            self.type = 1
            return
        
        if p.startswith("$sw.z/"):
            self.type = 2
            self.org = p[6:]
            return
        
        # extension
        self.mode = 1
        self.ext = ""
        i = p.rfind(".")
        if i != -1:
            self.ext = p[i + 1:]
            if self.ext.startswith("a"):
                self.mode = 2
            if self.ext.startswith("i"):
                self.mode = 0

        if p.startswith("$ui/"):
            self.type = 4
            x = p[4:]
            i = x.find("/", l)
            if i == -1:
                self.resbuild = x
                self.resname = ""
            else:
                self.resbuild = x[:i]
                self.resname = x[i + 1:]
            return
                                
        self.type = 3
        if self.ext != "":
            p = p[:len(p) - len(self.ext) - 1]
        orgHome = p
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
        self.lang = defLang()
        self.breq = None
        if len(qs) != 0:
            i = qs.find("lang=")
            if i != -1:
                j = qs.find("&", i + 5)
                self.lang = qs[i + 5:j] if j != -1 else qs[i + 5:]
            else:
                self.lang = defLang()
            
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
    global cfg
    p = cfg.uipath + "/" + resname
    if cfg.uipath.endswith("/"):
        p = cfg.uipath + resbuild + "/" + resname
    return p
           
def getSwjs():
    global cfg
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
        d2 = "const inb = " + str(cfg.inb) + ";\nconst uib = " +  str(cfg.uib) + ";\nconst cp = \"" + cfg.cp + "\";\n;"
        d3 = "const CP = cp ? '/' + cp + '/' : '/';\nconst CPOP = CP + '$op/';\nconst CPUI = CP + '$ui/';\nconst BC = inb + '.' + uib[0];\nconst x = CPUI + BC +'/';\nconst lres = [\n"
        f = open(p, "rb")
        t = f.read().decode("utf-8");
        return (d1 + d2 + d3 + ",\n".join(lst) + "\n];\n" + t).encode("utf-8")
    except Exception as e:
        raise AppEx("swjs", [p, str(e)])

def info(org):
    opsite = cfg.opsites.get(org, None)
    if opsite is None:
        raise AppEx("org", [org])
    d = {"inb":cfg.inb, "uib":cfg.uib, "opsite":opsite}
    return json.dumps(d).encode("utf-8")

def getRes(resbuild, resname):
    p = uiPath(resbuild, resname)
    try:
        f = open(p, "rb")
        return f.read()
    except Exception as e:
        raise AppEx("res", [p, str(e)])

def getHome(url):
    build = str(cfg.inb) + "." + str(cfg.uib[0])
    cpui = cfg.cp + "/$ui" if len(cfg.cp) != 0 else "$ui"
    if url.breq is not None:
        if url.breq[0] != cfg.inb:
            raise AppEx("majeur", [cfg.inb, url.breq[0]], url.lang)
        if url.breq[0] == 0:
            build = str(cfg.inb) + "." + cfg.uib[0]
        else:
            if url.breq[1] in cfg.uib:
                build = str(cfg.inb) + "." +  url.breq[1]
            else:
                raise AppEx("mineur", [cfg.inb, str(cfg.uib), url.breq[1]], url.lang)
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
        return "".join(lst).encode("utf-8")
    except:
        raise AppEx("home", [url.home, build, p], url.lang)

mimeTypes =  {
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

def application(environ, start_response):
    global mimeTypes
    try:
        url = Url(environ)
        if url.type == 1:
            result = getSwjs(); 
            ext = "js"
        elif url.type == 2:
            result = info(url.org)
            ext = "json"
        elif url.type == 3:
            result = getHome(url)
            ext = "html"
        elif url.type == 4:
            result = getRes(url.resbuild, url.resname)
            ext = url.ext
        else:
            raise AppEx(cfg.langs[0], "rni", environ["PATH_INFO"])
        start_response("200 OK", [('Content-type', mimeTypes.get(ext, "application/octet-stream")), ('Content-length', str(len(result)))])
        return [result]    
    except AppEx as e:
        al.warn(e.msg)
        txt = str(e.msg).encode("utf-8")
        start_response("400 Bad Request", [('Content-type', "text/plain"), ('Content-length', str(len(txt)))])
        return [txt]
    except Exception as e:
        traceback.print_exc()
        tb = traceback.format_exc()
        txt = (str(e) + tb).encode("utf-8")
        start_response("400 Bad Request", [('Content-type', "text/plain"), ('Content-length', str(len(txt)))])
        return [txt]

if cfg.debugserver:
    al.setWarn()
    httpd = make_server('localhost', 8000, application)
    al.warn("Serving on port 8000...")
    httpd.serve_forever()