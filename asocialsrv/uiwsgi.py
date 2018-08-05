"""
Trois services : (quatre en test)
a) génération de home page
https://site1/cpui/org/page_build.mode?lg=fr&a=xx&b=yy
https://site1/org/page_build.mode?lg=fr&a=xx&b=yy

b) génération du script du service worker
https://site/cpui/org/sw_in.ui.js

c) ping UI retournant le site du serveur op et ses builds
https://site/cpui/ping

d) service des ressources UI statiques (pour test local)
"""

import os, sys, json, traceback
from wsgiref.simple_server import make_server

pyp = os.path.dirname(__file__)
sys.path.insert(0, pyp)
from config import Cfg

cfg = Cfg()

def defLang(lgx = None):
    global cfg
    return lgx if (lgx is not None or lgx in cfg.langs) else cfg.langs[0]

class AL:
    LEVEL = 2
    def setInfo():
        AL.LEVEL = 0
    def setWarn():
        AL.LEVEL = 0
    def setError():
        AL.LEVEL = 0
    def info(text):
        if AL.LEVEL >= 0:
            print(text, file=sys.stderr)
    def warn(text):
        if AL.LEVEL >= 1:
            print(text, file=sys.stderr)
    def error(text):
        print(text, file=sys.stderr)

class Dic:
    def __init__(self):
        global cfg;
        Dic.d1 = " - 0:{0} 1:{1} 2:{2} 3:{3}"
        self.dic = {}
        for x in cfg.langs:
            self.dic[x] = {}
        self.set("fr", "rni", "Requête non identifiée [{0}]")
        self.set("fr", "res", "Ressource non lisible [{0}]. Cause : {1}")
        self.set("fr", "swjs", "Ressource sw.js non lisible [{0}]. Cause : {1}")
        self.set("fr", "rb", "Le répertoire de la build [{0}] n'est pas accessible : [{1}]")
        self.set("fr", "home", "la page d'accueil [{0}] pour la build [{1}] (path:[{2}]) n'est pas accessible")
        self.set("fr", "majeur", "la ou les builds déployées sont {0}.x : une {1}.x a été demandée")
        self.set("fr", "mineur", "la ou les builds déployées sont {0}.{1} : {2} a été demandée")
        self.set("fr", "org", "l'organisation {0} n'est pas hébergée")
            
    def set(self, lang, code, msg):
        if code is None or msg is None:
            return
        l = defLang(lang)
        self.dic[l][code] = msg
        
    def get(self, lang, code):
        if code is None:
            return "?" + Dic.d1
        lx = defLang(lang)
        m = self.dic[lx].get(code, None)
        if m is None and lx != cfg.langs[0]:
            m = self.dic[cfg.langs[0]].get(code, None)
        return m if m is not None else code + Dic.d1

dic = Dic()

def homeShortcut(sc = None):
    global cfg
    return cfg.homeShortcuts["?"] if sc is None else cfg.homeShortcuts.get(sc, None)

class AppEx(Exception):
    def __init__(self, lang, code, args = []):
        global dic
        self.msg = dic.get(lang, code)
        Exception.__init__(self, self.msg.format(code, args))

class Url:
    def __init__(self, environ):
        global cfg
        p = environ["PATH_INFO"] # /cp/prod-home
        AL.info(p)
        
        # on enlève le context-path s'il y en a un
        l = len(cfg.cp)
        if l != 0 and p.startswith("/" + cfg.cp + "/"):
            p = p[l + 2:]       
        else:
            p = p[1:]
        # on isole l'extension
        self.mode = 1
        self.ext = ""
        i = p.rfind(".")
        if i != -1:
            self.ext = p[i + 1:]
            p = p[:i]
            if self.ext.startswith("a"):
                self.mode = 2
            if self.ext.startswith("i"):
                self.mode = 0
        
        if self.ext == "js" and p == "sw":
            self.type = 1
            return
        
        if p.startswith("z/"):
            self.type = 2
            self.org = p[2:]
            return
        
        if p.startswith("$ui/"):
            self.type = 4
            x = p[4:]
            i = p.find("/", 4)
            x = "" if str(self.ext) == 0 else "." + self.ext
            if i == -1:
                self.resbuild = p
                self.resname = "" + x
            else:
                self.resbuild = p[4:i]
                self.resname = p[i + 1:] + x
            return
        
        self.type = 3
        orgHome = p
        if len(p) == 0:
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
    cx = "/" + cfg.cp + "/$ui/" if (len(cfg.cp) != 0) else "/$ui/"
    try:
        lst = []
        for subdir, dirs, files in os.walk(dx):
            for file in files:
                if subdir.endswith("/"):
                    filepath = subdir + file
                else:
                    filepath = subdir + "/" + file
                lst.append(cx + filepath[lx:])
        d1 = "const shortcuts = " + json.dumps(cfg.homeShortcuts) + ";\n"
        d2 = "const build = \"" + build + "\";\nconst cp = \"" + cfg.cp + "\";\nconst lres = [\n"
        f = open(p, "r")
        t = f.read();
        return (d1 + d2 + ",\n".join(lst) + "\n];\n" + t).encode("utf-8")
    except Exception as e:
        raise AppEx(cfg.langs[0], "swjs", [p, str(e)])

def info(org):
    global cfg
    opsite = cfg.opsites.get(org, None)
    if opsite is None:
        raise AppEx(cfg.langs[0], "org", [org])
    return json.dumps({"uib":[cfg.inb, cfg.uib[0]], "opsite":opsite}).encode("utf-8")

def getRes(resbuild, resname):
    global cfg
    p = uiPath(resbuild, resname)
    try:
        f = open(p, "rb")
        return f.read()
    except Exception as e:
        raise AppEx(cfg.langs[0], "res", [p, str(e)])

def getHome(url):
    global cfg
    build = str(cfg.inb) + "." + str(cfg.uib[0])
    if url.breq is not None:
        if url.breq[0] != cfg.inb:
            raise AppEx(url.lang, "majeur", cfg.inb, url.breq[0])
        if url.breq[0] == 0:
            build = str(cfg.inb) + "." + cfg.uib[0]
        else:
            if url.breq[1] in cfg.uib:
                build = str(cfg.inb) + "." +  url.breq[1]
            else:
                raise AppEx(url.lang, "mineur", cfg.inb, str(cfg.uib), url.breq[1])
    p = uiPath(build, url.home + ".html")
    try:
        lst = []
        done = False
        cx = "/" if len(cfg.cp) == 0 else "/" + cfg.cp + "/"
        base = "<base href=\"" + cx + "$ui/" + build + "/\" data-build=\"" + build + "\">\n"
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
        raise AppEx(url.lang, "home", [url.home, build, p])

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

def app(environ, start_response):
    global cfg
    global mimeTypes
    global pyp
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
        txt = str(e).encode("utf-8")
        start_response("400 Bad Request", [('Content-type', "text/plain"), ('Content-length', str(len(txt)))])
        return [txt]
    except Exception as e:
        traceback.print_exc()
        tb = traceback.format_exc()
        txt = (str(e) + tb).encode("utf-8")
        start_response("400 Bad Request", [('Content-type', "text/plain"), ('Content-length', str(len(txt)))])
        return [txt]
        
AL.setInfo()
httpd = make_server('localhost', 8000, app)
AL.warn("Serving on port 8000...")
httpd.serve_forever()