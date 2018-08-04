"""
Trois services :
a) génération de home page
https://site1/cpui/org/page_build.mode?lg=fr&a=xx&b=yy
https://site1/org/page_build.mode?lg=fr&a=xx&b=yy

b) génération du script du service worker
https://site/cpui/org/sw_in.ui.js

c) ping UI retournant le site du serveur op et ses builds
https://site/cpui/ping
"""

############## Classe Configuration
class Cfg:
    def __init__(self):
        # context-path UI
        self.cp = "cp"
        
        # niveau d'intrerface
        self.inb = 1
        
        # builds UI servies, la première est l'officielle (une seule obligatoire)
        self.uib = [1, 2]
        
        # URL des serveurs op pour chaque organisation
        d1 = "http://localhost:8000/cp/$op"
        self.opsites = {"prod":d1, "demo":d1}
                
        # raccourcis
        self.homeShortcuts = {"?":"prod-index", "index2":"prod-index2", "index":"prod-index", "d":"demo-index", "admin":"prod-index2"}
        
        # langues supportées, la première est celle par défaut (obligatoire)
        self.langs = ["fr", "en"]
############### Fin de configuration

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

import os, sys, json
from wsgiref.simple_server import make_server

pyp = os.path.dirname(__file__)
sys.path.insert(0, pyp)

class AppEx(Exception):
    def __init__(self, lang, code, args = []):
        global dic
        self.msg = dic.get(lang, code)
        Exception.__init__(self, self.msg.format(code, args))

class Url:
    def __init__(self, environ):
        global cfg
        p = environ["PATH_INFO"] # /cp/prod-home
        
        # on enlève le context-path s'il y en a un
        l = len(cfg.cp)
        if l != 0 and p.startswith("/" + cfg.cp + "/"):
            p = p[l + 2:]       
        else:
            p = p[1:]
        # on enlève l'extension
        self.mode = 1
        i = p.find(".", -1)
        if i != -1:
            ext = p[i + 1:]
            p = p[:i]
            if ext.startswith("a"):
                self.mode = 2
            if ext.startswith("i"):
                self.mode = 0      
        
        if p == "sw.js":
            self.type = 1
            return
        
        if p.startswith("z/"):
            self.type = 2
            self.org = p[2:]
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
        if len(qs) != 0:
            i = qs.find("lang=")
            if i != -1:
                j = qs.find("&", i + 5)
                self.lang = qs[i + 5:j] if j != -1 else qs[i + 5:]
            else:
                self.lang = defLang()
            
            self.breq = None
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
           
def getSwjs(pyp):
    global cfg
    build = str(cfg.inb) + "." + str(cfg.uib[0])
    rootdir = pyp + "/" + build
    lx = len(rootdir)
    cx = "/" + cfg.cp + "/$ui" if (len(cfg.cp) != 0) else "/$ui"
    lst = []
    try:
        for subdir, dirs, files in os.walk(rootdir):
            for file in files:
                #print os.path.join(subdir, file)
                filepath = subdir + os.sep + file
                lst.append(cx + filepath[lx:])
        d1 = "const shortcuts = " + json.dumps(cfg.homeShortcuts) + ";\n"
        d2 = "const build = \"" + build + "\";\nconst cp = \"" + cfg.cp + "\;\nconst lres = [\n"
        f = open("build/" + build + "/src/sw.js", "r")
        t = f.read();
        return d1 + d2 + ",\n".join(lst) + "\n];\n" + t
    except:
        raise AppEx(cfg.langs[0], "rb", build, rootdir)

def info(org):
    global cfg
    opsite = cfg.opsites.get(org, None)
    if opsite is None:
        raise AppEx(cfg.langs[0], "org", org)
    build = str(cfg.inb) + "." + str(cfg.uib[0])
    return json.dumps({"uib":build, "opsite":opsite})

def getHome(pyp, url):
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
    rootdir = pyp + "/" + build + "/" + url.home + ".html"
    try:
        lst = []
        done = False
        cx = "/" if len(cfg.cp) == 0 else "/" + cfg.cp + "/"
        base = "<base href=\"" + cx + "/" + build + "/\" date-build=\"" + build + "\">\n"
        with open(rootdir, "r") as ins:
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
        return "".join(lst)
    except:
        raise AppEx(url.lang, "home", url.home, build, rootdir)

def app(environ, start_response):
    global cfg
    try:
        url = Url(environ)
        if url.type == 1:
            result = getSwjs(pyp); 
        elif url.type == 2:
            result = info(url.org)
        else:
            result = getHome(pyp, url)
        start_response("200 OK", [])
        return [result]    
    except Exception as e:
        start_response("400 Bad Request : " + str(e), [])
        return (str(e))
        
AL.setInfo()
httpd = make_server('localhost', 80, app)
AL.warn("Serving on port 8000...")
httpd.serve_forever()