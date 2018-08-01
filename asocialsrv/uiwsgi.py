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
    def __init__(self, pyp):
        # context-path UI
        self.cpui = "ui"
        # niveau d'intrerface
        self.inb = 1
        # builds UI servies, la première est celle conseillée (une seule obligatoire)
        self.uib = [1, 2]
        # builds OP servies, la première est celle conseillée (une seule obligatoire)
        self.opb = [1, 2]
        # path des ressources chez l'hébergeur. None si c'est dans le répertoire sous celui de ce script
        self.respath = "/home/daniel/git/asocial/asocial/build"
        
        # URL des sites pour chaque organisation
        d1 = "http://localhost:8000/op"
        self.sites = {"org1":d1, "org2":d1}
        
        # templates applicables à chaque page d'accueil
        t1 = "home1.html"
        t2 = "home2.html"
        self.homes = {"adh":t1, "admin":t2}
        
        # page d'accueil par défaut
        self.defhome = "adh"
        
        # raccourcis
        self.shortcuts = {"?":"org1/adh", "ad":"org1/admin"}
        
        # langues supportées, la première est celle par défaut (obligatoire)
        self.langs = ["fr", "en"]
        
        if self.respath is not None:
            self.respath = pyp
        if self.respath[-1] != "/":
            self.respath += "/"

    def defLg(self, lgx = None):
        return lgx if (lgx is not None or lgx in self.langs) else self.langs[0]
    def shorcut(self, sc = None):
        return self.shortcuts["?"] if sc is None else self.shortcuts[sc]
    def defUib(self):
        return str(self.inb) + "." + str(self.uib[0])
        
############### Fin de configuration

import os, sys
from wsgiref.util import request_uri, application_uri
from wsgiref.simple_server import make_server

pyp = os.path.dirname(__file__)
sys.path.insert(0, pyp)

cfg = Cfg(pyp)

class Dic:
    def __init__(self, cfg):
        Dic.d1 = " - 0:{0} 1:{1} 2:{2} 3:{3}"
        self.cfg = cfg;
        self.dic = {}
        for x in cfg.langs:
            self.dic[x] = {}
        self.set("fr", "mf", "L'adresse de la page est mal formée : ")
        self.set("fr", "mode", "les seules extensions auttorisées sont .i (mode incognito) et .a (mode avion). [{0}] a été trouvé")
        self.set("fr", "build", "le code d'une build doit être de la forme 4.7 ou 4.7.5 ([{0}] a été trouvé)")
        self.set("fr", "majeur", "seules les builds de niveau {0}. sont déployées. {1}. a été trouvé")
        self.set("fr", "mineur", "seules les builds {0}. sont déployées. {1} a été trouvé")
            
    def set(self, lg, code, msg):
        if code is None or msg is None:
            return
        l = self.cfg.defLg(lg)
        self.dic[l][code] = msg
    def get(self, lg, code):
        if code is None:
            return "?" + Dic.d1
        m = self.dic[self.cfg.defLg(lg)][code]
        return m if m is not None else code + Dic.d1
    def deplUib(self):
        s = 0
        for x in self.uib:
            s += str(self.inb) + "." + str(x) + " "
        return s
    def deplOpb(self):
        return self.opb.join(" ")

dic = Dic(cfg)

class AL:
    AL.LEVEL = 2
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

class AppEx(Exception):
    def __init__(self, lg, code, args = []):
        global dic
        self.msg = dic.get(lg, code)
        Exception.__init__(self, self.msg.format(code, args))

class UiUrl:
    def __init__(self, environ):
        global cfg
        self.pi = environ["PATH_INFO"]
        self.qs = environ["QUERY_STRING"]
        self.lg = cfg.defLg()
        if self.qs is not None:
            i = self.qs.find("lg=")
            if i != -1:
                j = self.qs.index("&", i + 3)
                self.lg = self.qs[i:j] if j != -1 else self.qs[i:]
            else:
                self.lg = cfg.defLg()
        self.lg = cfg.defLg(self.lg)
        
        # on enlève le site https://site:port/ (le dernier / n'est pas obligatoire
        i = self.pi.index("://")
        p = self.pi[i + 3]
        try:
            j = p.index("/")
            p = p[j + 1:]
        except:
            p = ""
        
        # on enlève l'extension .a ou .i (quoi que .a n'a pas à être trouvé ici)
        self.mode = 1
        i = p.find(".", -1)
        if i != -1:
            ext = p[i + 1:]
            if ext != "a" and ext != "i":
                raise AppEx(self.lg, "mode", [ext])
            p = p[:2]
            self.mode = 0 if ext == "i" else 2
        
        # on enlève le context-path s'il y en a un
        if cfg.cpui is not None and p.startswith(cfg.cpui):
            p = p[len(cfg.cpui):]
        
        # on enlève la build
        i = p.find("_", -1)
        b = ""
        if i != -1:
            b = p[i + 1:]
            
            if ext != "a" and ext != "i":
                raise AppEx(self.lg, "mode")
            p = p[:2]
            self.mode = 0 if ext == "i" else 2
        
        # p : "" raccourci complet
        # p org
            
    def splitBuild(self, b):
        i = b.find(".")
        if i == -1:
            raise AppEx(self.lg, "build", [b])
        majeur = 0
        try:
            majeur = int(b[:i])
            bx = b[i + 1:]
            if majeur != self.cfg.inb:
                raise AppEx(self.lg, "majeur", [str(cfg.inb), str(majeur)])
            i = bx.find(".")
            if i == -1:
                try:
                    mineur = int(b[i + 1:])
                    if mineur not in self.cfg.uib:
                        raise AppEx(self.lg, "mineur", [cfg.deployUib, b])
                    return [majeur, mineur, 0]
                except:
                    raise AppEx(self.lg, "build", [b])
            else:
                try:
                    op = int(b[i + 1:])
                    if op not in self.cfg.opb:
                        raise AppEx(self.lg, "op", [cfg.deployOpb, str(op)])
                    return [majeur, mineur, 0]
                except:
                    raise AppEx(self.lg, "build", [b])
                
        except:
            raise AppEx(self.lg, "build", [b])
                
            

def app(environ, start_response):
    try:
        uiUrl = UiUrl(environ)
        
        
    except:
        
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