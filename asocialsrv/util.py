import os, sys, json
from config import cfg
from datetime import datetime

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

#################################################################
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

#################################################################
class AppExc(Exception):
    def __init__(self, err, args=[], lang="?"):
        self.msg = dics.format(err, args, lang)
        Exception.__init__(self, self.msg)
        self.args = args
        self.err = err
        c = err[0]
        self.toRetry = (c == 'B') or (c == 'X') or (c == 'C')

#################################################################
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

#################################################################
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

#################################################################
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
        print(l2b)
        l2c = st2.stamp
        print(l2c)
        st3 = Stamp.fromStamp(l3)
        l3b = st3.epoch
        print(l3b)
        l3c = st3.stamp
        print(l3c)

#################################################################
class FakeOp:
    def __init__(self):
        self.org = "z"
        self.opName = "operation.Fake"
        self.stamp = Stamp.fromEpoch(Stamp.epochNow())

#################################################################
