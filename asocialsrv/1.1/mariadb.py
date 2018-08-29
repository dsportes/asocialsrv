import pymysql.cursors
from settings import settings
import pymysqlpool
from root import al, dics, AppExc, Stamp
from document import DocumentArchive, ValidationList
import json
from gzip import compress, decompress
    
class Provider:
    dics.set("fr", "XSQL1", "Incident d'accès à la base de données. Opération:{0} Org:{1} SQL:{2} Cause:{3}")
    dics.set("fr", "XCV", "Trop de contention détectée à la validation. Opération:{0} Org:{1} Document:{2} ")
    
    def create_conn():
        return pymysql.connect(host=settings.db['host'], db=settings.db['db'], user=settings.db['user'], password=settings.db['password'],
                             charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

    pool = pymysqlpool.Pool(create_instance=create_conn, max_count=settings.db['poolSize'], timeout=settings.db['timeOut'])
    lapseRefreshOnOff = 300000
    onofStamp = Stamp()
    onoffDict = {}
    
    def __init__(self, operation):
        try:
            self.connection = Provider.pool.get()
            self.operation = operation
            self.org = operation.org
        except Exception as e:
            AppExc("XSQLCONX", [operation.opName, operation.org, e.message()])
    
    def close(self):
        try:
            self.connection.rollback()
            self.connection.close()
        finally:
            return
    
    def SqlExc(self, sql, exc):
        al.warn(str(exc))
        return AppExc("XSQL1", self.operation.opName, self.org, sql, exc.message())
    
    #######################################################################
    
    sqlonoff = "SELECT `org`, `ison`, `info` FROM `onoff`"
    def onoff(self):
        """
        Retourne le couple des deux entrées de la table ONOFF,
        a) celle du serveur lui-même z -> infoGen
        b) celle de l'organisation org -> infoOrg
        Quand une entrée n'est pas trouvée, le terme vaut {ison:-1, info:""}
        ONOFF est relu a minima 
        """
        sql = Provider.sqlonoff
        try:
            lapse = self.operation.stamp.epoch - Provider.onofStamp.epoch
            if lapse > Provider.lapseRefreshOnOff:
                with self.connection.cursor() as cursor:
                    cursor.execute(sql)
                    lst = cursor.fetchall()
                    d = {}
                    for x in lst:
                        d[x['org']] = {'ison':x['ison'], 'info':x.get('info', "")}
                    Provider.onoffDict = d
                    Provider.onoffStamp = self.operation.stamp
            d = Provider.onoffDict
            z = d.get("z", {'ison':-1, 'info':""})
            y = d.get(self.operation.org, {'ison':-1, 'info':""})
            return (z, y)
        except Exception as e:
            raise self.SqlExc(sql, e)
 
    #######################################################################
    """
    CREATE TABLE `prod_item` (
     `docid` varchar(128) NOT NULL COMMENT 'identifiant du document',
     `clkey` varchar(128) NOT NULL COMMENT 'classe / identifiant de l''item',
     `version` bigint(20) NOT NULL COMMENT 'version de l''item, date-heure de dernière modification',
     `size` int(11) NOT NULL,
     `ctime` bigint(20) DEFAULT NULL,
     `dtime` bigint(20) DEFAULT NULL,
     `totalsize` int(11) DEFAULT NULL,
     `serial` varchar(4000) DEFAULT NULL COMMENT 'sérialiastion de l''item',
     `serialGZ` blob COMMENT 'sérialisation gzippée de l''item',
     PRIMARY KEY (`docid`,`clkey`) USING BTREE,
     UNIQUE KEY `version` (`docid`,`version`,`size`,`clkey`) USING BTREE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    
    Soit vc et dtc les version et dtime de la cible :  vc >= dtc  
        Soit vs et dts les version et dtime de la source : vs >= dts et vc >= vs  
        Soit vd et dtd les version et dtime du delta (finalement de la nouvelle archive) : vd = vs  
        Les items du delta contiennent toujours tous les items créés ou modifiés après vc  
        On peut avoir les successions temporelles suivantes :
        - cas 1 : vs vc dtc dts
            - dtd = dtc
            - items contient les suppressions entre dtc et vc
        - cas 2 : vs vc dts dtc
            - dtd = dts 
            - items contient les suppressions entre dts et vc 
        - cas 3 : vs dts vc dtc
            - dtd = dts
            - keys contient les clkeys des items modifiés après vc 
        - cas 4 : vc = 0 (dtc = 0)
            - dtd = dts
            - items contient les suppressions depuis dts, c'est à dire toutes
    
    """

    def clkey(row):
        clkey = row["clkey"]
        i = clkey.find("[")
        cl = clkey[i:]
        k = json.loads(clkey[:i])
        return (cl, tuple(k))

    def meta(row, cl):
        if cl == "hdr":
            return (row["version"], row["size"], row["ctime"], row["dtime"], row["totalSize"])
        else:
            return (row["version"], row["size"], row["ctime"])
        
    def content(row):
        try:
            s = row["serial"]
            if s is not None:
                return s
            b = row["serialGZ"]
            if b is None:
                return ""
            t = decompress(b)
            return t.decode("utf-8")
        except:
            return ""

    sqlitems1 = "SELECT `clkey`, `version`, `size`, `ctime`, `dtime`, `totalsize`, `serial`, `serialGZ` FROM "
    sqlitems2a = " WHERE `docid` = %s"
    sqlitems2b = " WHERE `docid` = %s and `clkey` = 'hdr[]'"
    sqlitems2c = " WHERE `docid` = %s and `version` > %s and `clkey` != 'hdr[]'"

    sqlitems3 = "SELECT `clkey` FROM "
    sqlitems3c = " WHERE `docid` = %s and `version` < %s and `clkey` != 'hdr[]' and `size` != 0"
    
    def getArchive(self, table, docid):
        """
        Construit une archive complète depuis la base
        """
        arch = DocumentArchive(table, docid, True)
        arch.docid = docid
        sql = Provider.sqlitems1 + self.org + "_item" + Provider.sqlitems2a
        try:
            nbItems = 0
            with self.connection.cursor() as cursor:
                nbItems = cursor.execute(sql, (docid,))
                lst = cursor.fetchall()
                for row in lst:
                    cl, keys = Provider.clkey(row)
                    itd = arch.descr.itemDescrByCode.get(cl, None)
                    if itd is None or (not itd.isSingleton and len(itd.keys) != len(keys)):
                        continue
                    meta = Provider.meta(row, cl)
                    if cl == "hdr":
                        arch.version = meta[0]
                        arch.ctime = meta[2]
                        arch.dtime = meta[3]
                        arch.totalSize = meta[4]
                    content = Provider.content(row)
                    arch.addItem(cl, keys, meta, content)
            if nbItems == 0:
                return (0, None)
            else:
                return (3, arch)
        except Exception as e:
            raise self.SqlExc(sql, e)

    def getHdr(self, table, docid, version):
        """
        Retourne hdr : nécessaire pour savoir si le document existe
        """
        arch = DocumentArchive(table, docid, False)
        arch.docid = docid
        sql = Provider.sqlitems1 + self.org + "_item" + Provider.sqlitems2b
        try:
            with self.connection.cursor() as cursor:
                if cursor.execute(sql, (docid, version)) == 0:
                    return None
                lst = cursor.fetchone()
                row = lst[0]
                meta = Provider.meta(row, "hdr")
                arch.version = meta[0]
                arch.dtime = meta[3]
                arch.totalSize = meta[4]
                content = Provider.content(row)
                return (meta, content)
        except Exception as e:
            raise self.SqlExc(sql, e)
    
    def getDelta(self, cible, isfull):
        """
        Construit l'archive delta du document table/docid pour la mise à jour de la cible
        retourne le tuple (statut, delta)
        statut vaut : 0-document inconnu, 1-cible à jour, 2:delta, 3:complet (pas delta)
        Avant lecture du hdr dans la base on ne connaît ni vs ni dts
        Les cas 1 et 2 peuvent se traiter en un select des items modifiés après dtc
        Le cas 3 nécessite un second select pour construire keys
        
        """
        if cible.version == 0:      # cas 4 traité à part
            return self.getArchive(cible.descr.table, cible.docid)
        
        hdr = self.getHdr(cible.descr.table, cible.docid)
        if hdr is None:
            return (0, None)

        m = hdr[0]
        vs = m[0]
        if vs <= cible.version:
            return (1, None)

        cts = m[2]
        if cible.ctime != cts:      # cas 4 traité à part, comme si cible n'avait rien
            return self.getArchive(cible.descr.table, cible.docid)
        
        arch = DocumentArchive(cible.descr.table, cible.docid, isfull)
        arch.docid = cible.docid
        arch.version = vs
        arch.totalSize = m[4]
        arch.ctime = cts
        arch.dtime = vs
        arch.addItem3("hdr", tuple(), m, hdr[1])
        
        if not isfull:  # cas traité à part : seul hdr
            return (2, arch)
        
        """
        - cas 1 : vs vc dtc dts
            - dtd = dtc
            - items contient les suppressions entre dtc et vc
        - cas 2 : vs vc dts dtc
            - dtd = dts 
            - items contient les suppressions entre dts et vc 
        - cas 3 : vs dts vc dtc
            - dtd = dts
            - keys contient les clkeys des items modifiés après vc 
        """
        dts = m[3]
        vc = cible.version
        dtc = cible.dtime
        dtd = dts
        cas = 3
        if vc <= dts:
            minv = vc
        elif dtc < dts:
            cas = 2
            minv = dts
        else:
            cas = 1
            dtd = dtc
            minv = dtc
        arch.dtime = dtd
        
        sql = Provider.sqlitems1 + self.org + "_item" + Provider.sqlitems2c
        try:
            with self.connection.cursor() as cursor:
                if cursor.execute(sql, (cible.docid, minv)) != 0:
                    lst = cursor.fetchall()
                    for row in lst:
                        cl, keys = Provider.clkey(row)
                        itd = arch.descr.itemDescrByCode.get(cl, None)
                        if itd is None or (not itd.isSingleton and len(itd.keys) != len(keys)):
                            continue
                        meta = Provider.meta(row, cl)
                        content = Provider.content(row)
                        v = meta[0]
                        inc = meta[0] != 0
                        if not inc:     # deleted : l'inclure ou non
                            if cas == 1:
                                inc = v > dtc and v < vc
                            elif cas == 2:
                                inc = v > dts and v < vc
                        if inc:    
                            arch.addItem(cl, keys, meta, content)
        except Exception as e:
            raise self.SqlExc(sql, e)
        
        if cas != 3:
            return (2, arch)
        
        # arch.keys est le set de toutes les clkeys modifiées avant vc et existantes
        arch.keys = set()
        sql = Provider.sqlitems3 + self.org + "_item" + Provider.sqlitems3c
        try:
            with self.connection.cursor() as cursor:
                if cursor.execute(sql, (cible.docid, vc)) != 0:
                    lst = cursor.fetchall()
                    for row in lst:
                        arch.keys.add(row["clkey"])
        except Exception as e:
            raise self.SqlExc(sql, e)
        return (2, arch)


    sqlpurge = "DELETE FROM "
    sqlpurgea = " WHERE `docid` = %s"

    def purge(self, descr, docid):
        """
        Purge les items d'un docid donné
        """
        names = list("item")
        for idx in descr.indexes:
            names.append(idx)
        
        for n in names:
            sql = Provider.sqlpurge + self.org + "_" + n + Provider.sqlpurgea
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(sql, (docid,))
            except Exception as e:
                raise self.SqlExc(sql, e)

    sqlitems4 = "SELECT `version` FROM "
    sqlitems4a = " WHERE `docid` = %s FOR UPDATE NOWAIT"

    def lockHdr(self, table, docid, version):
        """
        Check la version du hdr. Si version == 0, ne DOIT pas exister
        """
        sql = Provider.sqlitems4 + self.org + "_item" + Provider.sqlitems4a
        try:
            with self.connection.cursor() as cursor:
                if cursor.execute(sql, (docid,)) == 0:
                    if version == 0:
                        return
                    raise AppExc("XCV", self.operation.opName, self.org, table + "/" + docid)
                lst = cursor.fetchone()
                v = lst[0]["version"]
                if v >= version:
                    raise AppExc("XCV", self.operation.opName, self.org, table + "/" + docid)
                return
        except Exception as e:
            al.warn(str(e))
            raise AppExc("XCV", self.operation.opName, self.org, table + "/" + docid)
          
    def insertMetaAndContent(self, table, docid, u): # u keys, meta, content
        pass

    def updateMetaOnly(self, table, docid, u):
        pass
    
    def updateMetaAndDeleteContent(self, table, docid, u):
        pass
    
    def updateMetaAndContent(self, table, docid, u):
        pass
          
    def setSingleIndex(self, name, ird, docid, kns, cols, keys, val): # val LE tuple d'index
        pass
    
    def setMultipleIndex(self, name, ird, docid, kns, cols, keys, lval): # lval : list des tuples d'index
        pass
          
    def IUDitems(self, upd):
        for u in upd.updates.values():
            # c : 1:insert meta and content, 2:update meta only, 3:update meta and delete content, 4:update meta and content
            # u {"c":c, "cl":cl, "keys":keys, "meta":meta, "content":content}
            if u.c == 1:
                self.insertMetaAndContent(upd.table, upd.docid, u)
            elif u.c == 2:
                self.updateMetaOnlytable(upd.table, upd.docid, u)
            elif u.c == 3:
                self.updateMetaAndDeleteContent(upd.table, upd.docid, u)
            else:
                self.updateMetaAndContent(upd.table, upd.docid, u)
        
        for idx in upd.indexes.values():
            # {"name":n, "isList":idx.isList(), "kn":self._descr.keys, "cols":idx.columns, "updates":[]}
            # colomns : startswith("*") column of the list
            # updates : {"ird":ird, "keys":self._keys, "val": newv if ird != 3 else None}
            if idx.isList:
                for ui in idx.updates:
                    self.setMultipleIndex(idx.n, idx.ird, upd.docid, idx.kns, idx.cols, ui.keys, ui.val)
            else:
                for ui in idx.updates:
                    self.setSingleIndex(idx.n, idx.ird, upd.docid, idx.kns, idx.cols, ui.keys, ui.val)
        
    def validation(self, vl):
        """
        validation de tous les documents
        status : 0:deleted, 1:unmodified, 2:modified, 3:created 4:recreated (after deletion)
        upd = (list((table, docid)), list((table, docid)),list(upd),list(upd),list(upd))
        version
        """
        # lock des documents. Pour les created ne devaient pas exister, mais lock quand même
        self.connection.begin()
        
        for descr, docid in vl.upd[0]:
            self.lockHdr(descr.table, docid, vl.version)
        for descr, docid in vl.upd[1]:
            self.lockHdr(descr.table, docid, vl.version)
        for upd in vl.upd[2]:
            self.lockHdr(upd.table, upd.docid, vl.version)
        for upd in vl.upd[3]:
            self.lockHdr(upd.table, upd.docid, 0)
        for upd in vl.upd[4]:
            self.lockHdr(upd.table, upd.docid, vl.version)
        
        for descr, docid in vl.upd[0]:
            self.purge(descr, docid)
        for upd in vl.upd[4]:
            self.purge(upd.descr, docid)
            
        for upd in vl.upd[2]:
            self.IUDitems(upd)
        for upd in vl.upd[3]:
            self.IUDitems(upd)
        for upd in vl.upd[4]:
            self.IUDitems(upd)
        
        
