import pymysql.cursors
from settings import settings
import pymysqlpool
from root import al, dics, AppExc, Stamp
from document import DocumentArchive
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
        except:
            pass
        try:
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
        
    def contentarg(content):
        if content is None or len(content) == 0:
            return (None, None)
        if len(content) < 2000:
            return (content, None)
        b = content.encode("utf-8")
        return (None, compress(b))

    sqlitems1 = "SELECT `clkey`, `version`, `size`, `ctime`, `dtime`, `totalsize`, `serial`, `serialGZ` FROM "
    sqlitems2a = "_item WHERE `docid` = %s"
    sqlitems2b = "_item WHERE `docid` = %s and `clkey` = 'hdr[]'"
    sqlitems2c = "_item WHERE `docid` = %s and `version` > %s and `clkey` != 'hdr[]'"

    sqlitems3 = "SELECT `clkey` FROM "
    sqlitems3c = "_item WHERE `docid` = %s and `version` < %s and `clkey` != 'hdr[]' and `size` != 0"
    
    def getArchive(self, table, docid):
        """
        Construit une archive complète depuis la base
        """
        arch = DocumentArchive(table, docid, True)
        arch.docid = docid
        sql = Provider.sqlitems1 + self.org + "_" + table + Provider.sqlitems2a
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
        sql = Provider.sqlitems1 + self.org + "_" + table + Provider.sqlitems2b
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
    
    def getDelta(self, table, cible, isfull):
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
        
        sql = Provider.sqlitems1 + self.org + "_" + table + Provider.sqlitems2c
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
        sql = Provider.sqlitems3 + self.org + "_" + table + Provider.sqlitems3c
        try:
            with self.connection.cursor() as cursor:
                if cursor.execute(sql, (cible.docid, vc)) != 0:
                    lst = cursor.fetchall()
                    for row in lst:
                        arch.keys.add(row["clkey"])
        except Exception as e:
            raise self.SqlExc(sql, e)
        return (2, arch)

    ####################################################################################

    sqlpurge = "DELETE FROM "
    sqlpurgea = " WHERE `docid` = %s"

    def purge(self, descr, docid):
        """
        Purge les items d'un docid donné
        """
        names = list(descr.table + "_item")
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
    sqlitems4a = "_item WHERE `docid` = %s FOR UPDATE NOWAIT"

    def lockHdr(self, table, docid, version):
        """
        Check la version du hdr. Si version == 0, ne DOIT pas exister
        """
        sql = Provider.sqlitems4 + self.org + "_" + table + Provider.sqlitems4a
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
    
    """
    `docid` varchar(128) NOT NULL COMMENT 'identifiant du document',
     `clkey` varchar(128) NOT NULL COMMENT 'classe / identifiant de l''item',
     `version` bigint(20) NOT NULL COMMENT 'version de l''item, date-heure de dernière modification',
     `size` int(11) NOT NULL,
     `ctime` bigint(20) DEFAULT NULL,
     `dtime` bigint(20) DEFAULT NULL,
     `totalsize` int(11) DEFAULT NULL,
     `serial` varchar(4000) DEFAULT NULL COMMENT 'sérialiastion de l''item',
     `serialGZ` blob COMMENT 'sérialisation gzippée de l''item',

    """
    sqlitemins1 = "INSERT INTO "
    sqlitemins2 = "_item (`docid`, `clkey`, `version`, `size`, `ctime`, `dtime`, `totalsize`, `serial`, `serialGZ`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    sqlitemupd1 = "UPDATE "
    sqlitemupd2a = "_item SET `version` = %s, `size` = %s, `ctime` = %s, `dtime` = %, `totalsize` = %s WHERE `docid` = %s AND `clkey` = %s"
    sqlitemupd2b = "_item SET `version` = %s, `size` = %s, `ctime` = %s, `dtime` = %, `totalsize` = %s, `serial` = %s, `serialGZ` = %s WHERE `docid` = %s AND `clkey` = %s"
    
    def _dclk(self, docid, u):
        return (docid, u["cl"] + json.dumps(u["keys"]))
    
    def _meta(self, u):
        meta = u["meta"]
        return meta if len(meta) == 5 else meta + (0, 0)
    
    def _sql(self, sql, args):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, args)
        except Exception as e:
            raise self.SqlExc(sql, e)
    
    def _insidx(self, name, cols, kns, docid, keys, lval):
        t = list("INSERT INTO " + self.org + "_" + name +"(")
        for col in cols:
            t.append("`" + (col if not col.startswith("*") else col[1:]) + "`,")
        t.append("`docid`")
        if len(kns) > 0:
            for k in kns:
                t.append(",`" + k + "`")
        t.append(") VALUES ")
        t2 = []
        for i in range(len(lval)):
            t3 = []
            for j in range(len(cols) + len(kns) + 1):
                t3.append("%s")
            t2.append("(" + ",".join(t3) + ")")
        t.append(",".join(t2))
        sql = "".join(t)
        args = list()
        for val in lval:
            for x in val:
                args.append(x)
            args.append(docid)
            if len(keys) > 0:
                for k in keys:
                    args.append(k)
        self._sql(sql, args)
        
    def _updidx(self, name, cols, kns, docid, keys, val):
        t = list("UPDATE " + self.org + "_" + name +" SET ")
        t2 = []
        for col in cols:
            t2.append("`" + (col if not col.startswith("*") else col[1:]) + "`=%s")
        t.append(",".join(t2))
        t.append(" WHERE `docid`=%s")
        for kn in kns:
            t.append(" and `" + kn + "` = %s")
        sql = "".join(t)
        args = list()
        for v in val:
            args.append(v)
        args.append(docid)
        for kv in keys:
            args.append(kv)
        self._sql(sql, args)
            
    def _delidx(self, name, kns, docid, keys):
        t = list("DELETE FROM " + self.org + "_" + name +" WHERE `docid`=%s")
        for kn in kns:
            t.append(" and `" + kn + "` = %s")
        sql = "".join(t)
        args = list()
        args.append(docid)
        for kv in keys:
            args.append(kv)
        self._sql(sql, args)
                        
    def IUDitems(self, upd):
        nbi = 0
        for u in upd.updates.values():
            nbi += 1
            # c : 1:insert meta and content, 2:update meta only, 3:update meta and delete content, 4:update meta and content
            # u {"c":c, "cl":cl, "keys":keys, "meta":meta, "content":content}
            if u["c"] == 1:
                args = self._dclk(upd.docid, u) + self._meta(u) + Provider.contentarg(u["content"])
                sql = Provider.sqlitemins1 + self.org + "_" + upd.table + Provider.sqlitemins2
            elif u["c"] == 2:
                args = self._meta(u) + self._dclk(upd.docid, u)
                sql = Provider.sqlitemupd1 + self.org + "_" + upd.table + Provider.sqlitemupd2a
            elif u["c"] == 3:
                args = self._meta(u) + (None, None) + self._dclk(upd.docid, u)      
                sql = Provider.sqlitemupd1 + self.org + "_" + upd.table + Provider.sqlitemupd2b
            else:
                args = self._meta(u) + Provider.contentarg(u["content"]) + self._dclk(upd.docid, u)
                sql = Provider.sqlitemupd1 + self.org + "_" + upd.table + Provider.sqlitemupd2b
            self._sql(sql, args)
        
        for idx in upd.indexes.values():
            # {"name":n, "isList":idx.isList(), "kn":self._descr.keys, "cols":idx.columns, "updates":[]}
            # colomns : startswith("*") column of the list
            # updates : {"ird":ird, "keys":self._keys, "val": newv if ird != 3 else None}
            if idx.isList:
                for ui in idx.updates:
                    nbi += 1
                    if idx["ird"] == 1:        # insert
                        self._insidx(idx["n"], idx["cols"], idx["kns"], upd.docid, ui["keys"], ui["val"])
                    elif idx["ird"] == 2:      # replace
                        self._delidx(idx["n"], idx["kns"], upd.docid, ui["keys"])
                        self._insidx(idx["n"], idx["cols"], idx["kns"], upd.docid, ui["keys"], ui["val"])
                    else:               # delete
                        self._delidx(idx["n"], idx["kns"], upd.docid, ui["keys"])
            else:
                for ui in idx.updates:
                    nbi += 1
                    if idx["ird"] == 1:        # insert
                        self._insidx(idx["n"], idx["cols"], idx["kns"], upd.docid, ui["keys"], (ui["val"],))
                    elif idx["ird"] == 2:      # replace
                        self._updidx(idx["n"], idx["cols"], idx["kns"], upd.docid, ui["keys"], ui["val"])
                    else:               # delete
                        self._delidx(idx["n"], idx["kns"], upd.docid, ui["keys"])
            return nbi
        
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
            
        nbi = 0
        for i in range(4):
            nbi += len(vl.upd[i])
        
        for descr, docid in vl.upd[0]:
            self.purge(descr, docid)
        for upd in vl.upd[4]:
            self.purge(upd.descr, docid)
            
        for upd in vl.upd[2]:
            nbi += self.IUDitems(upd)
        for upd in vl.upd[3]:
            nbi += self.IUDitems(upd)
        for upd in vl.upd[4]:
            nbi += self.IUDitems(upd)
        
        self.connection.commit()
        return nbi


class FakeOp:
    def __init__(self):
        self.org = "prod"
        self.opName = "test"
        self.stamp = Stamp.fromEpoch(Stamp.epochNow())
        
def test():
    op = FakeOp()
    provider = Provider(op)
    return provider.onoff()

z, y = test()
print("OK")


    