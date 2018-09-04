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
        s = str(exc)
        al.warn(s)
        return AppExc("XSQL1", [self.operation.opName, self.org, sql, s])
    
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
    CREATE TABLE `prod_compte` (
     `itkey` varchar(500) NOT NULL,
     `version` bigint(20) NOT NULL,
     `dtcas` bigint(20) NOT NULL,
     `size` int(11) NOT NULL,
     `serial` varchar(4000) DEFAULT NULL,
     `serialGZ` blob DEFAULT NULL,
     PRIMARY KEY (`itkey`, `version`, `dtcas`, `size`) USING BTREE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    
    Soit vc et dtc les version et dtime de la cible :  vc >= dtc  
        Soit vs et dts les version et dtime de la source : vs >= dts et vc >= vs  
        Soit vd et dtd les version et dtime du delta (finalement de la nouvelle archive) : vd = vs  
        Les items du delta contiennent toujours tous les items créés ou modifiés après vc  
        On peut avoir les successions temporelles suivantes :
        - cas 1 : vb va dtb
            - items contient les suppressions entre dtb et vb et les créations / modifications / suppressions après va
        - cas 2 : vb dtb va
            - items contient les items créés / modifiés / supprimés après va
            - keys contient les clkeys des items modifiés et existants avant va 
    """

    def clkeys(row):
        s = row["itkey"]
        i = s.find(" ")
        if i == -1:
            return ("hdr", ())
        j = s.find("[", i)
        if j == -1:
            return (s[i + 1:], ())
        k = json.loads(s[j:])
        return (s[i + 1:j], tuple(k))
    
    def meta(row):
        return (row["version"], row["dtcas"], row["size"])
    
    def content(row):
        try:
            s = row["serial"]
            if s is not None:
                return s
            b = row["serialgz"]
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

    sqlitems1a = "SELECT `version`, `dtcas`, `size`, `serial`, `serialgz` FROM "
    sqlitems1b = "SELECT `itkey`, `version`, `dtcas`, `size`, `serial`, `serialgz` FROM "
    sqlitems1c = "SELECT `clkey` FROM "
    sqlitems1d = "SELECT `itkey`, `version`, `dtcas`, `size`, IF(`dtcas` < 0, NULL, `serial`) AS `serial`, IF(`dtcas` < 0, NULL, `serialgz`) AS `serialgz` FROM "
    
    sqlitems2a = " WHERE `itkey` = %s"
    sqlitems2b = " WHERE `itkey` >= %s and `ìtkey` <= %s"
    sqlitems2c = " WHERE `itkey` >= %s and `ìtkey` <= %s AND `version` <= %s AND `dtcas` >= 0"
    sqlitems2d = " WHERE `itkey` >= %s and `ìtkey` <= %s AND `version` > %s"
    
    def getArchive(self, table, docid, isfull):
        """
        Construit une archive depuis la base
        """
        arch = DocumentArchive(table, docid, isfull)
        arch.docid = docid
        sql = Provider.sqlitems1b + self.org + "_" + table + (Provider.sqlitems2b if isfull else Provider.sqlitems2a)
        try:
            nbItems = 0
            with self.connection.cursor() as cursor:
                nbItems = cursor.execute(sql, (docid, docid + " zzzzzzzz"))
                lst = cursor.fetchall()
                for row in lst:
                    cl, keys = Provider.clkeys(row)
                    itd = arch.descr.itemDescrByCode.get(cl, None)
                    if itd is None or (not itd.isSingleton and len(itd.keys) != len(keys)):
                        continue
                    arch.addItem(cl, keys, Provider.meta(row), Provider.content(row))
            if nbItems == 0:
                return (0, None)
            else:
                return (3, arch)
        except Exception as e:
            raise self.SqlExc(sql, e)

    def getHdr(self, table, docid):
        """
        Retourne hdr : nécessaire pour savoir si le document existe
        """
        arch = DocumentArchive(table, docid, False)
        arch.docid = docid
        sql = Provider.sqlitems1a + self.org + "_" + table + Provider.sqlitems2a
        try:
            with self.connection.cursor() as cursor:
                if cursor.execute(sql, (docid,)) == 0:
                    return None
                row = cursor.fetchone()
                arch.addItem("hdr", (), Provider.meta(row), Provider.content(row))
            return arch
        except Exception as e:
            raise self.SqlExc(sql, e)
    
    def getDelta(self, old, isfull):
        """
        Construit l'archive delta du document table/docid pour la mise à jour depuis old
        retourne le tuple (statut, delta)
        statut vaut : 0-document inconnu, 1-old à jour, 2:delta (old -> base)
        Lecture du hdr pour savoir si le document existe et avoir sa version dtcas
        - cas not wk : vb va dtb
            - items contient les suppressions entre dtb et vb et les créations / modifications / suppressions après va
        - cas wk : vb dtb va
            - items contient les items créés / modifiés / supprimés après va
            - keys contient les clkeys des items modifiés et existants avant va 
        
        """
        arch = self.getHdr(old.descr.table, old.docid)
        if arch is None:
            return (0, None)

        if arch.version <= old.version:
            return (1, None)
        
        if not isfull:  # cas traité à part : seul hdr
            return (2, arch)
        
        wk = old.version < arch.dtcas
        minv = arch.dtcas if wk else old.version
        sql = Provider.sqlitems1d + self.org + "_" + old.descr.table + Provider.sqlitems2d
        try:
            with self.connection.cursor() as cursor:
                if cursor.execute(sql, (old.docid, old.docid + " zzzzzzzz", minv)) != 0:
                    lst = cursor.fetchall()
                    for row in lst:
                        cl, keys = Provider.clkeys(row)
                        itd = arch.descr.itemDescrByCode.get(cl, None)
                        if itd is None or (not itd.isSingleton and len(itd.keys) != len(keys)):
                            continue
                        meta = Provider.meta(row)
                        if not wk:
                            if meta[1] < 0: # del
                                arch.addItem(cl, keys, meta, None)
                            else: # cre maj
                                if meta[0] > old.version:
                                    arch.addItem(cl, keys, meta, Provider.content(row))
                        else:
                            arch.addItem(cl, keys, meta, Provider.content(row))
        except Exception as e:
            raise self.SqlExc(sql, e)
        
        if wk:
            # keys contient les clkeys des items modifiés et existants avant va
            arch.keys = set()
            sql = Provider.sqlitems1c + self.org + "_" + old.descr.table + Provider.sqlitems2c
            try:
                with self.connection.cursor() as cursor:
                    if cursor.execute(sql, (old.docid, old.docid + " zzzzzzzz", minv)) != 0:
                        lst = cursor.fetchall()
                        for row in lst:
                            arch.keys.add(Provider.clkeys(row))
            except Exception as e:
                raise self.SqlExc(sql, e)
        return (2, arch)

    ####################################################################################

    sqlpurge = "DELETE FROM "

    def purge(self, descr, docid):
        """
        Purge les items d'un docid donné
        """
        names = list(descr.table)
        for idx in descr.indexes:
            names.append(idx)
        
        for n in names:
            sql = Provider.sqlpurge + self.org + "_" + n + Provider.sqlitems2b
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(sql, (docid, docid + " zzzzzzzz"))
            except Exception as e:
                raise self.SqlExc(sql, e)

    sqlitems4 = "SELECT `version` FROM "
    sqlitems4a = " WHERE `docid` = %s FOR UPDATE NOWAIT"

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
                    raise AppExc("XCV", [self.operation.opName, self.org, table + "/" + docid])
                row = cursor.fetchone()
                v = row["version"]
                if v >= version:
                    raise AppExc("XCV", [self.operation.opName, self.org, table + "/" + docid])
                return
        except Exception as e:
            al.warn(str(e))
            raise AppExc("XCV", [self.operation.opName, self.org, table + "/" + docid])
    
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
    sqlitemins2 = " (`docid`, `clkey`, `version`, `size`, `ctime`, `dtime`, `totalsize`, `serial`, `serialGZ`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    sqlitemupd1 = "UPDATE "
    sqlitemupd2a = " SET `version` = %s, `size` = %s, `ctime` = %s, `dtime` = %s, `totalsize` = %s WHERE `docid` = %s AND `clkey` = %s"
    sqlitemupd2b = " SET `version` = %s, `size` = %s, `ctime` = %s, `dtime` = %s, `totalsize` = %s, `serial` = %s, `serialGZ` = %s WHERE `docid` = %s AND `clkey` = %s"
    
    def _dclk(self, docid, u):
        return (docid, u.cl + json.dumps(u.keys))
    
    def _meta(self, u):
        meta = u.meta
        return meta if len(meta) == 5 else meta + (0, 0)
    
    def _sql(self, sql, args):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, args)
        except Exception as e:
            raise self.SqlExc(sql, e)
    
    def _insidx(self, name, cols, kns, docid, keys, lval):
        t = list("INSERT INTO " + self.org + "_" + name +" (")
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
            t2.append("`" + (col if not col.startswith("*") else col[1:]) + "` = %s")
        t.append(",".join(t2))
        t.append(" WHERE `docid` = %s")
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
            if u.c == 1:
                args = self._dclk(upd.docid, u) + self._meta(u) + Provider.contentarg(u.content)
                sql = Provider.sqlitemins1 + self.org + "_" + upd.table + Provider.sqlitemins2
            elif u.c == 2:
                args = self._meta(u) + self._dclk(upd.docid, u)
                sql = Provider.sqlitemupd1 + self.org + "_" + upd.table + Provider.sqlitemupd2a
            elif u.c == 3:
                args = self._meta(u) + (None, None) + self._dclk(upd.docid, u)      
                sql = Provider.sqlitemupd1 + self.org + "_" + upd.table + Provider.sqlitemupd2b
            else:
                args = self._meta(u) + Provider.contentarg(u.content) + self._dclk(upd.docid, u)
                sql = Provider.sqlitemupd1 + self.org + "_" + upd.table + Provider.sqlitemupd2b
            self._sql(sql, args)
        
        for idxUpd in upd.indexes.values():
            n = idxUpd.idx.name
            cols = idxUpd.idx.columns
            kns = idxUpd.idx.kns
            # {"name":n, "isList":idx.isList(), "kn":self._descr.keys, "cols":idx.columns, "updates":[]}
            # colomns : startswith("*") column of the list
            # updates : {"ird":ird, "keys":self._keys, "val": newv if ird != 3 else None}
            if idxUpd.idx.isList():
                for ikv in idxUpd.updates:
                    nbi += 1
                    if ikv.ird == 1:        # insert
                        self._insidx(n, cols, kns, upd.docid, ikv.keys, ikv.val)
                    elif ikv.ird == 2:      # replace
                        self._delidx(n, kns, upd.docid, ikv.keys)
                        self._insidx(n, cols, kns, upd.docid, ikv.keys, ikv.val)
                    else:               # delete
                        self._delidx(n, kns, upd.docid, ikv.keys)
            else:
                for ikv in idxUpd.updates:
                    nbi += 1
                    if ikv.ird == 1:        # insert
                        self._insidx(n, cols, kns, upd.docid, ikv.keys, (ikv.val,))
                    elif ikv.ird == 2:      # replace
                        self._updidx(n, cols, kns, upd.docid, ikv.keys, ikv.val)
                    else:               # delete
                        self._delidx(n, cols, kns, upd.docid, ikv.keys)
        return nbi
        
    def validate(self, vl):
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

    """
    - t0 : volume reçue dans la requête.
    - t1 : volume sorti dans la réponse.
    - t2 : nombre de documents lus.
    - t3 : nombre d'items lus.
    - t4 : nombre d'items écrits.
    - t5 : nombre de tâches créées / mise à jour.
    - t6 : nombre de secondes du traitement.
    - t7 : 0
    - f0 à f7
    - table
    - tktid
    - version
    - state
    """
    
    sqlacctkt = "INSERT INTO acctkt (`org`, `doctbl`, `tktid`, `version`, `state`, \
    `t0`, `t1`, `t2`, `t3`, `t4`, `t5`, `t6`, `t7`, \
    `f0`, `f1`, `f2`, `f3`, `f4`, `f5`, `f6`, `f7`) \
     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE \
     `version`= VALUES(`version`), `state` = VALUES(`state`), \
     `t0` = `t0` + VALUES(`t0`), `t1` = `t1` + VALUES(`t1`), `t2` = `t2` + VALUES(`t2`), `t3` = `t3` + VALUES(`t3`), \
     `t4` = `t4` + VALUES(`t4`), `t5` = `t5` + VALUES(`t5`), `t6` = `t6` + VALUES(`t6`), `t7` = `t7` + VALUES(`t7`), \
     `f0` = `f0` + VALUES(`f0`), `f1` = `f1` + VALUES(`f1`), `f2` = `f2` + VALUES(`f2`), `f3` = `f3` + VALUES(`f3`), \
     `f4` = `f4` + VALUES(`f4`), `f5` = `f5` + VALUES(`f5`), `f6` = `f6` + VALUES(`f6`), `f7` = `f7` + VALUES(`f7`)"
    def setAccTkt(self, tk):
        args = [tk.org, tk.table, tk.tktid, tk.v, tk.state]
        for t in tk.t:
            args.append(t)
        for f in tk.f:
            args.append(f)
        try:
            self.connection.begin()
            with self.connection.cursor() as cursor:
                cursor.execute(Provider.sqlacctkt, args)
            self.connection.commit()
        except Exception as e:
            raise self.SqlExc(Provider.sqlacctkt, e)

    def searchInIndex(self, code, rc, sc, startCols):
        sqlx = ["SELECT "]
        i = 0
        for c in rc:
            sqlx.append(("`" if i == 0 else ", `") + (c if not c.startswith("*") else c[1:]) + "`")
            i += 1
        sqlx.append("FROM " + self.org + "_" + code + " WHERE ")
        i = 0
        for c in sc:
            sqlx.append((" `" if i == 0 else " and `") + (c if not c.startswith("*") else c[1:]) + "` = %s")
            i += 1
        sql = "".join(sqlx)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, startCols)
                return cursor.fetchall()
        except Exception as e:
            raise self.SqlExc(sql, e)

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


    