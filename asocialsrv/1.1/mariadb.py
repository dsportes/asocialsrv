from pymysql import Connection, connect, cursors
from settings import settings
import pymysqlpool
from root import al, dics, AppExc, Stamp
from document import DocumentArchive, DocumentDescr, LocalUpdate, Update, ValidationList, AccTkt, Meta, ClKeys
import json
from gzip import compress, decompress
from typing import Dict, Tuple, Any, Iterable
    
class OnOff:
    def __init__(self, ison:int, info:str) -> OnOff:
        self.ison = ison
        self.info = info
Row = Dict[str, Any]

class Provider:
    dics.set("fr", "XSQL1", "Incident d'accès à la base de données. Opération:{0} Org:{1} SQL:{2} Cause:{3}")
    dics.set("fr", "XCV", "Trop de contention détectée à la validation. Opération:{0} Org:{1} Document:{2} ")
    
    def _create_conn() -> Connection:
        return connect(host=settings.db['host'], db=settings.db['db'], user=settings.db['user'], password=settings.db['password'],
                             charset='utf8mb4', cursorclass=cursors.DictCursor)

    pool = pymysqlpool.Pool(create_instance=_create_conn, max_count=settings.db['poolSize'], timeout=settings.db['timeOut'])
    lapseRefreshOnOff = 300000
    onofStamp = Stamp()
    onoffrep = {}
    
    def __init__(self, operation) -> Provider:
        try:
            self.connection = Provider.pool.get()
            self.operation = operation
            self.org = operation.org
        except Exception as e:
            AppExc("XSQLCONX", [operation.opName, operation.org, e.message()])
    
    def close(self) -> None:
        try:
            self.connection.rollback()
        except:
            pass
        try:
            self.connection.close()
        finally:
            return
    
    def SqlExc(self, sql, exc) -> AppExc:
        s = str(exc)
        al.warn(s)
        return AppExc("XSQL1", [self.operation.opName, self.org, sql, s])
    
    #######################################################################
    
    
    sqlonoff = "SELECT `org`, `ison`, `info` FROM `onoff`"
    def onoff(self) -> Tuple(OnOff, OnOff):
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
                        d[x['org']] = OnOff(x['ison'], x.get('info', ""))
                    Provider.onoffDict = d
                    Provider.onoffStamp = self.operation.stamp
            d = Provider.onoffDict
            z = d.get("z", OnOff(-1, ""))
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
    """

    def clkeys(row: Row) -> ClKeys:
        s = row["itkey"]
        i = s.find(" ")
        if i == -1:
            return ("hdr", ())
        j = s.find("[", i)
        if j == -1:
            return (s[i + 1:], ())
        k = json.loads(s[j:])
        return (s[i + 1:j], tuple(k))
    
    def meta(row: Row) -> Meta:
        return (row["version"], row["dtcas"], row["size"])
    
    def content(row: Row) -> str:
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
        
    def contentarg(content:str) -> Tuple[str, bytes]:
        if content is None or len(content) == 0:
            return (None, None)
        if len(content) < 2000:
            return (content, None)
        b = content.encode("utf-8")
        return (None, compress(b))

    sqlitems1a = "SELECT `itkey`, `version`, `dtcas`, `size`, `serial`, `serialgz` FROM "
    sqlitems1b = "SELECT `version`, `dtcas`, `size`, IF(`version` = %s, NULL, `serial`) AS `serial`, IF(`version` = %s, NULL, `serialgz`) AS `serialgz` FROM "
    sqlitems1c = "SELECT `clkey` FROM "
    sqlitems1g = "SELECT `version` FROM "

    sqlitems2a = " WHERE `itkey` = %s"
    sqlitems2b = " WHERE `itkey` >= %s and `ìtkey` <= %s"
    sqlitems2c = " WHERE `itkey` = %s  AND `version` > %s"
    sqlitems2d = " WHERE `itkey` >= %s and `ìtkey` <= %s AND ((`version` > %s AND `dtcas` >= 0) OR (`version` > %s AND `dtcas` < 0))"
    sqlitems2e = " WHERE `itkey` >= %s and `ìtkey` <= %s AND `version` > %s AND `dtcas` >= 0"
    sqlitems2f = " WHERE `itkey` >= %s and `ìtkey` <= %s AND `version` < %s AND `dtcas` >= 0"
    
    def getArchive(self, table:str, docid:str, isfull) -> DocumentArchive:
        """
        Construit une archive depuis la base uniquement
        """
        sql = Provider.sqlitems1a + self.org + "_" + table + (Provider.sqlitems2b if isfull else Provider.sqlitems2a)
        try:
            with self.connection.cursor() as cursor:
                if cursor.execute(sql, (docid, docid + " zzzzzzzz")) == 0:
                    return None
                arch = DocumentArchive(table, docid, isfull)
                lst = cursor.fetchall()
                for row in lst:
                    cl, keys = Provider.clkeys(row)
                    itd = arch.descr.itemDescrByCode.get(cl, None)
                    if itd is None or (not itd.isSingleton and len(itd.keys) != len(keys)):
                        continue
                    arch.addItem(cl, keys, Provider.meta(row), Provider.content(row))
                return arch
        except Exception as e:
            raise self.SqlExc(sql, e)

    def _getHdr(self, table:str, docid:str, version:int) -> Tuple[Meta, str]:
        """
        Retourne hdr : nécessaire pour savoir si le document existe
        """
        sql = Provider.sqlitems1b + self.org + "_" + table + Provider.sqlitems2c
        try:
            with self.connection.cursor() as cursor:
                if cursor.execute(sql, (version, version, docid, version)) == 0:
                    return (None, None)
                row = cursor.fetchone()
                return (Provider.meta(row), Provider.content(row))
        except Exception as e:
            raise self.SqlExc(sql, e)
    
    def getUpdatedArchive(self, old:DocumentArchive) -> DocumentArchive:
        """
        Construit one nouvelle archive du document table/docid depuis old, une ancienne archive de version va) et l'état de la base de version vb
        - **la nouvelle archive inclut toujours la liste des items créés ou modifiés entre `va` et `vb`** mais d'autres items sont à intégrer :
            - à) les items existants dans l'archive antérieure, non modifiés depuis `va` et toujours existants.
            - b) les items supprimés depuis `dtcas` et dont certains sont déjà cités dans l'archive antérieure.
        - ***Deux situations se présentent***
            - **cas 1 : va >= dtcas de la base**. Toutes les suppressions d'items permettant de construire la nouvelle archive depuis l'ancienne sont disponibles : 
                - *on tire de la base et on inclut dans la nouvelle archive: a) les items existants créés / modifiés après `va`, b) les items détruits postérieurement à `dtcas`*
                - les items de l'archive antérieure *qui ne figurent pas* dans la nouvelle archive sont examinés :
                    - ceux marqués existants et pas déjà réfrencés dans la nouvelle archive, sont référencés dans la nouvelle archive.
            - **cas 2 : va < dtcas de la base**. La base ne dispose plus de la trace de toutes les suppressions intervenues entre `va` et `dtcas` de la base. 
                - *on tire de la base : a) les items existants créés / modifiés après `va` qui sont intégrés dans la nouvelle archive, b) la liste `lk` des clés des items existants (créés / modifiés) avant `va`*. 
                - les items de l'archive antérieure qui ne figurent pas dans la nouvelle archive sont examinés :
                    - ceux marqués existants et dont la clé figure dans `lk`, sont référencés dans la nouvelle archive.
                    - ceux marqués détruits, ne sont référencés dans la nouvelle archive que si leur destruction est postérieure à `dtcas`.
        
        """
        meta, content = self._getHdr(old.descr.table, old.docid, old.version)
        if meta is None:    # le document n'existe pas
            return None
        if meta[0] == old.version:  # le document n'a pas changé
            return old
            
        arch = DocumentArchive(old.descr.table, old.docid, old.isfull)  # nouvelle archive
        arch.addItem("hdr", (), meta, content)
                
        if old.version >= arch.dtcas:
            # on tire de la base et on inclut dans la nouvelle archive: 
            # a) les items existants créés / modifiés après `va`
            # b) les items détruits postérieurement à `dtcas`
            sql = Provider.sqlitems1a + self.org + "_" + old.descr.table + Provider.sqlitems2d
            try:
                with self.connection.cursor() as cursor:
                    if cursor.execute(sql, (old.docid, old.docid + " zzzzzzzz", old.version, arch.dtcas)) != 0:
                        lst = cursor.fetchall()
                        for row in lst:
                            cl, keys = Provider.clkeys(row)
                            itd = arch.descr.itemDescrByCode.get(cl, None)
                            if itd is None or (not itd.isSingleton and len(itd.keys) != len(keys)):
                                continue
                            meta = Provider.meta(row)
                            if meta[1] < 0:     # suppression
                                arch.addItem(cl, keys, meta, None)
                            else:               # creation maj
                                arch.addItem(cl, keys, meta, Provider.content(row))
                # les items de l'archive antérieure *qui ne figurent pas* dans la nouvelle archive sont examinés :
                # ceux marqués existants et pas déjà référencés dans la nouvelle archive, sont référencés dans la nouvelle archive.
                for k in old.clkeys():
                    if not k in arch.items:
                        meta, content = old.items[k]
                        if meta[1] >= 0:
                            arch.addItem(k[0], k[1], meta, content)
                return arch
            except Exception as e:
                raise self.SqlExc(sql, e)

        # on tire de la base : 
        # a) les items existants créés / modifiés après `va`, 
        sql = Provider.sqlitems1a + self.org + "_" + old.descr.table + Provider.sqlitems2e
        try:
            with self.connection.cursor() as cursor:
                if cursor.execute(sql, (old.docid, old.docid + " zzzzzzzz", old.version)) != 0:
                    lst = cursor.fetchall()
                    for row in lst:
                        cl, keys = Provider.clkeys(row)
                        itd = arch.descr.itemDescrByCode.get(cl, None)
                        if itd is None or (not itd.isSingleton and len(itd.keys) != len(keys)):
                            continue
                        arch.addItem(cl, keys, Provider.meta(row), Provider.content(row))
        except Exception as e:
            raise self.SqlExc(sql, e)
        
        # b) la liste `lk` des clés des items existants (créés / modifiés) avant `va`*. 
        lk = set()
        sql = Provider.sqlitems1c + self.org + "_" + old.descr.table + Provider.sqlitems2f
        try:
            with self.connection.cursor() as cursor:
                if cursor.execute(sql, (old.docid, old.docid + " zzzzzzzz", old.version)) != 0:
                    lst = cursor.fetchall()
                    for row in lst:
                        lk.add(Provider.clkeys(row))
        except Exception as e:
            raise self.SqlExc(sql, e)
        # Les items de l'archive antérieure qui ne figurent pas dans la nouvelle archive sont examinés :
        # - ceux marqués existants et dont la clé figure dans `lk`, sont référencés dans la nouvelle archive.
        # - ceux marqués détruits, ne sont référencés dans la nouvelle archive que si leur destruction est postérieure à `dtcas`.
        for k in old.clkeys():
            meta, content = old.items[k]
            if meta[1] >= 0:
                if k in lk:
                    arch.addItem(k[0], k[1], meta, content)
            else:
                if meta[0] > arch.dtcas:
                    arch.addItem(k[0], k[1], meta, content)
        return arch

    ####################################################################################

    def _purge(self, descr:DocumentDescr, docid:str) -> None:
        """
        Purge les items d'un docid donné
        """
        sql = "DELETE FROM " + self.org + "_" + descr.table + Provider.sqlitems2b
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (docid, docid + " zzzzzzzz"))
        except Exception as e:
            raise self.SqlExc(sql, e)       
        for n in descr.indexes:
            sql = "DELETE FROM " + self.org + "_" + n + " WHERE `docid` = %s"
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(sql, (docid,))
            except Exception as e:
                raise self.SqlExc(sql, e)

    def _lockHdr(self, table:str, docid:str, version:int) -> None:
        """
        Check la version du hdr. Si version == 0, ne DOIT pas exister
        """
        sql = Provider.sqlitems1g + self.org + "_" + table + " WHERE `docid` = %s FOR UPDATE NOWAIT"
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
        
    def _itkey(self, docid:str, u:LocalUpdate) -> str:
        return docid if u.cl == "hdr" else (docid + " " + u.cl + ("" if len(u.keys) == 0 else json.dumps(u.keys)))
        
    def _sql(self, sql:str, args:Iterable) -> None:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, args)
        except Exception as e:
            raise self.SqlExc(sql, e)
    
    def _insidx(self, name:str, cols:Iterable[str], kns:Tuple[str], docid:str, keys:Iterable, lval:Iterable) -> None:
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
        
    def _updidx(self, name:str, cols:Iterable[str], kns:Tuple[str], docid:str, keys:Iterable, val) -> None:
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
            
    def _delidx(self, name:str, kns:Tuple[str], docid:str, keys:Iterable) -> None:
        t = list("DELETE FROM " + self.org + "_" + name +" WHERE `docid`= %s")
        for kn in kns:
            t.append(" and `" + kn + "` = %s")
        sql = "".join(t)
        args = list()
        args.append(docid)
        for kv in keys:
            args.append(kv)
        self._sql(sql, args)
                        
    sqlitemins = " (`itkey`, `version`, `dtcas`, `size`, `serial`, `serialGZ`) VALUES (%s, %s, %s, %s, %s, %s)"
    sqlitemupd2 = " SET `version` = %s, `dtcas` = %s, `size` = %s WHERE `itkey` = %s"

    def _IUDitems(self, upd:Update) -> int:
        nbi = 0
        for u in upd.updates.values():
            nbi += 1
            # c : 1:insert meta and content, 2:update meta only, 3:update meta and delete content, 4:update meta and content
            # u {"c":c, "cl":cl, "keys":keys, "meta":meta, "content":content}
            if u.c == 1:
                args = self._itkey(upd.docid, u) + u.meta + Provider.contentarg(u.content)
                sql = "INSERT INTO " + self.org + "_" + upd.table + Provider.sqlitemins
            elif u.c == 2:
                args = u.meta + self._itkey(upd.docid, u)
                sql = "UPDATE " + self.org + "_" + upd.table + Provider.sqlitemupd
            elif u.c == 3:
                args = u.meta + (None, None) + self._itkey(upd.docid, u)      
                sql = "UPDATE " + self.org + "_" + upd.table + Provider.sqlitemupd
            else:
                args = u.meta + Provider.contentarg(u.content) + self._itkey(upd.docid, u)
                sql = "UPDATE " + self.org + "_" + upd.table + Provider.sqlitemupd
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
        
    def validate(self, vl:ValidationList) -> int:
        """
        validation de tous les documents
        status : 0:deleted, 1:unmodified, 2:modified, 3:created 4:recreated (after deletion)
        upd = (list((table, docid)), list((table, docid)),list(upd),list(upd),list(upd))
        version
        """
        # lock des documents. Pour les created ne devaient pas exister, mais lock quand même
        self.connection.begin()
        
        for descr, docid in vl.upd[0]:
            self._lockHdr(descr.table, docid, vl.version)
        for descr, docid in vl.upd[1]:
            self._lockHdr(descr.table, docid, vl.version)
        for upd in vl.upd[2]:
            self._lockHdr(upd.table, upd.docid, vl.version)
        for upd in vl.upd[3]:
            self._lockHdr(upd.table, upd.docid, 0)
        for upd in vl.upd[4]:
            self._lockHdr(upd.table, upd.docid, vl.version)
            
        nbi = 0
        for i in range(4):
            nbi += len(vl.upd[i])
        
        for descr, docid in vl.upd[0]:
            self._purge(descr, docid)
        for upd in vl.upd[4]:
            self._purge(upd.descr, docid)
            
        for upd in vl.upd[2]:
            nbi += self._IUDitems(upd)
        for upd in vl.upd[3]:
            nbi += self._IUDitems(upd)
        for upd in vl.upd[4]:
            nbi += self._IUDitems(upd)
        
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
     
    def setAccTkt(self, tk:AccTkt) -> None:
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

    def searchInIndex(self, name:str, startCols:Dict[str, Any], resCols:Iterable[str]) -> Iterable[Dict[str, Any]]:
        # startcols : {"c1":"v1", "c2:n2 ... }
        # resCols : ("c3","c4")
        sqlx = ["SELECT "]
        i = 0
        for c in resCols:
            sqlx.append(("`" if i == 0 else ", `") + c + "`")
            i += 1
        sqlx.append(" FROM " + self.org + "_" + name + " WHERE ")
        lv = []
        for c in startCols.keys():
            sqlx.append((" `" if i == 0 else " and `") + c + "` = %s")
            lv.append(startCols[c])
        sql = "".join(sqlx)
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, lv)
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

#z, y = test()
#print("OK")

    