import pymysql.cursors
from settings import settings
import pymysqlpool
from root import al, dics, AppExc, Stamp
from document import DocumentArchive
import json
from gzip import compress, decompress

    
class Provider:
    dics.set("fr", "XSQL1", "Incident d'accès à la base de données. Opération:{0} Org:{1} SQL:{2} Cause:{3}")
    
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
            self.connection.close()
        finally:
            return
    
    def SqlExc(self, sql, exc):
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
    sqlitems2 = " WHERE `docid` = %s"
    sqlitems2 = " WHERE `docid` = %s and `clkey` = 'hdr[]'"
    
    def getArchive(self, table, docid, isfull):
        """
        Construit une archive complète depuis la base
        """
        arch = DocumentArchive(table, docid, isfull)
        sql = Provider.sqlitems1 + self.org + "_item" + Provider.sqlitems2 if isfull else Provider.sqlitems2
        try:
            nbItems = 0
            with self.connection.cursor() as cursor:
                nbItems = cursor.execute(sql, (docid,))
                lst = cursor.fetchall()
                for row in lst:
                    cl, keys = Provider.clkey(row)
                    meta = Provider.meta(row, cl)
                    content = Provider.content(row)
                    arch.addItem(cl, keys, meta, content)
            if nbItems == 0:
                return (0, None)
            else:
                return (3, arch)
        except Exception as e:
            raise self.SqlExc(sql, e)

    
    def getDelta(self, cible, isfull):
        """
        Construit l'archive delta du document table/docid pour la mise à jour de la cible
        retourne le tuple (statut, delta)
        statut vaut : 0-document inconnu, 1-cible à jour, 2:delta, 3:full
        """
        pass

           
