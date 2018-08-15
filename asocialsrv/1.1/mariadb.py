import pymysql.cursors
from settings import settings
import pymysqlpool
from root import al, dics, AppExc, Stamp
    

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
        except Exception as e:
            AppExc("XSQLCONX", [operation.opName, operation.org, e.message()])
    
    def close(self):
        try:
            self.connection.close()
        finally:
            return
    
    def SqlExc(self, sql, exc):
        return AppExc("XSQL1", self.operation.opName, self.operation.org, sql, exc.message())
    
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


           
