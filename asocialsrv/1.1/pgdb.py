import psycopg2
from settings import settings
from root import dics, AppExc
from sql import Provider

class PgdbProvider(Provider):
    dics.set("fr", "XSQL1", "Incident d'accès à la base de données. Opération:{0} Org:{1} SQL:{2} Cause:{3}")
    dics.set("fr", "XCV", "Trop de contention détectée à la validation. Opération:{0} Org:{1} Document:{2} ")
    
    def _create_conn():
        return psycopg2.connect(host=settings.db['host'], database=settings.db['database'], user=settings.db['user'], password=settings.db['password'], port=5433)
    
    def __init__(self, operation):
        super().__init__(operation)
        try:
            self.connection = PgdbProvider._create_conn()
            # self.connection = connect(host=settings.db['host'], db=settings.db['db'], user=settings.db['user'], password=settings.db['password'], charset='utf8mb4', cursorclass=cursors.DictCursor)
        except Exception as e:
            AppExc("XSQLCONX", [operation.opName, operation.org, str(e)])

