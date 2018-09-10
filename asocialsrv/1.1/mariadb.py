from pymysql import Connection, connect, cursors
from settings import settings
import pymysqlpool
from util import AppExc
from sql import Provider

class MariadbProvider(Provider):
    def _create_conn() -> Connection:
        return connect(host=settings.db['host'], database=settings.db['database'], user=settings.db['user'], password=settings.db['password'],
                             charset='utf8mb4', cursorclass=cursors.DictCursor)

    pool = pymysqlpool.Pool(create_instance=_create_conn, max_count=settings.db['poolSize'], timeout=settings.db['timeOut'])
    
    def __init__(self, operation):
        super().__init__(operation)
        try:
            self.connection = MariadbProvider.pool.get()
            # self.connection = connect(host=settings.db['host'], db=settings.db['db'], user=settings.db['user'], password=settings.db['password'], charset='utf8mb4', cursorclass=cursors.DictCursor)
        except Exception as e:
            AppExc("XSQLCONX", [operation.opName, operation.org, str(e)])
    