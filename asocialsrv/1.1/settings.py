class Settings:
    
    def __init__(self):
        self.PG = False
        
        if self.PG:
            self.dbProvider = ("pgdb", "PgdbProvider")
        else:
            self.dbProvider = ("mariadb", "MariadbProvider")
            
        self.db = {"host":"127.0.0.1", "database":"asocial1", "user":"asocial", "password":"nonuke", "poolSize":10, "timeOut":10}
        self.MAXCACHESIZE = 50*1000*1024
                    
settings = Settings()