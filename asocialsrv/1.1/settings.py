class Settings:
    def __init__(self):
        self.dbProvider = ("mariadb", "Provider")
        self.db = {"host":"127.0.0.1", "db":"asocial1", "user":"asocial", "password":"nonuke", "poolSize":10, "timeOut":10}
        self.MAXCACHESIZE = 50*1000*1024

settings = Settings()