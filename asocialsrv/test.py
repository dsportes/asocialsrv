from settings import sqlselector
from util import Stamp

if True:
#    p = getattr(importlib.__import__("sqlselector"), "sqlselector").get()
    z, y = sqlselector.get().onoff()
    print("z->" + z.str() + "\nprod->" + y.str())

if False:
    Stamp.test()