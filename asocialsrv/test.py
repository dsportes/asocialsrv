import importlib

p = getattr(importlib.__import__("sqlselector"), "sqlselector").get()

z, y = p.onoff()
print("z->" + z.str() + "\nprod->" + y.str())
