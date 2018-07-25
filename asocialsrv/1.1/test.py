from threading import local

data = local()
data.__setattr__('x', 'foo')

z = local()
y = z.__getattribute__('x')
print(z)