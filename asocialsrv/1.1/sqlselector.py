from settings import settings
import importlib, sys

class SqlSelector:
    providerClass = None
    def get(self, op = None):
        try:
            if SqlSelector.providerClass is None:
                providerModule = importlib.__import__(settings.dbProvider[0])
                SqlSelector.providerClass = getattr(providerModule, settings.dbProvider[1])
                if SqlSelector.providerClass is None:
                    print("Provider class NON TROUVEE : " + settings.dbProvider[0] + "." + settings.dbProvider[1], file=sys.stderr)
            return SqlSelector.providerClass(op)
        except Exception as e:
            print("Provider class NON TROUVEE : " + settings.dbProvider[0] + "." + settings.dbProvider[1] + str(e), file=sys.stderr)

sqlselector = SqlSelector()
