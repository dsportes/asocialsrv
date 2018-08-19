import json

class Item:
    def __init__(self):
        self._meta = {"p1":"v1"}
        self._temp = {}
        
    def toJson(self):
        m = self._meta
        t = self._temp
        del self.__dict__["_meta"]
        del self.__dict__["_temp"]
        ser = json.dumps(self.__dict__)
        self._meta = m
        self._temp = t
        return ser

class Item1(Item):
    def __init__(self):
        super().__init__()
        self.nom = "daniel"
        self.age = 68
        self.compte = 3250.27
        self.famille = {"conjoint":"Domi", "enfants":["Cécile", "Julien", "Thomas", "Emilie"]}
        self.voyages = [{"d":"Chine", "a":2018}, {"d":"Vietnam", "a":2017}]
        self._temp["p2"] = "v2"
        
    
it1 = Item1()
ser = it1.toJson()
print(ser)


print("vrai" if it1.nom else "faux")
    
it1.nom = ""
print("vrai" if it1.nom else "faux")

it1.nom = None
print("vrai" if it1.nom else "faux")

