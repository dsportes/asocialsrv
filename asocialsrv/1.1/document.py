import json

class DocumentDescr: 
    def __init__(self, cls, table):
        self.cls = cls
        cls._descr = self
        self.documentName = self.cls.__name__
        self.table = table
        self.hdr = None
        self.itemDescrByCls = {}
        self.itemDescrByCode = {}
        self.indexes = {}
        
    def hasIndexes(self):
        return len(self.indexes) != 0
    
class ItemDescr:
    def __init__(self, DocumentClass, ItemClass, code, indexes):
        dd = Document.descr(DocumentClass)
        assert (dd is not None), "DocumentClass not registered"
        assert (ItemClass.__name__ not in dd.itemDescrByCls), "ItemClass is already registered"
        assert (code is not None and isinstance(code, str) and code not in dd.itemDescrByCode or len(code) > 3), "code is None or is already registered or has a length > 3"
        assert (indexes is None or isinstance(indexes, list))
        self.documentDescr = dd
        self.cls = ItemClass
        ItemClass._descr = self
        self.code = code
        self.nbOfIndexes = 0
        dd.itemDescrByCls[ItemClass.__name__] = self
        dd.itemDescrByCode[code] = self
        if code == "hdr":
            dd.hdr = self
        self.isSingleton = issubclass(ItemClass, Singleton)
        self.indexes = {}
        if indexes is not None and len(indexes) != 0:
            for idx in indexes:
                assert (idx.name not in dd.indexes), "Index " + idx.name + " already registered in the document"
                dd.indexes[idx.name] = idx
                self.indexes[idx.name] = idx

    def hasIndexes(self):
        return len(self.indexes) != 0

class Index:
    def __init__(self, name, columns, varList=None):
        assert (name is not None and isinstance(name, str) and len(name) > 0), "Index name is not a non empty string"
        assert (columns is not None and isinstance(columns, list) and len(columns) > 0), "colums of Index " + name + " not a non empty list"
        for col in columns:
            assert (col is not None and isinstance(col, str)), "A column of Index " + name + " name is None or not a string"
            i = columns.index(col)
            j = len(columns) - columns[::-1].index(col) - 1
            assert j == i , "Duplicate column name " + col + "in Index " + name
        self.columns = columns
        assert varList is None or (isinstance(varList, str) and len(varList) > 0), "varList not an item variable name"
        self.varList = varList
        self.name = name
        
    def isList(self):
        return self.varList is not None
        
class Document:
    _byCls = {}
    _byTable = {}
    
    def register(cls, table):
        assert (cls is not None and issubclass(cls, Document)), "Document class is None or not a subclass of Document"
        assert (table is not None and isinstance(table, str)), "Table code is not a string"
        assert (cls.__name__ not in Document._byCls), "Document is already registered"
        assert (table not in Document._byTable), "Table is already registered"
        descr = DocumentDescr(cls, table)
        Document._byCls[cls.__name__] = descr
        Document._byTable[table] = descr
        return descr
        
    def descr(cls):
        return Document._byCls.get(cls.__name__, None)

    def descrOfTable(table):
        return Document._byTable.get(table, None)
        
    def registerItem(DocumentClass, ItemClass, code, indexes=None):
        assert (DocumentClass is not None and DocumentClass.__name__ in Document._byCls), "Document class is None or not registered"
        assert (ItemClass is not None and issubclass(ItemClass, BaseItem)), "ItemClass is not a subclass of Item or Singleton"
        if code == "hdr":
            assert issubclass(ItemClass, Singleton), "hdr must be a Singleton"
        return ItemDescr(DocumentClass, ItemClass, code, indexes)
    
    def check():
        for docName in Document._byCls:
            descr = Document._byCls[docName]
            assert descr.hdr is not None, "Document " + docName + " must have an hdr Singleton"
            
    def create(clsOrTable):
        if clsOrTable is None:
            return None
        if isinstance(clsOrTable, str):
            dd = Document._byTable.get(clsOrTable, None)
        else:
            dd = Document._byCls.get(clsOrTable.__name__, None)
        if dd is None:
            return None
        d = dd.cls()
        d._singletons = {}
        d._items = {}
        for c in dd.itemDescrByCode:
            itd = dd.itemDescrByCode[c]
            if not itd.isSingleton:
                d._items[itd.code] = {}
        return d
     
    def loadFromStoreData(self, store_data):
        for k in store_data:
            try:
                i = k.find("/")
                v = int(k[:i])
                j = k.find("/", i + 1)
                cl = k[i + 1:] if j == -1 else k[i + 1: j]
                key = None if j == -1 else k[j + 1:]
                itd = self._descr.itemDescrByCode.get(cl, None)
                if itd is None:
                    continue
                ms = store_data[k]
                item = itd.cls(self)
                if ms is None:      # deleted item
                    item._meta = {}
                else:
                    item._meta = json.loads(ms[0])
                    item._meta["serial"] = ms[1];
                item._meta["version"] = v;
                if itd.isSingleton:
                    self._singletons[cl] = item
                else:
                    item._meta["key"] = key;
                    self._items[cl][key] = item
            except Exception as e:
                continue
        return self
                    
    def getItem(self, itd, key, orNew):
        item = self._singletons[itd.code] if key is None else self._items[itd.code][key]
        if item is None:
            if orNew is None:
                return None
            item = itd.cls(self)
            item._meta = {"version":0, "loaded":True} # just created
        else:
            if "serial" not in item._meta:      # was deleted
                if orNew is None:
                    return None
                item._meta = {"version":0, "loaded":True} # just created (whatever it was deleted or not)
                item._temp = {}
                item.reset()
            if "loaded" not in item._meta:
                item.loadFromJson(item._meta["serial"])
                item._meta["loaded"] = True
        if key is None:
            self._singletons[itd.code] = item
        else:
            self._items[itd.code][key] = item
        return item
    
    def item(self, cls, key=None):
        itd = self._descr.itemDescrByCls.get(cls.__name__, None)
        if itd is None:
            return None
        return self.getItem(itd, None, False) if itd.isSingleton else self.getItem(itd, key, False)

    def itemOrNew(self, cls, key=None):
        itd = self._descr.itemDescrByCls.get(cls.__name__, None)
        if itd is None:
            return None
        return self.getItem(itd, None, True) if itd.isSingleton else self.getItem(itd, key, True)
    
    def prepareToValidate(self):
        toSave = []
        vt1 = 0
        vt2 = 0
        for cl in self._singletons:
            v1, v2 = self._singletons[cl].preValidate(toSave)
            vt1 += v1
            vt2 += v2
        for cl in self._items:
            x = self._items[cl]
            for key in x:
                v1, v2 = x[key].preValidate(toSave)
                vt1 += v1
                vt2 += v2
        if len(toSave) == 0:
            return False
        hdr = self._singletons["hdr"]
        hdr._meta["vt1"] = vt1
        hdr._meta["vt2"] = vt2
        if "newSerial" not in hdr._meta: # siluation of a change
            hdr._meta["newSerial"] = hdr._meta["oldSerial"]
        if "st" not in hdr._meta:
            hdr._meta["st"] = 10
    
class BaseItem:
    """
    if serial not in meta, the item did'nt exit before the operation. version is its deletion stamp
    if deleted in meta, the item was deleted in the operation (existing before or not depending on serial)
    if committed in meta, the item probably changed in the operation (if not existing before, it's a creation, else a modification)
    if loaded in meta, the item object has actual values, else not yet deserialized
    if version == 0, the item has been created in the operation (or recreated depending on serial)
    """
    def __init__(self, document):
        self._temp = {}
        
    def loadFromJson(self, json_data):
        self.reset()
        if json_data is not None:
            d = json.loads(json_data)       # d est un dict
            for var in d:
                setattr(self, var, d[var])
        
        if len(self._descr.indexes) != 0:
            # calcul de la serialisation initiale des index
            self._meta["oldIndexes"] = {}
            for idx in self._descr.indexes:
                self._meta["oldIndexes"][idx] = self.getIndexedValues(idx)
        return self
                
    def reset(self):
        for var in self.__dict__:
            if not var.startswith("_"):
                del self.__dict__[var]

    def toJson(self):
        m = self._meta
        t = self._temp
        del self.__dict__["_meta"]
        del self.__dict__["_temp"]
        ser = json.dumps(self.__dict__)
        self._meta = m
        self._temp = t
        return ser

    def getIndexedValues(self, idxName):
        idx = self._descr.indexes.get(idxName, None)
        if idx is None:
            return None
        if not idx.isList():
            values = []
            for var in idx.columns:
                values.append(getattr(self, var, None))            
            return values
        else:
            result = []
            lst = getattr(self, idx.varList)
            for x in lst:
                values = []
                for var in idx.columns:
                    values.append(x.get(var, None))            
                result.append(values)
            return result   
        
    def commit(self):
        self._meta["committed"] = True
        
    def setV2(self, v2):
        if v2 != self._meta["v2"]:
            self._meta["committed"] = True
            self._meta["nv2"] = v2
            
    def v1(self):       # v1 at operation start
        return 0 if "serial" not in self._meta else self._meta["v1"]
    
    def v2(self):       # v2 at operation start
        return 0 if "serial" not in self._meta else self._meta["v2"]

    def nv2(self):       # v2 last change or at operation start
        return self.v2() if "nv2" not in self._meta else self._meta["nv2"]

    def vt1(self):       # v1 at operation start
        return 0 if self._descr["code"] != "hdr" or "serial" not in self._meta else self._meta["vt1"]
    
    def vt2(self):       # v2 at operation start
        return 0 if self._descr["code"] != "hdr" or "serial" not in self._meta else self._meta["vt2"]
            
    def delete(self):
        self.reset()
        self._temp = {}
        self._meta["deleted"] = True
                
    def preValidate(self, toSave):
        m = self._meta
        if "serial" not in m:       # did'nt exist before
            if "committed" in m:    # it's a creation
                m["st"] = 1
                ser = self.toJson()
                m["nv1"] = len(ser)
                m["newSerial"] = ser
                m["newIndexes"] = {}
                for idx in self._descr.indexes:
                    m["newIndexes"][idx] = self.getIndexedValues(idx)
            toSave.append(self)
        else:                       # was existing before
            if "deleted" in m:      # but now deleted
                m["st"] = 2
                m["nv1"] = 0
                toSave.append(self)
            else:
                if "committed" in m:    # perhaps updated
                    ser = self.toJson()
                    st = 0
                    if m["nv2"] != m["nv2"]:
                        st = 3
                    if ser != m["serial"]:
                        m["newSerial"] = ser
                        m["nv1"] = len(ser)
                        m["newIndexes"] = {}
                        st += 10
                        for idx in self._descr.indexes:
                            x = self.getIndexedValues(idx)
                            y = m["oldIndexes"][idx]
                            if x != y:
                                m["newIndexes"][idx] = x
                    if st > 0:
                        m["st"] = st
                        toSave.append(self)
        v1 = m["v1"] if "nv1" not in m else m["nv1"]
        v2 = m["v2"] if "nv2" not in m else m["nv2"]
        return (v1, v2)
        
        def postValidate(self, stamp):
            m = self._meta
            s = m["st"]
            if s == 1:
                m["version"] = stamp
                m["serial"] = m["newSerial"]
                for idx in self._descr.indexes:
                    if idx in m["newIndexes"]:
                        m["oldIndexes"][idx] = m["newIndexes"][idx]
                m["v1"] = m["nv1"]
                del m["newIndexes"]
                del m["newSerial"]
                del m["st"]
                del m["nv2"]
                del m["nv1"]
            elif s == 2:
                m["version"] = stamp
                del m["newIndexes"]
                del m["newSerial"]
                del m["serial"]
                del m["st"]
                del m["nv2"]
                del m["nv1"]
            else:
                m["version"] = stamp
                if s % 10 == 3:
                    m["v2"] = m["nv2"]
                    del m["nv2"]
                if s // 10 == 1:
                    m["serial"] = m["newSerial"]
                    m["v1"] = m["nv1"]
                    del m["nv1"]
                    for idx in self._descr.indexes:
                        if idx in m["newIndexes"]:
                            m["oldIndexes"][idx] = m["newIndexes"][idx]
                del m["newIndexes"]
                del m["newSerial"]
                del m["st"]

class Singleton(BaseItem):
    def __init__(self, document):
        super().__init__(document)

class Item(BaseItem):
    def __init__(self, document):
        super().__init__(document)

