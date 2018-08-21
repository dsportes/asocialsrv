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
    """
    _docid : identification of the document
    _status : -1:NOT existing, 0:deleted, 1:unmodified, 2:modified, 3:created 4:recreated (after deletion)
    _isfull : True: all items, False:only hdr singleton
    _isro : True: read only, False:modifications allowed
    _hdr : header singleton, never None
    _singletons : dict of singletons by class
    _items : dict by class of items by key
    _dtime : issued from hdr
    _ctime
    _vt1
    _vt2
    _nvt1
    _nvt2
    _changedItems : dict of items created / recreated / deleted / modified
    
    """
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
    
    def id(self):
        return self._descr.table + "/" + self._docid
 
    def fk(self, itd, key):
        return self.id() + ("@" + itd.code + ("/" + key if key is not None else "") if itd is not None else "")
           
    def delete(self):
        assert not self.isro and self._status > 0, "deletion not allowed on read only or already deleted document [" + self.id() + "]"
        self.detachAll();
        self._status = 0;

    def detachAll(self):
        for k in self._singletons:
            self._singletons[k]._document = None
        del self._singletons
        for cl in self._items:
            for k in self._items[cl]:
                self.items[cl][k]._document = None
        del self._items
        if hasattr(self, "_changeditems"):
            del self._changeditems
        if hasattr(self, "_hdr"):
            del self._hdr
            
    def create(clsOrTable, store_data, isro, isfull):
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
        for cl in d._descr.itemDescrByCode:
            d.items[cl] = {}
        d._isfull = isfull
        d._isro = isro
        d._status = 1 if store_data is not None else 3
        for c in dd.itemDescrByCode:
            itd = dd.itemDescrByCode[c]
            if not itd.isSingleton:
                d._items[itd.code] = {}
        if store_data is not None:
            d.loadFromStoreData(store_data)
        else:   # created empty and ready and committed
            itd = d._descr.itemDescrByCode.get("hdr")
            d._hdr = itd.cls(d)
            d._status = 3
            d._ready = True
            d._ctime = 0
            d._dtime = 0
            d._vt1 = 0
            d._vt2 = 0
            d._hdr.commit()
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
                if ms is None:      # NOT existing
                    item._status = -1
                    item._version = v
                else: # existing, not ready
                    item._meta = json.loads(ms[0])
                    item._serial = ms[1];
                    item._status = 1
                    item._version = v
                if itd.isSingleton:
                    if cl == "hdr":
                        self._hdr = item
                        self._dtime = item._meta.get("dtime", 0)
                        self._ctime = item._meta.get("ctime", 0)
                        self._vt1 = item._meta.get("vt1", 0)
                        self._vt2 = item._meta.get("vt2", 0)
                    else:
                        self._singletons[cl] = item
                else:
                    item._key = key;
                    self._items[cl][key] = item
            except Exception as e:
                continue
                    
    def getItem(self, itd, key, orNew):
        assert self._isfull, "getItem [" + self.fk(itd, key) + "] on a not full document"
        if itd.code == "hdr":
            return self._hdr
        item = self._singletons[itd.code] if key is None else self._items[itd.code][key]
        if item is None:
            if orNew is None:
                return None
            # created empty
            item = itd.cls(self)
            item._status = 3
            item._ready = True
            item._newserial = "{}"
            if key is None:
                self._singletons[itd.code] = item
            else:
                self._items[itd.code][key] = item
        else:
            if item._status == -1:      # was NOT existing
                if orNew is None:
                    return None
                # created empty
                item.reset()
                item._status = 3
                item._ready = True
                item._newserial = "{}"
                return item
            # deleted OR existing
            if item._status == 0: # deleted
                if orNew is None:
                    return item
                # RE created empty
                item.reset()
                item._status = 4
                item._ready = True
                item._newserial = "{}"
                return item
            if item._status > 1: # created / recreated / modified
                return item
            # not modified, perhaps never loaded
            if not hasattr(item, "_ready"):
                item.loadFromJson(item._serial)
                item._ready = True
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
    
    def vt1(self):
        return 0 if not hasattr(self, "_vt1") else self._vt1

    def vt2(self):
        return 0 if not hasattr(self, "_vt2") else self._vt2

    def nvt1(self):
        return 0 if not hasattr(self, "_nvt1") else self._nvt1

    def nvt2(self):
        return 0 if not hasattr(self, "_nvt2") else self._nvt2
    
    def nbChanges(self):
        return 0 if hasattr(self, "_changeditems") else len(self._changeditems)
    
    def notifyChange(self, dv1, dv2, chg, item): 
        # chg: 0:no new item to save, 1:add item, -1:remove item, dv1/dv2 delta of volumes
        if chg != 0 and not hasattr(self, "_changeditems"):
            self._changeditems = {}
        if chg < 0:
            del self._changeditems[item.id()]
        if chg > 0:
            self._changeditems[item.id()] = item
        if dv1 != 0:
            self._nvt1 = self.nvt1() + dv1
        if dv2 != 0:
            self._nvt1 = self.nvt1() + dv2
        ch = self.nbChanges() != 0
        if self._status == 1:
            if ch:
                self._status = 2
        elif self._status == 2:
            if not ch:
                self._status = 1
        
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
    _status : -1:NOT existing, 0:deleted, 1:unmodified, 2:modified, 3:created 4:recreated (after deletion)
    _document : which the item belongs to. When None, the item is disconnected (ignored by the document)
    _version : 0 for created / recreated, for NOT existing deletion time, last modified for other status
    _key : for items, not singleton
    _meta : {version v1 v2  hdr(dtime, ctime, vt1, vt2) others(cas) : initial values
    _ready : the content of the item is ready, else still serialized
    _serial : if None, the item did'nt exit before the operation _status == -1). version is its deletion stamp
    _newserial : if None not committed
    _nv1 : new value of v1 after committed
    _nv2 : new value of v1 after committed
    _nvt1 : initial value of vt1 : hdr only
    _nvt2 : initial value of vt2 : hdr only
    _oldIndexes : initial values of indexes (dict by index name)
    _newIndexes : new values of indexes (dict by index name), only those having chjanged
    
    if deleted in meta, the item was deleted in the operation (existing before or not depending on serial)
    if committed in meta, the item probably changed in the operation (if not existing before, it's a creation, else a modification)
    if loaded in meta, the item object has actual values, else not yet deserialized
    if version == 0, the item has been created in the operation (or recreated depending on serial)
    """
    
    def id(self):
        return self._descr.code + ("" if self.descr.isSingleton else "/" + self._key)
    
    def fk(self):
        di = "disconnected" if self._document is None else self._document._descr.table + "/" + self._document._docid
        return di + "@" + self.id()

    def istosave(self):
        return self._status == 0 or self._status == 0

    def isexisting(self):
        return self._status > 1

    def wasexisting(self):
        return self._status > 0
    
    def __init__(self, document):
        self._document = document
        
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
        bk = {}
        for x in self.__dict__:
            if not x.startswith("_"):
                bk[x] = self.__dict__[x]
        return json.dumps(bk)

    def getIndexedValues(self, idxName):
        idx = self._descr.indexes.get(idxName, None)
        if idx is None:
            return None
        if not idx.isList():
            values = []
            for var in idx.columns:
                values.append(getattr(self, var, None))            
            return json.dumps(values)
        else:
            result = []
            lst = getattr(self, idx.varList)
            for x in lst:
                values = []
                for var in idx.columns:
                    values.append(x.get(var, None))            
                result.append(values)
            return json.dumps(result)   
        
    def commit(self):
        assert self._document and not self.document._isro, "commit forbidden on disconnect or read only documents [" + self.fk() + "]"
        assert self.status > 0, "commit forbidden on a deleted item [" + self.fk() + "]"
        ser = self.toJson()
        dv1, dv2 = self.getDV(ser)
        if self._status > 2:    # created / recreated : status inchangé
            self._newserial = ser
            self._document.notifyChange(dv1, dv2, 0)
            return
        # unmodified or modified
        if self._status == 2 and ser == self._serial:  # WAS modified but nothing new
            self._status = 1;
            del self.__dict__["_newserial"]
            del self.__dict__["_newindexes"]
            self._document.notifyChange(dv1, dv2, -1, self) # one less to save
            return
        # unmodified or modified but new
        if ser == self._serial:
            return  # nothing changed
        self._newserial = ser
        self._status = 2
        self._document.notifyChange(dv1, dv2, 1, self)
        self._newindexes = {}
        for idx in self._descr.indexes:
            idx = self.getIndexedValues(idx)
            old = self._oldindexes[idx] if hasattr(self, "_oldindexes") else ""
            if idx != old:
                self._newindexes[idx] = idx
        
    def getDV(self, ser):
        dv1 = len(ser) - self.nv1()
        self._nv1 = len(ser)
        av2 = 0 if not hasattr(self, "v2") else self.v2 # v2 actuellement déclaré dans l'item
        dv2 = av2 - self.nv2()
        self._nv2 = av2
        return(dv1, dv2)
        
    def delete(self):
        pass #TODO
        
    def rollback(self):
        pass #TODO
        
    def v1(self):
        return 0 if not hasattr(self, "_meta") or not hasattr(self._meta, "v1") else self._meta["v1"]
    
    def nv1(self):
        return 0 if not hasattr(self, "_nv1") else self._nv1
    
    def v2(self):
        return 0 if not hasattr(self, "_meta") or not hasattr(self._meta, "v2") else self._meta["v2"]
    
    def nv2(self):
        return 0 if not hasattr(self, "_nv2") else self._nv2
    
    # TODO
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

