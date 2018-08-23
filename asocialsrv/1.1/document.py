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
    def __init__(self, DocumentClass, ItemClass, code, keys, indexes):
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
        for key in keys:
            assert (key is not None and isinstance(key, str)), "A property name in keys " + key + " is None or not a string"
            i = keys.index(key)
            j = len(keys) - keys[::-1].index(key) - 1
            assert j == i , "Duplicate property name " + key + "in keys"
        self.keys = keys
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
            assert j == i , "Duplicate name " + col + "in Index " + name
        self.columns = columns
        assert varList is None or (isinstance(varList, str) and len(varList) > 0), "varList not an item property name"
        self.varList = varList
        self.name = name
        
    def isList(self):
        return self.varList is not None
        
class Document:
    """
    _docid : identification of the document. None when released
    _status : -1:NOT existing, 0:deleted, 1:unmodified, 2:modified, 3:created 4:recreated (after deletion)
    _isfull : True: all items, False:only hdr singleton
    _age : age tolerance in seconds. > 0 is read only
    _hdr : header singleton, never None
    _singletons : dict of singletons by class
    _items : dict by class of items by key
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
        
    def registerItem(DocumentClass, ItemClass, code, keys, indexes=None):
        assert (DocumentClass is not None and DocumentClass.__name__ in Document._byCls), "Document class is None or not registered"
        assert (ItemClass is not None and issubclass(ItemClass, Item)), "ItemClass is not a subclass of Item"
        assert keys is not None and isinstance(keys, list) and len(keys) > 0, "keys must be an non empty list of property names"
        if code == "hdr":
            assert issubclass(ItemClass, Singleton), "hdr must be a Singleton"
        return ItemDescr(DocumentClass, ItemClass, keys, code, indexes)

    def registerSingleton(DocumentClass, ItemClass, code, indexes=None):
        assert (DocumentClass is not None and DocumentClass.__name__ in Document._byCls), "Document class is None or not registered"
        assert (ItemClass is not None and issubclass(ItemClass, Singleton)), "ItemClass is not a subclass of Singleton"
        return ItemDescr(DocumentClass, ItemClass, [], code, indexes)
    
    def check():
        for docName in Document._byCls:
            descr = Document._byCls[docName]
            assert descr.hdr is not None, "Document " + docName + " must have an hdr Singleton"
    
    def id(self):
        return self._descr.table + "/" + (self._docid if self._docid is not None else "!!!RELEASED!!!")
    
    def hdr(self):
        assert self._docid is not None, "document " + self.id()
        return self._hdr
 
    def fk(self, itd, keys):
        return self.id() + ("@" + itd.code + ( json.dumps(keys) if keys is not None else "") if itd is not None else "")
           
    def release(self):
        assert self._docid is not None, "document " + self.id()
        self._docid = None
        # TODO
    
    def delete(self):
        assert self._docid is not None and self._age == 0 and self._status > 0, "deletion not allowed on read only or already deleted document [" + self.id() + "]"
        self._detachAll();
        self._status = 0;

    def _detachAll(self):
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
            
    def _create(clsOrTable, store_data, age, isfull):
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
        d._age = age
        d._isfull = isfull
        for cl in d._descr.itemDescrByCode:
            d.items[cl] = {}
        d._status = 1 if store_data is not None else 3
        for c in dd.itemDescrByCode:
            itd = dd.itemDescrByCode[c]
            if not itd.isSingleton:
                d._items[itd.code] = {}
        if store_data is not None:
            d._loadFromStoreData(store_data)
        else:   # created empty and ready and committed
            itd = d._descr.itemDescrByCode.get("hdr")
            d._hdr = itd.cls(d)
            d._hdr.newEmpty(3)
        return d
     
    def _loadFromStoreData(self, store_data):
        for k in store_data:
            try:
                if self._descr.isSingleton:
                    cl = k
                else:
                    i = k.find("[")
                    cl = k[:i]
                    key = k[i:]
                    keys = json.loads(self._key)
                itd = self._descr.itemDescrByCode.get(cl, None)
                if itd is None:
                    continue
                if len(itd.keys) != len(self._keys):
                    continue
                ms = store_data[k]
                if ms is None or len(ms) == 0:
                    continue
                item = itd.cls(self)
                item._meta = ms[0]
                lm = len(item._meta)
                if len(ms) == 1:         # NOT existing
                    item._status = -1
                    if lm != 1:
                        continue
                else: # existing, not ready
                    item._serial = ms[1];
                    item._status = 1                 
                if (cl == "hdr" and lm != 5) or lm != 3:
                    continue 
                if itd.isSingleton:
                    if cl == "hdr":
                        self._hdr = item
                    else:
                        self._singletons[cl] = item
                else:
                    item._keys = keys;
                    self._items[cl][key] = item
            except Exception as e:
                continue
                    
    def _getItem(self, itd, keys, orNew):
        if itd.code == "hdr":
            return self._hdr
        assert self._isfull, "_getItem [" + self.fk(itd, keys) + "] on a not full document"
        assert (not itd.isSingleton and keys is not None and len(itd.keys) == len(keys)) or (itd.isSingleton and keys is None), "_getItem [" + self.fk(itd, keys) + "] incorrect number of keys"
        if keys is not None:
            kstr= json.dumps(keys)
        item = self._singletons[itd.code] if itd._isSingleton is None else self._items[itd.code][kstr]
        if item is None:
            if orNew is None:
                return None
            # created empty
            item = itd.cls(self)
            if itd._isSingleton:
                item._newEmpty(3)
                self._singletons[itd.code] = item
            else:
                item._keys = keys       # completely new : DOES NOT have keys
                item._newEmpty(3)
                self._items[itd.code][kstr] = item
        else:
            if item._status == -1:      # was NOT existing (deleted before the operation). HAS keys
                if orNew is None:
                    return None
                # created empty
                item._newEmpty(3)
                return item
            # deleted OR existing
            if item._status == 0: # deleted during the operation. HAS keys
                if orNew is None:
                    return item
                # RE created empty
                item._setEmpty(4)
                return item
            if item._status > 1: # created / recreated / modified (already loaded)
                return item
            # not modified, perhaps never loaded
            if not hasattr(item, "_ready"):
                item._loadFromJson(item._serial)
                item._setKeys()
                item._ready = True
                return item
    
    def item(self, cls, keys=None):
        assert self._docid is not None, "document " + self.id()
        itd = self._descr.itemDescrByCls.get(cls.__name__, None)
        if itd is None:
            return None
        return self._getItem(itd, None, False) if itd.isSingleton else self._getItem(itd, keys, False)

    def itemOrNew(self, cls, keys=None):
        assert self._docid is not None, "document " + self.id()
        itd = self._descr.itemDescrByCls.get(cls.__name__, None)
        if itd is None:
            return None
        return self._getItem(itd, None, True) if itd.isSingleton else self._getItem(itd, keys, True)
        
    def nbChanges(self):
        return 0 if hasattr(self, "_changeditems") else len(self._changeditems)
    
    def _notifyChange(self, dl, chg, item): 
        # chg: 0:no new item to save, 1:add item, -1:remove item, dv1/dv2 delta of volumes
        if chg != 0 and not hasattr(self, "_changeditems"):
            self._changeditems = {}
        if chg < 0:
            del self._changeditems[item.id()]
        if chg > 0:
            self._changeditems[item.id()] = item
        if dl != 0:
            self._hdr._nl = self._hdr.NL() + dl
        ch = self.nbChanges() != 0
        if self._status == 1:
            if ch:
                self._status = 2
        elif self._status == 2:
            if not ch:
                self._status = 1
        
    def _prepareToValidate(self):
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
    _meta : [v c l d t] : item initial meta attributes
    _keys : [] of item key attributes. Cannot be changed here by the application logic
    _ready : the content of the item is ready (deserialized from _serial)
    _serial : if None, the item did'nt exit before the operation _status == -1). _meta[0] version of deletion
    _newserial : if None not committed
    _nl : new length of the item after commit
    _nt : new value of _t (_meta[4] : hdr only
    _oldIndexes : initial values of indexes (dict by index name)
    _newIndexes : new values of indexes (dict by index name), only those having chjanged
    """
    
    def id(self):
        return self._descr.code + ("" if self.descr.isSingleton else json.dumps(self.getKeys()))
    
    def fk(self):
        di = "disconnected" if self._document is None else self._document._descr.table + "/" + self._document._docid
        return di + "@" + self.id()

    def istosave(self):
        return self._status == 0 or self._status == 0

    def isexisting(self):
        return self._status > 1

    def wasexisting(self):
        return self._status > 0
    
    def V(self):
        return self.meta[0]
    
    def C(self):
        return self.meta[1] if len(self._meta) > 1 else 0

    def L(self):
        return self.meta[2] if len(self._meta) > 2 else 0

    def NL(self):
        return self._nl if hasattr(self, "_nl") else self.L()

    def D(self):
        return self.meta[3] if len(self._meta) > 3 else 0
    
    def T(self):
        return self.meta[4] if len(self._meta) > 4 else 0

    def NT(self):
        return self._nt if hasattr(self, "_nt") else self.T()
    
    def __init__(self, document):
        self._document = document
        
    def _loadFromJson(self, json_data):
        self.reset()
        if json_data is not None:
            d = json.loads(json_data)       # d est un dict
            for var in d:
                setattr(self, var, d[var])
        
        if len(self._descr.indexes) != 0:
            # calcul de la serialisation initiale des index
            self._meta["oldIndexes"] = {}
            for idx in self._descr.indexes:
                self._meta["_oldIndexes"][idx] = self.getIndexedValues(idx)
        return self
                
    def _newEmpty(self, status):
        self._reset()
        d = self.__dict__
        if hasattr(d, "_newindexes"):
            del d["_newindexes"]
        self._meta = [0,0,0,0,0] if self._descr.code == "hdr" else [0,0,0]
        self._newserial = "{}"
        self._status = status
        self._ready = True
        self._setKeys()
        self._nl = self._getKeyLength()
        self._document._notifyChange(self._nl, 1, self)
    
    def _setKeys(self):
        if not self._desr.isSingleton:
            i = 0
            for nk in self._descr.keys:
                setattr(self, nk, self._keys[i])
                i += 1

    def _getKeys(self):
        k = []
        if not self._desr.isSingleton:
            for nk in self._descr.keys:
                k.append(getattr(self, nk))
        return k
    
    def _getKeyLength(self):
        return 0 if self._desr.isSingleton else len(json.dumps(self.getKeys()))
    
    def _reset(self):
        for var in self.__dict__:
            if not var.startswith("_"):
                del self.__dict__[var]

    def _delChanges(self, opt=False):
        d = self.__dict__
        if hasattr(d, "_newindexes"):
            del d["_newindexes"]
        if hasattr(d, "_nl"):
            del d["_nl"]
        if not opt:
            if hasattr(d, "_ready"):
                del d["_ready"]
            if hasattr(d, "_newserial"):
                del d["_newserial"]
        else:
            self._newserial = "{}"
            self._ready = True

    def toJson(self):
        assert self._document._docid is not None, "document " + self._document.id()
        bk = {}
        for x in self.__dict__:
            if not x.startswith("_"):
                bk[x] = self.__dict__[x]
        return json.dumps(bk)

    def getIndexedValues(self, idxName):
        assert self._document._docid is not None, "document " + self._document.id()
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
        assert self._document._docid is not None, "document " + self._document.id()
        assert self._document._age == 0, "commit forbidden on read only documents " + self.fk()
        assert self._status > 0, "commit forbidden on a deleted item " + self.fk()
        ser = self.toJson()
        dv1, dv2 = self._getDV(ser)
        if self._status > 2:    # created / recreated : status inchangé
            self._newserial = ser
            self._document._notifyChange(dv1, dv2, 0)
            return
        # unmodified or modified
        if self._status == 2 and ser == self._serial:  # WAS modified but nothing new
            self._status = 1;
            del self.__dict__["_newserial"]
            del self.__dict__["_newindexes"]
            self._document._notifyChange(dv1, dv2, -1, self) # one less to save
            return
        # unmodified or modified but new
        if ser == self._serial:
            return  # nothing changed
        self._newserial = ser
        self._status = 2
        self._document._notifyChange(dv1, dv2, 1, self)
        self._newindexes = {}
        for idx in self._descr.indexes:
            idx = self.getIndexedValues(idx)
            old = self._oldindexes[idx] if hasattr(self, "_oldindexes") else ""
            if idx != old:
                self._newindexes[idx] = idx
        
    def _getDV(self, ser):
        dv1 = len(ser) - self.nv1()
        self._nv1 = len(ser)
        av2 = 0 if not hasattr(self, "v2") else self.v2 # v2 actuellement déclaré dans l'item
        dv2 = av2 - self.nv2()
        self._nv2 = av2
        return(dv1, dv2)
        
    def delete(self):
        assert self._document._docid is not None, "document " + self._document.id()
        assert self._document._age == 0, "delete forbidden on read only documents " + self.fk()
        assert self._status > 0, "delete forbidden on a deleted item " + self.fk()
        if self._status < 1:
            return
        self._status = 0
        self._changes()
        self.reset()
        
    def rollback(self):
        assert self._document._docid is not None, "document " + self._document.id()
        assert self._document._age == 0, "rollback forbidden on read only documents " + self.fk()
        #TODO
            
    # TODO
    def _preValidate(self, toSave):
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

