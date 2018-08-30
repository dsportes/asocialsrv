import json
from root import Stamp, Operation, dics, AppExc
from threading import Lock
from settings import settings

dics.set("fr", "XCW", "Trop de contention détectée en phase work. Opération:{0} Org:{1} Document:{2} ")

class Update:
    def __init__(self, docDescr, docid, toDel, toUpd, version, newDtime):
        """
        updates for an item:
        c : 1:insert meta and content, 2:update meta only, 3:update meta and delete content, 4:update meta and content
        cl : item class
        key : item key, "" pour a singleton
        meta : [v, c, l, d, t] for hdr, vcl for others, v for "deleted"
        content: string serialized item
        
        updates for an index : {keys:[k], ird:1, val:[l1]] for simple, val:[[l2] ...]] for multiple
        [k] : values of keys or [] for a singleton
        iud: 1:insert new value (no old values) 2:replace old value by new value, 3:delete old value
        
        if newDtime is not None, purge deletions befaore newDtime
        """
        self.docid = docid
        self.toDel = toDel
        self.toUpd = toUpd
        self.version = version
        self.table = docDescr.table
        self.hasIndexes = docDescr.hasIndexes()
        if toUpd:
            self.updates = {}
            if self.hasIndexes:
                self.indexes = {}
                for n in docDescr.indexes:
                    idx = docDescr.indexes[n]
                    self.indexes[n] = {"name":n, "isList":idx.isList(), "kn":self._descr.keys, "cols":idx.columns, "updates":[]}
                    
    def addUpdate(self, c, cl, keys, meta, content):
        # c : 1:insert meta and content, 2:update meta only, 3:update meta and delete content, 4:update meta and content
        self.updates[(cl, keys)] = {"c":c, "cl":cl, "keys":keys, "meta":meta, "content":content}
        
    def getIndex(self, idx):
        return self.indexes.get(idx, None)
            

class DocumentArchive:
    """
    A DocumentArchive instance is a snapshot of a document given by a couple table / docid
    After building (constructor and sequence of addItem), a DocumentArchive is immutable.
    A DocumentArchive can be required as full or hdr only.
    """
    def __init__(self, table, docid, isfull=True):
        assert (table is not None and isinstance(table, str) and len(table) > 1), "Table code is not a valid string"
        assert (docid is not None and isinstance(docid, str) and len(docid) > 1), "docid is not a valid string"
        assert (table in Document._byTable), "Table is not registered"
        self.descr = Document._byTable[table]
        self.docid = docid
        self.isfull = isfull
        self.items = {}
        self.hdr = None
        self.version = 0
        self.ctime = 0
        self.dtime = 0
        self.totalSize = 0

    def id(self, clkey=None):
        return self.table + "/" + self.docid + ("" if clkey is None else "@" + clkey)

    def id2(self, cl, keys):
        return self.table + "/" + self.docid + "@" + cl +str(keys)
        
    def addItem(self, cl, keys, meta, content):
        if cl == "hdr":
            assert len(meta) == 5, "Invalid meta size for " + self.id2(cl, keys)
            # version size ctime dtime totalSize
            self.version = meta[0]
            self.ctime = meta[2]
            self.dtime = meta[3]
            self.totalSize = meta[4]
        else:
            assert len(meta) == 3, "Invalid meta size for " + self.id(cl, keys)
        if meta[1] == 0:
            v = (meta, None)        
        else:
            assert content is not None and isinstance(content, str) and len(content) >= 2, "Invalid content for " + self.id2(cl, keys)
            v = (meta, content)
        k = (cl,keys)
        self.items[k] = v
        if cl == "hdr":
            self.hdr = v
            
    def clkeys(self):
        return self.items.keys()
    
    def getItem(self, clkey):
        return self.items.get(clkey, None)
    
    def merge(self, delta):
        # returns a new archive from self (the source) and a delta. delta HAS changes
        target = DocumentArchive(self.table, self.docid, self.isfull)
        target.version = delta.version
        target.dtime = delta.dtime
        target.ctime = delta.ctime
        
        if not self.isfull:
            target.addItem("hdr", tuple(), delta.hdr["meta"], delta.hdr["content"])
            return target

        for clkey in delta.items:  
            # adding all items from the delta
            item = delta.items[clkey]
            target.addItem(item["cl"], item["keys"], item["meta"], item["content"])
            
        keys = getattr(delta, "keys", None)
        for clkey in self.items:  
            """
            adding all items from the source 
            IF unchanged (not already added because modified)
            AND IF deleted, deleted after delta dtime (sill in the deletion history)
            AND IF existing, no keys set or listed in the keys set (il any) (or still exists)
            """ 
            if clkey in target.items:
                continue
            item = delta.items[clkey]
            m = item["meta"]
            if m[1] == 0: # deleted
                if m[0] > delta.dtime:  # after delta dtime, to be in deletion history
                    target.addItem(item["cl"], item["keys"], item["meta"], None)
            else:       # existing and not changed
                if keys is None or (keys is not None and clkey in keys):             
                    target.addItem(item["cl"], item["keys"], item["meta"], item["content"])
        return target
        

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
    
    def id(self, docid):
        return self.table + "/" + docid;
    
class ItemDescr:
    def __init__(self, DocumentClass, ItemClass, code, keys, indexes):
        dd = Document.descr(DocumentClass)
        assert (dd is not None), "DocumentClass not registered"
        assert (ItemClass.__name__ not in dd.itemDescrByCls), "ItemClass is already registered"
        assert code is not None and isinstance(code, str) and code not in dd.itemDescrByCode and len(code) > 0, "code is None or not a string or empty or is already registered"
        assert (indexes is None or isinstance(indexes, tuple))
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
                idx.kns = keys
                dd.indexes[idx.name] = idx
                self.indexes[idx.name] = idx

    def hasIndexes(self):
        return len(self.indexes) != 0

class Index:
    def __init__(self, name, columns, varList=None):
        assert (name is not None and isinstance(name, str) and len(name) > 0), "Index name is not a non empty string"
        assert (columns is not None and isinstance(columns, tuple) and len(columns) > 0), "colums of Index " + name + " not a non empty list"
        for col in columns:
            assert (col is not None and isinstance(col, str)), "A column of Index " + name + " name is None or not a string"
            i = columns.index(col)
            j = len(columns) - columns[::-1].index(col) - 1
            assert j == i , "Duplicate name " + col + "in Index " + name
        self.columns = columns
        assert varList is None or (isinstance(varList, str) and len(varList) > 0), "varList not an item property name"
        self.varList = varList
        self.name = name
        self.kn = tuple()
        
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
    DELHISTORYINDAYS = 30

    def filter(args, cl, keys, content):
        return content
    
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
        assert keys is not None and isinstance(keys, tuple) and len(keys) > 0, "keys must be an non empty list of property names"
        if code == "hdr":
            assert issubclass(ItemClass, Singleton), "hdr must be a Singleton"
        return ItemDescr(DocumentClass, ItemClass, code, keys, indexes)

    def registerSingleton(DocumentClass, ItemClass, code, indexes=None):
        assert (DocumentClass is not None and DocumentClass.__name__ in Document._byCls), "Document class is None or not registered"
        assert (ItemClass is not None and issubclass(ItemClass, Singleton)), "ItemClass is not a subclass of Singleton"
        return ItemDescr(DocumentClass, ItemClass, code, tuple(), indexes)
    
    """
    def check():
        for docName in Document._byCls:
            descr = Document._byCls[docName]
            assert descr.hdr is not None, "Document " + docName + " must have an hdr Singleton"
    """
    
    def id(self):
        return self._descr.table + "/" + self.docid()
    
    def docid(self):
        return self._docid if self._docid is not None else "!!!RELEASED!!!"
    
    def hdr(self):
        assert self._docid is not None, "Document " + self.id()
        return self._hdr
 
    def fk(self, itd, keys):
        return self.id() + "@" + (itd.code if itd is not None else "???") + str(keys)
           
    def release(self):
        assert self._docid is not None, "Document already released " + self.id()
        self._docid = None
        self._detachAll()
        if hasattr(self, "_age"):
            del self._age
        if hasattr(self, "_isfull"):
            del self._isfull
        self.operation.releaseDocument(self)

    def delete(self):
        assert self._docid is not None and self._age == 0 and self._status > 0, "Deletion not allowed on released or read only or deleted document [" + self.id() + "]"
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
        if hasattr(self, "_hdr"):
            self._hdr._document = None
            del self._hdr
        if hasattr(self, "_changeditems"):
            del self._changeditems
            
    def _createNew(clsOrTable, docid, age, isfull): # if store_data is given, load from cache, else to create
        if isinstance(clsOrTable, str):
            docDescr = Document._byTable.get(clsOrTable, None)
        else:
            docDescr = Document._byCls.get(clsOrTable.__name__, None)
        assert docDescr is not None, "Not registered document class / table " + clsOrTable
        assert docid is not None and isinstance(docid, str) and len(docid) > 0, "Empty docid on createNew"
        return Document._create(docDescr, docid, None, age, isfull)
            
    def _createFromArchive(arch): # if store_data is given, load from cache, else to create
        assert arch is not None and isinstance(arch, DocumentArchive), "createFromArch with no arch"
        return Document._create(arch.descr, arch.docid, arch, 0, True)

    def _create(docDescr, docid, arch, age, isfull): # if store_data is given, load from cache, else to create            
        d = docDescr.cls()
        d._descr = docDescr
        d._docid = docid
        d._age = age
        d._isfull = isfull
        d._singletons = {}
        d._items = {}
        for cl in d._descr.itemDescrByCode:
            d._items[cl] = {}
            
        if arch is not None:
            d._status = 1
            d.arch = arch
            
            if isfull:
                for clkey in arch.clkeys():
                    itd = d._descr.itemDescrByCode.get(clkey[0], None)
                    meta, content = arch.getItem(clkey)
                    item = itd.cls()._setDocument(d)._setup(-1 if meta[1] == 0 else 1, meta, clkey[1])
                    item._serial = content;
                    d._storeItem(item)
            else:
                itd = d._descr.itemDescrByCode.get("hdr", None)
                meta, content = arch.getItem(("hdr",()))
                item = itd.cls()._setDocument(d)._setup(-1 if meta[1] == 0 else 1, meta, clkey[1])
                item._serial = content;
                d._storeItem(item)
                
            d._hdr._loadFromJson(d._hdr._serial)
            d._hdr._status = 1
            d._hdr._oldindexes = d._hdr._getAllIndexes()
        else:           # created with an empty hdr, ready and committed
            d._status = 3
            item = d._descr.itemDescrByCode.get("hdr").cls()._setDocument(d)._setup(3, (0,0,0,0,0), ())
            d._storeItem(item)
        return d
    
    def _storeItem(self, item):
        cl = item._descr.code
        if item._descr.isSingleton:
            if cl == "hdr":
                self._hdr = item
            self._singletons[cl] = item
        else:
            self._items[cl][item._keys] = item
        
    def _getItem(self, itd, keys, orNew):
        assert keys is not None and isinstance(keys, tuple), "getItem without keys tuple "+ self.fk(itd, keys)
        if itd.code == "hdr":
            return self._hdr
        assert self._isfull, "Get item not hdr on a not full document " + self.fk(itd, keys)
        assert (not itd.isSingleton and keys is not None and len(itd.keys) == len(keys)) or (itd.isSingleton and keys is None), "_getItem [" + self.fk(itd, keys) + "] incorrect number of keys"
        item = self._singletons.get(itd.code, None) if itd.isSingleton else self._items[itd.code].get(keys, None)
        
        if item is None:
            if not orNew:
                return None
            # created empty
            item = itd.cls()._setDocument(self)._setup(3, (0,0,0), keys)
            self._storeItem(item)
            return item

        if item._status > 1:        # created / recreated / modified (already loaded)
            return item
        
        if item._status == -1:      # was NOT existing (deleted before the operation). HAS keys
            if orNew is None:
                return None
            # created empty
            item = itd.cls()._setDocument(self)._setup(3, (0,0,0), keys)
            self._storeItem(item)
            return item
        
        if item._status == 0:       # deleted during the operation. HAS keys
            if orNew is None:
                return item
            # RE created empty, ready, committed 
            item._resetChanges()
            return item
        
        # item._status == 1 : not modified, never loaded
        if not hasattr(item, "_ready"):
            item._loadFromJson(item._serial)
            item._oldindexes = item._getAllIndexes()
            return item
    
    def item(self, cls, keys=tuple()):
        assert self._docid is not None, "Document " + self.id()
        itd = self._descr.itemDescrByCls.get(cls.__name__, None)
        assert itd is not None, "Item not registered " + cls
        return self._getItem(itd, keys, False)

    def itemOrNew(self, cls, keys=tuple()):
        assert self._docid is not None, "Document " + self.id()
        itd = self._descr.itemDescrByCls.get(cls.__name__, None)
        assert itd is not None, "Item not registered " + cls
        return self._getItem(itd, keys, True)
        
    def nbChanges(self):
        return 0 if not hasattr(self, "_changeditems") else len(self._changeditems)
    
    def _notifyChange(self, dl, chg, item): 
        # chg: 0:no new item to save, 1:add item, -1:remove item, dv1/dv2 delta of volumes
        i = (item._descr.code, item._keys)
        if chg != 0 and not hasattr(self, "_changeditems"):
            self._changeditems = {}
        if chg < 0:
            if i in self._changeditems:
                del self._changeditems[i]
        if chg > 0:
            self._changeditems[i] = item
        if dl != 0:
            self._hdr._nt = self._hdr.NT() + dl
        n = self.nbChanges()
        if self._status == 1 and n != 0:
            self._status = 2
        elif self._status == 2 and n == 0:
            self._status = 1
        
    def _validate(self, version):
        # _status : -1:NOT existing, 0:deleted, 1:unmodified, 2:modified, 3:created 4:recreated (after deletion)
        if self._status <= 0: # document deleted
            return (0, None, None)

        if self._status == 1: # document unchanged
            return (1, None, None)
        
        ctime = version if self._status > 2 else self._hdr.ctime()
        
        ndtime = self._newDelHistoryLimit(version, self._hdr.version(), self._hdr.dtime(), ctime, getattr(self._descr.cls, "DELHISTORYINDAYS", 20))
        hch = ndtime != self._hdr.dtime()
        toDel = self._status == 0 or self._status == 4
        toUpd = self._status >= 2
        upd = Update(self._descr, self.docid(), toDel, toUpd, version, ndtime if hch else None)
        arch = DocumentArchive(self._descr.table, self._docid, self._isfull)

        for cl in self._singletons:
            self._singletons[cl]._validate(upd, arch, version, ndtime)
        for cl in self._items:
            x = self._items[cl]
            for keys in x:
                x[keys]._validate(upd, arch, version, ndtime)
        return (self._status, upd, arch)
    
    def _newDelHistoryLimit(self, nv, ov, od, ct, nbd):
        if ov == 0:
            return nv
        snv = Stamp.fromStamp(nv)
        nvnbd = Stamp.fromEpoch(snv.epoch - (nbd * 86400000)).stamp
        if (nvnbd > ov):
            # previos version is SO old. No modification / deletion in the nbd last days. NO DELETION HISTORY
            return nv
        if (nvnbd < od):
            # previous deletion limit is still valid (less nbd old), don't touch it
            return od
        nv2nbd = Stamp.fromEpoch(snv.epoch - (2 * nbd * 86400000)).stamp
        if (nv2nbd < od):
            # previous deletion limit is still valid (less 2*nbd old), don't touch it
            return od
        # need for shorting the deletion limt to nbd old
        return ct if nvnbd < ct else nvnbd
    
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
    _oldindexes : initial values of indexes (dict by index name)
    _newindexes : new values of indexes (dict by index name), only those having chjanged
    """
    
    def id(self):
        return self._descr.code + str(self._keys)
    
    def fullId(self):
        return ("!!!DISCONNECTED!!!" if self._document is None else self._document.id()) + "@" + self.id()

    def istosave(self):
        return self._status == 0 or self._status == 0

    def isexisting(self):
        return self._status > 1

    def wasexisting(self):
        return self._status > 0
    
    def version(self):
        return self._meta[0]
    
    def size(self):
        return self._meta[1]

    def cas(self):
        return self._meta[2]
    
    def ctime(self):
        return self._meta[2] if len(self._meta) == 5 else self._document._hdr._meta[2]

    def dtime(self):
        return self._meta[3] if len(self._meta) == 5 else self._document._hdr._meta[3]

    def totalSize(self):
        return self._meta[4] if len(self._meta) == 5 else self._document._hdr._meta[4]

    def NL(self):
        return self._nl if hasattr(self, "_nl") else self.size()

    def NT(self):
        return self._nt if hasattr(self, "_nt") else self.totalSize()
    
    def _setDocument(self, document):
        self._document = document
        return self
        
    def _loadFromJson(self, json_data):
        self._reset()
        if json_data is not None:
            d = json.loads(json_data)       # d is a dict
            for var in d:
                setattr(self, var, d[var])
        if not self._descr.isSingleton:
            self._setKeys()
        self._ready = True
        return self
                
    def _getAllIndexes(self):
        if len(self._descr.indexes) == 0:
            return None
        res = {}
        for idx in self._descr.indexes:
            v = self._getIndexedValues(idx)
            if v is not None:
                res[idx] = v
        return res if len(res) > 0 else None        

    def _getIndexedValues(self, idxName):
        idx = self._descr.indexes.get(idxName, None)
        if idx is None:
            return None
        if not idx.isList():
            values = []
            for var in idx.columns:
                v = getattr(self, var, None)
                if v is None:
                    return None
                values.append(v)            
            return values
        else:
            result = []
            lst = getattr(self, idx.varList, None)
            if lst is None:
                return None
            for line in lst:
                values = []
                for var in idx.columns:
                    if var.startswith("*"):
                        v = line.get(var[1:], None)
                    else:
                        v = getattr(self, var, None)
                    if v is None:
                        break
                    values.append(v)
                if len(values) == len(idx.columns):
                    result.append(values)
            return result if len(result) > 0 else None
                
    def _setup(self, status, meta, keys): # status -1:NOT 1:unmodified 3:created            
        self._keys = keys
        self._setKeys()
        self._meta = meta     
        self._status = status
        self._kl = len(self._descr.code) + len(json.dumps(self._keys))
        if status == 3:
            self._newserial = "{}"
            self._ready = True            
            self._nl = self._kl + 2
            self._document._notifyChange(self._nl, 1, self)
        return self
    
    def _setKeys(self):
        i = 0
        for nk in self._descr.keys:
            setattr(self, nk, self._keys[i])
            i += 1
    
    def _reset(self):
        lst = []
        for var in self.__dict__:
            if not var.startswith("_"):
                lst.append(var)
        for var in lst:
            del self.__dict__[var]

    def _resetChanges(self):
        self._reset()
        self._status = 4
        self._newserial = "{}"
        self._ready = True
        nl = self._kl + 2
        dv = nl - self.NL()
        self._nl = nl
        self._document._notifyChange(dv, 1, self)

    def toJson(self):
        assert self._document._docid is not None, "Document " + self._document.id()
        bk = {}
        for x in self.__dict__:
            if not x.startswith("_"):
                v = self.__dict__[x]
                if v is not None and not (isinstance(v, int) and v == 0):
                    bk[x] = v
        return json.dumps(bk)

    def commit(self):
        assert self._document is not None, "Disconnected item : all actions forbidden " + self.id()
        assert self._document._docid is not None, "Document " + self._document.id()
        assert self._document._age == 0, "Commit forbidden on read only document " + self.fullId()
        assert self._status > 0, "Commit forbidden on a deleted item " + self.fullId()
        ser = self.toJson()
        nl = self._kl + len(ser)
        dv = nl - self.NL()
        self._nl = nl
        
        if self._status > 2:            # created / recreated : status not changed
            if self._newserial != ser:  # content changed
                self._newserial = ser
                self._newindexes = self.getAllIndexes()
                self._document._notifyChange(dv, 0)
            return
        
        if self._status == 2:       # modified
            if ser == self._serial: # but ... NOT
                self._status = 1;
                del self.__dict__["_newserial"]
                del self.__dict__["_newindexes"]
                self._document._notifyChange(dv, -1, self) # one less to save
            else:
                if self._newserial != ser:  # content changed
                    self._newserial = ser
                    self._newindexes = self.getAllIndexes()
                    self._document._notifyChange(dv, 0)
            return
        
        # unmodified status == 1
        if ser != self._serial:
            self._status = 2
            self._newserial = ser
            self._newindexes = self._getAllIndexes()
            self._document._notifyChange(dv, 1, self)
        
    def delete(self):
        assert self._document is not None, "Disconnected item : all actions forbidden " + self.id()
        assert self._document._docid is not None, "Document " + self._document.id()
        assert self._document._age == 0, "Deleting item forbidden on read only documents " + self.fullId()
        assert self._descr.code != "hdr", "Deleting hdr forbidden " + self.fullId()
        if self._status < 1:
            return
        self._status = 0
        self._reset()
        if hasattr(self, "_newserial"):
            del self._newserial
        if hasattr(self, "_newindexes"):
            del self._newindexes
        nl = self._kl
        dv = nl - self.NL()
        self._nl = nl
        self._document._notifyChange(dv, 1, self)
        
    def rollback(self):
        assert self._document is not None, "Disconnected item : all actions forbidden " + self.id()
        assert self._document._docid is not None, "Document " + self._document.id()
        assert self._document._age == 0, "Rollback forbidden on read only documents " + self.fullId()
        if self._status < 0 or self.status == 1:
            return
        nl = self._kl + len(self._serial)
        dv = nl - self.NL()
        self._nl = 0
        self._reset()
        if hasattr(self, "_newserial"):
            del self._newserial
        if hasattr(self, "_newindexes"):
            del self._newindexes
        if hasattr(self, "_serial") and self._serial is not None and len(self._serial) != 0:
            # was existing at the operation begining
            self._status = 1
            self._loadFromJson(self._serial)
            self._oldindexes = self._getAllIndexes()
            self._ready = True
            self._document._notifyChange(dv, 1, self)
        else:       # was NOT existing, disconnecting
            self._document._notifyChange(dv, -1, self)
            self._ready = False
            self._document = None
            self._status = -1
            
    def _eq(self, oldv, newv, isList):
        if not isList:
            i = 0
            for v in oldv:
                if newv[i] != v:
                    return False
                i += 1
            return True
        if len(oldv) != len(newv):
            return False
        j = 0
        for x in oldv:
            y = newv[j]
            i = 0
            for v in x:
                if y[i] != v:
                    return False
                i += 1
            j += 1
        return True
    
    def _validate(self, upd, arch, version, ndtime):
        # _status : -1:NOT existing, 0:deleted, 1:unmodified, 2:modified, 3:created 4:recreated (after deletion)
        # c : 1:insert meta and content, 2:update meta only, 3:update meta and delete content, 4:update meta and content
        # this method is invoked ONLY WHEN document has changes

        if self._status == -1 and self._meta[0] > ndtime: # keep it in deletion history
            arch.addItem(self._descr.code, self.keys, self._meta, None)

        if self._status == 0:   # deletion
            meta = (version, 0, 0)
            upd.addUpdate(3, self._descr.code, self._keys, meta, None)
            arch.addItem(self._descr.code, self.keys, meta, None)
            return
        
        if self._status == 1:   # unmodified, but hdr is always modified (because document has changes)
            if self._descr.code == "hdr":
                ctime = version if self.ctime() == 0 else self.ctime()
                meta = (version, self._meta[1], ctime, ndtime, self.NT())
                upd.addUpdate(2, "hdr", (), meta, None)
            arch.addItem(self._descr.code, self.keys, meta, self._serial)
            return

        if self._descr.code == "hdr":
            ctime = version if self.ctime() == 0 else self.ctime()
            meta = (version, len(self._newserial), ctime, ndtime, self.NT())
        else:
            meta = (version, len(self._newserial), self._meta[2])
        c = 1 if self._status == 3 else 4
        upd.addUpdate(c, self._descr.code, self._keys, meta, self._newserial)
        arch.addItem(self._descr.code, self._keys, meta, self._newserial)
        if not self._descr.hasIndexes():
            return
        
        oldi = self._oldindexes if hasattr(self, "_oldindexes") else None
        newi = self._newindexes if hasattr(self, "_newindexes") else None
        for idx in self._descr.indexes:
            # upd.indexes[n] = {"name":n, "isList":idx.isList(), "kns":key names, "cols":idx.columns, "updates":[]}
            # (kns): tuple of names of key attribrutes () for a singletion            # updates for an index : {keys:[k], ird:1, val:[l1]] for simple, val:[[l2] ...]] for multiple
            # [k] : values of keys or [] for a singleton
            # ird: 1:insert new value (no old values) 2:replace old value by new value, 3:delete old value
            ui = upd.getIndex(idx)
            if ui is not None:
                oldv = oldi.get(idx, None) if oldi is not None else None
                newv = newi.get(idx, None) if newi is not None else None
                ird = 0 if oldv is None and newv is None else (3 if oldv is not None and newv is None else (1 if oldv is None and newv is not None else (2 if not self._eq(oldv, newv, ui["isList"]) else 0)))
                if ird != 0:
                    ui["updates"].append({"ird":ird, "keys":self._keys, "val": newv if ird != 3 else None})
         
class Singleton(BaseItem):
    pass

class Item(BaseItem):
    pass

class DOperation(Operation):
    gCache = { }     # key : archive.id() - value : (archive, lastGet, lastCheck)
    gCacheLock = Lock()
    gCacheSize = 0
    
    def _getArchive(fid):
        with DOperation.gCacheLock:
            now = Stamp.epochNow()
            drec = DOperation.gCache.get(fid, None)
            if drec != None:
                DOperation.gCache[fid] = (drec[0], now, drec[2])
                return drec
            else:
                return None
    
    def _setArchiveLastCheck(fid, stamp):
        with DOperation.gCacheLock:
            drec = DOperation.gCache.get(fid, None)
            # tuple : archive, lastGet, lastCheck
            if drec != None:
                DOperation.gCache[id] = (drec[0], drec[1], stamp)
    
    def _storeArchive(arch, stamp):
        with DOperation.gCacheLock:
            tsAfter = arch.totalSize
            drec = DOperation.gCache.get(arch.id(), None)
            if drec != None:
                if drec[0].version > arch.version:
                    return
                tsBefore = drec[0].totalSize
                DOperation.gCache[arch.id()] = (arch, stamp, stamp)
                DOperation.gCacheSize += tsAfter - tsBefore
                if tsAfter > tsBefore and DOperation.gCacheSize > settings.MAXCACHESIZE:
                    DOperation._cleanUp()

    def _removeArchive(fid, version):
        with DOperation.gCacheLock:
            drec = DOperation.gCache.get(fid, None)
            if drec != None:
                if drec[0].version <= version:
                    DOperation.gCacheSize -= drec[0].totalSize
                    del DOperation.gCache[fid]
    
    def _cleanup():
        lst = sorted(DOperation.gCache.values(), key=lambda drec: drec[1])
        mx = settings.MAXCACHESIZE // 2
        vol = 0
        for drec in lst:
            vol += drec[0].totalSize
            del DOperation.gCache[drec[0].id()]
            if vol > mx:
                return

    def _getDescr(self, clsOrTable):
        if isinstance(clsOrTable, str):
            docDescr = Document._byTable.get(clsOrTable, None)
        else:
            docDescr = Document._byCls.get(clsOrTable.__name__, None)
        assert docDescr is not None, "Not registered document class / table " + clsOrTable
        return docDescr

    def _releaseDocument(self, doc):
        del self._docCache[doc._descr.id(doc._docid)]
        
    def getOrNew(self, clsOrTable, docid, isfull):
        assert docid is not None and isinstance(docid, str) and len(docid) > 0, "Empty docid on operation.getOrNew"
        descr = self._getDescr(clsOrTable)
        d = self._get(descr, docid, 0, isfull)
        if d is not None:
            return d
        d = Document._create(descr, docid, None, 0, isfull)
        d.operation = self
        self._docCache[descr.id(docid)] = (d, 0)
        return d
        
    def get(self, clsOrTable, docid, age, isfull): 
        assert docid is not None and isinstance(docid, str) and len(docid) > 0, "Empty docid on operation.get"
        descr = self._getDescr(clsOrTable)
        return self._get(descr, docid, age, isfull)
    
    def _get(self, descr, docid, age, isfull): 
        fid = descr.id(docid)
        d = self._docCache.get(fid, None)
        if d is not None:           # document in operation cache
            assert (isfull and d.isfull) or not isfull, "An operation cannot require full and restricted state of a same document : " + id
            assert (age == 0 and d[1] == 0) or age != 0 , "An operation cannot require age > 0 and then 0 of a same document : " + id
            return d[0]   # OK
        drec = DOperation._getArchive(fid)
        if drec is not None:        # archive found in global cache
            arch = drec[0]
            if arch.version > self.stamp: # document plus trop récent
                raise AppExc("XCW", self.opName, self.org, fid)
            now = drec[1]       # lastGet
            lc = drec[2]        # lastCheck
            if (isfull and arch.isfull) or not isfull:      # fullness is compatible
                if now - lc < age * 1000:                   # age is compatible
                    d = Document._createFromArchive(arch)   # build document
                    self._docCache[fid] = (d , age)         # store document in operation cache
                    d.operation = self
                    self._t2 += 1
                    self._t3 += 1 if not isfull else len(arch.items)
                    return d
                
            # has to be refreshed from DB
            status, delta = self.provider.getDelta(arch, isfull)
            if status == 0:         # document does not exist
                return None
            if status == 1:         # nothing to refresh
                DOperation._setArchiveLastCheck(fid, now)    # set the last refresh stamp in global cache
                d = Document._createFromArchive(arch)        # build document
                d.operation = self
                self._docCache[fid] = (d , age)               # store document in operation cache
                self._t2 += 1
                self._t3 += 1 if not isfull else len(arch.items)
                return d
            if status == 3:         # full replacement
                newarch = delta
            else:
                newarch = arch.merge(delta)
            DOperation._storeArchive(newarch, now)          # store it in global cache
            if arch.version > self.stamp: # document plus trop récent
                raise AppExc("XCW", self.opName, self.org, fid)
            d = Document._createFromArchive(newarch)        # build document
            d.operation = self
            self._docCache[fid] = (d, age)                   # store document in operation cache
            self._t2 += 1
            self._t3 += 1 if not isfull else len(newarch.items)
            return d
        # archive not found in global cache
        status, delta = self.provider.getArchive(descr.table, docid, isfull)
        if status == 0:         # document does not exist
            return None
        DOperation._storeArchive(newarch, now)          # store it in global cache
        d = Document._createFromArchive(arch)           # build document
        d.operation = self
        self._docCache[id] = (d , age)                   # store document in operation cache
        return d
    
    def validation(self):
        if len(self._docCache) == 0:
            return
        vl = ValidationList(self.stamp)
        todo = False
        arch2del = list()
        arch2set = list()
        for drec in self._docCache.values():
            d = drec[0]
            if d._age == 0 and d._status >= 0:
                status, arch = vl.add(d, self.stamp)
                if status == 0:
                    arch2del.append(d._descr.id())
                elif status >= 2:
                    arch2set.append(arch)
                todo = True
        if todo:
            self._t4 = self.provider.validate(vl)
            now = Stamp.epochNow()
            for fid in arch2del:
                DOperation._removeArchive(fid, self.stamp)  # version
            for arch in arch2set:
                DOperation._storeArchive(arch, now)  # lastGet / lastCheck  
        
    def syncs(self, syncs):
        # TODO
        return {}
    
    def process(self, param):
        return None
    
    def __init__(self, execCtx):
        super().__init__(execCtx)
        self._docCache = {}      # key id(), value: tuple (arch, age)
        self._accTkt  = AccTkt(self.stamp, execCtx.contentLength)
        self._t2 = 0 # nombre de documents lus
        self._t3 = 0 # nombre d'items lus
        self._t4 = 0 # nombre d'items écrits.
        self._t5 = 0 # nombre de tâches créées / mise à jour.
        
    def work(self): 
        result = self.process(self.param)
        self.validation()
        if result is not None:
            jsonResp = result.getJson()
            if jsonResp is not None:
                x = self.inputData.get("syncs", None)
                if x is not None:
                    jsonResp["syncs"] = self.syncs(json.loads(x))
                    result.setJson(jsonResp)
        if self._accTkt.tktid is not None:
            self._accTkt.t[1] = result.finalLength() if result is not None else 0
            self._accTkt.t[1] = (Stamp.epochNow() - self.stamp.epoch) / 1000
            self._accTkt.t[2] = self._t2
            self._accTkt.t[3] = self._t3
            self._accTkt.t[4] = self._t4
            self._accTkt.t[5] = self._t5
            self._accTkt._record()
        return result
            
    def recordAccData(self, tktid, lcpt, state):
        self._accTkt.tktid = tktid
        self._accTkt.state = state
        for i, c in lcpt:
            if i > 7:
                break
            self._accTkt.f[i] = c
            
class ValidationList:
    def __init__(self, version):
        self.upd = (list(), list(),list(),list(),list())
        self.version = version
        
    def add(self, d, version):
        # _status : 0:deleted, 1:unmodified, 2:modified, 3:created 4:recreated (after deletion)
        status, upd, arch = d.validate(version)
        if status < 2:
            self.upd[status].append((d._descr, d._docid))
        else:
            self.upd[status].append(upd)
        return (status, arch)
            
class AccTkt:
    """
    - t0 : volume reçue dans la requête.
    - t1 : volume sorti dans la réponse.
    - t2 : nombre de documents lus.
    - t3 : nombre d'items lus.
    - t4 : nombre d'items écrits.
    - t5 : nombre de tâches créées / mise à jour.
    - t6 : nombre de secondes du traitement.
    - t7 : 0
    """
    def __init__(self, version, cl):
        self.t = [cl / 1000,0,0,0,0,0,0,0]
        self.f = [0,0,0,0,0,0,0,0]
        self.v = version
        self.state = {}
        self.tktid = None
    
    def _record(self):
        pass
        # TODO
    
            
            
        
        
