import json
from root import Stamp, Operation, dics, AppExc, Result, ExecCtx
from threading import Lock
from settings import settings
from typing import Dict, Tuple, Any, Iterable, Type, List

dics.set("fr", "XCW", "Trop de contention détectée en phase work. Opération:{0} Org:{1} Document:{2} ")

Meta = Tuple[int,int,int]
Keys = Iterable[Any]
ClKeys = Tuple[str,Tuple[Any]]
Sync = Dict[str, Any]
Param = Dict[str, Any]

class DocumentDescr: 
    def __init__(self, cls:Type, table:str):
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
    
    def id(self, docid:str) -> str:
        return self.table + "/" + docid;

class Index:
    def __init__(self, name:str, columns:Tuple[str], varList:str = None):
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
        self.kns = ()
        
    def isList(self):
        return self.varList is not None
    
class ItemDescr:
    def __init__(self, DocumentClass:Type, ItemClass:Type, code:str, keys:Tuple[str], indexes:Tuple[Index]):
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

class LocalIndex:
    def __init__(self, idx:str):
        self.idx = idx
        self.updates = [] # IdxVal
    
    def addIdxVal(self, ird:int, keys:Keys, val) -> None:
        self.updates.append(IrdKV(ird, keys, val))

class Update:
    def __init__(self, docDescr:DocumentDescr, docid:str, version:int, dtcas:int):
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
        self.version = version
        self.dtcas = dtcas
        self.table = docDescr.table
        self.hasIndexes = docDescr.hasIndexes()
        self.updates = {}
        if self.hasIndexes:
            self.indexes = {}
            for idx in docDescr.indexes.values():
                self.indexes[idx.name] = LocalIndex(idx)
                    
    def addUpdate(self, c:int, cl:str, keys:Keys, meta:Meta, content:str) -> None:
        self.updates[(cl, keys)] = LocalUpdate(c, cl, keys, meta, content)
        
    def getIndex(self, idx:str) -> LocalIndex:
        return self.indexes.get(idx, None)
            

class IrdKV:
    def __init__(self, ird:int, keys:Keys, val):
        self.ird = ird 
        self.keys = keys 
        self.val = val

class LocalUpdate:
    # c : 1:insert meta and content, 2:update meta only, 3:update meta and delete content, 4:update meta and content
    # meta : version dtcas size
    def __init__(self, c:int, cl:str, keys:Iterable, meta:Meta, content:str):
        self.c = c
        self.cl = cl
        self.keys = keys
        self.meta = meta
        self.content = content
        
class DocumentArchive:
    """
    A DocumentArchive instance is a snapshot of a document given by a couple table / docid
    After building (constructor and sequence of addItem), a DocumentArchive is immutable.
    A DocumentArchive can be required as full or hdr only.
    """
    def __init__(self, table:str, docid:str, isfull=True):
        assert (table is not None and isinstance(table, str) and len(table) > 1), "Table code is not a valid string"
        assert (docid is not None and isinstance(docid, str) and len(docid) > 1), "docid is not a valid string"
        self.descr = Document.descr(table)
        assert self.descr is not None, "Table is not registered"
        self.docid = docid
        self.isfull = isfull
        self.items = {}
        self.hdr = None
        self.version = 0
        self.dtcas = 0
        self.size = 0

    def id(self, clkey:str = None) -> str:
        return self.descr.table + "/" + self.docid + ("" if clkey is None else "@" + clkey)

    def id2(self, cl:str, keys:Keys) -> str:
        return self.descr.table + "/" + self.docid + "@" + cl + str(keys)
        
    def addItem(self, cl:str, keys:Keys, meta:Meta, content:str) -> None:
        if cl == "hdr":
            self.version = meta[0]
            self.dtcas = meta[1]
            self.size = meta[2]

        if meta[2] == 0:
            v = (meta, None)        
        else:
            assert content is not None and isinstance(content, str) and len(content) >= 2, "Invalid content for " + self.id2(cl, keys)
            v = (meta, content)
            
        if cl == "":
            self.hdr = v
        else:
            self.items[(cl,keys)] = v
            
    def clkeys(self) -> Iterable[ClKeys]:
        return self.items.keys()
    
    def getItem(self, clkeys:ClKeys):
        return self.items.get(clkeys, None)
    
        
class Document:
    """
    _docid : identification of the document. None when released
    _status : 0:deleted, 1:unmodified, 2:modified, 3:created
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
    
    def filterSyncItem(self, operation:Operation, fargs:Dict, docid:str, clstr, keys:Keys, version:int, dtcas:int, size:int, content:str) -> str:
        return content
    
    def register(cls:Type, table:str) -> DocumentDescr:
        assert (cls is not None and issubclass(cls, Document)), "Document class is None or not a subclass of Document"
        assert (table is not None and isinstance(table, str)), "Table code is not a string"
        assert (cls.__name__ not in Document._byCls), "Document is already registered"
        assert (table not in Document._byTable), "Table is already registered"
        descr = DocumentDescr(cls, table)
        Document._byCls[cls.__name__] = descr
        Document._byTable[table] = descr
        return descr
        
    def descr(clsOrTable):
        return Document._byTable.get(clsOrTable, None) if isinstance(clsOrTable, str) else  Document._byCls.get(clsOrTable.__name__, None)
        
    def registerItem(DocumentClass:Type, ItemClass:Type, code:str, keys:Tuple[str], indexes : Tuple[Index] = None) -> ItemDescr:
        assert (DocumentClass is not None and DocumentClass.__name__ in Document._byCls), "Document class is None or not registered"
        assert (ItemClass is not None and issubclass(ItemClass, Item)), "ItemClass is not a subclass of Item"
        assert keys is not None and isinstance(keys, tuple) and len(keys) > 0, "keys must be an non empty list of property names"
        if code == "hdr":
            assert issubclass(ItemClass, Singleton), "hdr must be a Singleton"
        return ItemDescr(DocumentClass, ItemClass, code, keys, indexes)

    def registerSingleton(DocumentClass:Type, ItemClass:type, code:str, indexes : Tuple[Index] = None) -> ItemDescr:
        assert (DocumentClass is not None and DocumentClass.__name__ in Document._byCls), "Document class is None or not registered"
        assert (ItemClass is not None and issubclass(ItemClass, Singleton)), "ItemClass is not a subclass of Singleton"
        return ItemDescr(DocumentClass, ItemClass, code, tuple(), indexes)
        
    def id(self) -> str:
        return self._descr.table + "/" + self.docid()
    
    def docid(self) -> str:
        return self._docid if self._docid is not None else "!!!RELEASED!!!"
    
    def hdr(self): # -> Singleton
        assert self._docid is not None, "Document " + self.id()
        return self._hdr
    
    def iscreated(self):
        return self._status == 3

    def ismodified(self):
        return self._status >= 1

    def isdeleted(self):
        return self._status == 0

    def isexisting(self):
        return self._status > 1
 
    def fk(self, itd:ItemDescr, keys:Keys) -> str:
        return self.id() + "@" + (itd.code if itd is not None else "???") + str(keys)
           
    def release(self) -> None:
        assert self._docid is not None, "Document already released " + self.id()
        self._docid = None
        self._detachAll()
        if hasattr(self, "_age"):
            del self._age
        if hasattr(self, "_isfull"):
            del self._isfull
        self.operation.releaseDocument(self)

    def delete(self) -> None:
        assert self._docid is not None and self._age == 0 and self._status > 0, "Deletion not allowed on released or read only or deleted document [" + self.id() + "]"
        self._detachAll();
        self._status = 0;

    def _detachAll(self) -> None:
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
        
    def _createFromArchive(arch:DocumentArchive): # -> Document
        assert arch is not None and isinstance(arch, DocumentArchive), "createFromArch with no arch"
        return Document._create(arch.descr, arch.docid, arch, 0, True)

    def _create(docDescr:DocumentDescr, docid:str, arch:DocumentArchive, age:int, isfull): #  -> Document if arch is given, load from archive, else to create            
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

            itd = d._descr.itemDescrByCode.get("hdr", None)
            meta, content = arch.hdr
            itd.cls()._setDocument(d)._setup(1, meta, "hdr", content)
            
            if isfull:
                for clkey in arch.clkeys():
                    itd = d._descr.itemDescrByCode.get(clkey[0], None)
                    meta, content = arch.getItem(clkey)
                    itd.cls()._setDocument(d)._setup(-1 if meta[2] == 0 else 1, meta, clkey[1], content)
                
            d._hdr._loadFromJson(d._hdr._serial)
            d._hdr._status = 1
            d._hdr._oldindexes = d._hdr._getAllIndexes()
        else:           # created with an empty hdr, ready and committed
            d._status = 3
            itd = d._descr.itemDescrByCode.get("hdr", None)
            itd.cls()._setDocument(d)._setup(3, (0,0,0), ())
        return d
            
    def _getItem(self, itd:ItemDescr, keys:Keys, orNew): # -> BaseItem
        if itd.code == "hdr":
            return self._hdr
        assert keys is not None and isinstance(keys, tuple), "getItem without keys tuple "+ self.fk(itd, keys)
        assert self._isfull, "Get item on a not full document " + self.fk(itd, keys)
        assert (not itd.isSingleton and keys is not None and len(itd.keys) == len(keys)) or (itd.isSingleton and keys is None), "_getItem [" + self.fk(itd, keys) + "] incorrect number of keys"

        item = self._singletons.get(itd.code, None) if itd.isSingleton else self._items[itd.code].get(keys, None)
        
        if item is None or item._status == -1: # does not exist or was deleted by a former operation
            # created empty idf orNew
            return itd.cls()._setDocument(self)._setup(3, (0,0,0), keys) if orNew else None

        if item._status == 0:       # deleted during the operation. HAS keys # if orNew RE created empty, ready, committed 
            return item._resetChanges() if orNew else item

        if item._status > 1:        # created / recreated / modified (already loaded)
            return item
                
        # item._status == 1 : not modified, never loaded
        if not hasattr(item, "_ready"):
            item._loadFromJson(item._serial)
            item._oldindexes = item._getAllIndexes()
            return item
    
    def item(self, cls:Type, keys:Keys = tuple()): # -> BaseItem:
        assert self._docid is not None, "Document " + self.id()
        itd = self._descr.itemDescrByCls.get(cls.__name__, None)
        assert itd is not None, "Item not registered " + cls
        return self._getItem(itd, keys, False)

    def itemOrNew(self, cls:Type, keys:Keys = tuple()): # -> BaseItem:
        assert self._docid is not None, "Document " + self.id()
        itd = self._descr.itemDescrByCls.get(cls.__name__, None)
        assert itd is not None, "Item not registered " + cls
        return self._getItem(itd, keys, True)
    
    def itemkeys(self, cls:Type) -> Iterable[Keys]:
        assert self._docid is not None, "Document " + self.id()
        itd = self._descr.itemDescrByCls.get(cls.__name__, None)
        assert itd is not None, "Item not registered " + cls
        if itd.isSingleton:
            return []
        return self._items[itd.code].keys()
         
    def nbChanges(self) -> int:
        return 0 if not hasattr(self, "_changeditems") else len(self._changeditems)
    
    def _notifyChange(self, dl:int, chg:int, item = None) -> None: 
        # chg: 0:no new item to save, 1:add item, -1:remove item, dv1/dv2 delta of volumes
        i = (item._descr.code, item._keys) if chg != 0 else ()
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
        
    def _validate(self, version:int) -> Tuple[int, Update, DocumentArchive]:
        # _status : 0:deleted, 1:unmodified, 2:modified, 3:created
        if self._status <= 0: # document deleted
            return (0, None, None)

        if self._status == 1: # document unchanged
            return (1, None, None)
        
        dtcas = version if self._status == 3 else self._hdr.dtcas()
        
        ndtcas = self._newDelHistoryLimit(version, self._hdr.version(), dtcas, getattr(self._descr.cls, "DELHISTORYINDAYS", 20))
        hch = ndtcas != self._hdr.dtcas()
        upd = Update(self._descr, self.docid(), version, ndtcas if hch else None)
        arch = DocumentArchive(self._descr.table, self._docid, self._isfull)

        for cl in self._singletons:
            self._singletons[cl]._validate(upd, arch, version, ndtcas)
        for cl in self._items:
            x = self._items[cl]
            for keys in x:
                x[keys]._validate(upd, arch, version, ndtcas)
        return (self._status, upd, arch)
    
    def _newDelHistoryLimit(self, nv:int, ov:int, od:int, nbd:int) -> int:
        if ov == 0:
            return nv
        snv = Stamp.fromStamp(nv)
        nvnbd = Stamp.fromEpoch(snv.epoch - (nbd * 86400000)).stamp # nbd days before
        if (nvnbd > ov):
            # previous version is SO old. No modification / deletion in the nbd last days. NO DELETION HISTORY
            return nv
        if (nvnbd < od):
            # previous deletion limit is still valid (less nbd old), don't touch it
            return od
        nv2nbd = Stamp.fromEpoch(snv.epoch - (2 * nbd * 86400000)).stamp # 2 * nbd days before
        if (nv2nbd < od):
            # previous deletion limit is still valid (less 2*nbd old), don't touch it
            return od
        # need for shorting the deletion limit to nbd days before
        return nvnbd
    
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
    
    def id(self) -> str:
        return self._descr.code + str(self._keys)
    
    def fullId(self) -> str:
        return ("!!!DISCONNECTED!!!" if self._document is None else self._document.id()) + "@" + self.id()

    def istosave(self):
        return self._status == 0 or self._status >= 2

    def iscreated(self):
        return self._status >= 3

    def ismodified(self):
        return self._status >= 1

    def isdeleted(self):
        return self._status <= 0

    def isexisting(self):
        return self._status > 1

    def wasexisting(self):
        return self._status > 0
    
    def version(self) -> int:
        return self._meta[0]
    
    def dtcas(self) -> int:
        return self._meta[1]

    def size(self) -> int:
        return self._meta[2]

    def NL(self) -> int:
        return self._nl if hasattr(self, "_nl") else self.size()

    def NT(self) -> int:
        return self._nt if hasattr(self, "_nt") else self._document._hdr.size()
    
    def _setDocument(self, document:Document): # -> BaseItem:
        self._document = document
        return self
        
    def _loadFromJson(self, json_data:str): # -> BaseItem:
        self._reset()
        if json_data is not None:
            d = json.loads(json_data)       # d is a dict
            for var in d:
                setattr(self, var, d[var])
        if not self._descr.isSingleton:
            self._setKeys()
        self._ready = True
        return self
                
    def _getAllIndexes(self) -> Dict[str, List]:
        if len(self._descr.indexes) == 0:
            return None
        res = {}
        for idx in self._descr.indexes:
            v = self._getIndexedValues(idx)
            if v is not None:
                res[idx] = v
        return res if len(res) > 0 else None        

    def _getIndexedValues(self, idxName:str) -> List:
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
                
    def _setup(self, status:int, meta:Meta, keys:Keys, content:str = None): # -> BaseItem: # status -1:NOT existing, 1:unmodified 3:created
        self._keys = keys
        self._setKeys()
        self._meta = meta     
        self._status = status
        self._kl = 0 if len(keys) == 0 else len(json.dumps(self._keys))
        if content is not None:
            self._serial = content
        if self._descr.isSingleton:
            if self._descr.code == "hdr":
                self._document._hdr = self
            else:
                self._document._singletons[self._descr.code] = self
        else:
            self._document._items[self._descr.code][self._keys] = self
        if status == 3:
            self._newserial = "{}"
            self._ready = True            
            self._nl = self._kl + 2
            self._document._notifyChange(self._nl, 1, self)
        return self
    
    def _setKeys(self) -> None:
        i = 0
        for nk in self._descr.keys:
            setattr(self, nk, self._keys[i])
            i += 1
    
    def _reset(self) -> None:
        lst = []
        for var in self.__dict__:
            if not var.startswith("_"):
                lst.append(var)
        for var in lst:
            del self.__dict__[var]

    def _resetChanges(self): # -> BaseItem:
        self._reset()
        self._status = 4
        self._newserial = "{}"
        self._ready = True
        nl = self._kl + 2
        dv = nl - self.NL()
        self._nl = nl
        self._document._notifyChange(dv, 1, self)
        return self

    def toJson(self) -> str: # do not serialize, keys, meta data (starting with _), None and 0 values
        kx = self._descr.keys
        bk = {}
        for x in self.__dict__:
            if not x.startswith("_") and x not in kx:
                v = self.__dict__[x]
                if v is not None and not (isinstance(v, int) and v == 0):
                    bk[x] = v
        return json.dumps(bk)

    def commit(self) -> None:
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
                self._newindexes = self._getAllIndexes()
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
        
    def delete(self) -> None:
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
        
    def rollback(self) -> None:
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
            
    def _eq(self, oldv:Iterable, newv:Iterable, isList): # True/False
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
    
    def _validate(self, upd:Update, arch:DocumentArchive, version:int, ndtcas:int) -> None:
        # _status : -1:NOT existing, 0:deleted, 1:unmodified, 2:modified, 3:created 4:recreated (after deletion)
        # c : 1:insert meta and content, 2:update meta only, 3:update meta and delete content, 4:update meta and content
        # this method is invoked ONLY WHEN a document has some changes

        if self._status == -1 and self._meta[0] > ndtcas: # keep it in deletion history
            arch.addItem(self._descr.code, self.keys, self._meta, None)

        if self._status == 0:   # deletion
            meta = (version, 0, 0)
            upd.addUpdate(3, self._descr.code, self._keys, meta, None)
            arch.addItem(self._descr.code, self.keys, meta, None)
            return
        
        if self._status == 1:   # unmodified, but hdr is always modified (because document has changes)
            if self._descr.code == "hdr":
                meta = (version, ndtcas, self.NT())
                upd.addUpdate(2, "hdr", (), meta, None)
            arch.addItem(self._descr.code, self.keys, meta, self._serial)
            return

        if self._descr.code == "hdr":
            meta = (version, ndtcas, self.NT())
        else:
            meta = (version, self._meta[1], len(self._newserial) + self._kl)
        c = 1 if self._status == 3 else 4
        upd.addUpdate(c, self._descr.code, self._keys, meta, self._newserial)
        arch.addItem(self._descr.code, self._keys, meta, self._newserial)
        if not self._descr.hasIndexes():
            return
        
        oldi = self._oldindexes if hasattr(self, "_oldindexes") else None
        newi = self._newindexes if hasattr(self, "_newindexes") else None
        for idx in self._descr.indexes:
            # upd.indexes[n] = {"name":n, "isList":idx.isList(), "kns":key names, "cols":idx.columns, "updates":[]}
            # (kns): tuple of names of key attribrutes () for a singletion            
            # updates for an index : {keys:[k], ird:1, val:[l1]] for simple, val:[[l2] ...]] for multiple
            # [k] : values of keys or [] for a singleton
            # ird: 1:insert new value (no old values) 2:replace old value by new value, 3:delete old value
            ui = upd.getIndex(idx)
            if ui is not None:
                oldv = oldi.get(idx, None) if oldi is not None else None
                newv = newi.get(idx, None) if newi is not None else None
                ird = 0 if oldv is None and newv is None else (3 if oldv is not None and newv is None else (1 if oldv is None and newv is not None else (2 if not self._eq(oldv, newv, ui.idx.isList()) else 0)))
                if ird != 0:
                    ui.addIdxVal(ird, self._keys, newv if ird != 3 else None)
         
class Singleton(BaseItem):
    pass

class Item(BaseItem):
    pass

Drec = Tuple[DocumentArchive, int, int]

class DOperation(Operation):
    gCache = { }     # key : archive.id() - value : Drec
    gCacheLock = Lock()
    gCacheSize = 0
    
    def _getArchive(fid:str) -> Drec:
        with DOperation.gCacheLock:
            now = Stamp.epochNow()
            drec = DOperation.gCache.get(fid, None) # tuple : archive, lastGet, lastCheck
            if drec != None:
                DOperation.gCache[fid] = (drec[0], now, drec[2])
                return drec
            else:
                return None
    
    def _setArchiveLastCheck(fid:str, stamp:int) -> None:
        with DOperation.gCacheLock:
            drec = DOperation.gCache.get(fid, None) # tuple : archive, lastGet, lastCheck
            if drec != None:
                DOperation.gCache[id] = (drec[0], drec[1], stamp)
    
    def _storeArchive(arch:DocumentArchive, stamp:int) -> None:
        with DOperation.gCacheLock:
            tsAfter = arch.size
            tsBefore = 0
            drec = DOperation.gCache.get(arch.id(), None)
            if drec != None:
                if drec[0].version > arch.version:
                    return
                tsBefore = drec[0].size
            DOperation.gCache[arch.id()] = (arch, stamp, stamp)
            DOperation.gCacheSize += tsAfter - tsBefore
            if tsAfter > tsBefore and DOperation.gCacheSize > settings.MAXCACHESIZE:
                DOperation._cleanUp()

    def _removeArchive(fid:str, version:int) -> None:
        with DOperation.gCacheLock:
            drec = DOperation.gCache.get(fid, None)
            if drec != None:
                if drec[0].version <= version:
                    DOperation.gCacheSize -= drec[0].size
                    del DOperation.gCache[fid]
    
    def _cleanup() -> None:
        lst = sorted(DOperation.gCache.values(), key=lambda drec: drec[1])
        mx = settings.MAXCACHESIZE // 2
        vol = 0
        for drec in lst:
            vol += drec[0].size
            del DOperation.gCache[drec[0].id()]
            if vol > mx:
                return

    def _releaseDocument(self, doc:Document) -> None:
        del self._docCache[doc._descr.id(doc._docid)]
        
    def getOrNew(self, clsOrTable, docid:str) -> Document:
        assert docid is not None and isinstance(docid, str) and len(docid) > 0, "Empty docid on operation.getOrNew"
        descr = Document.descr(clsOrTable)
        assert descr is not None, "Not registered document class / table " + clsOrTable

        d = self._get(descr, docid, 0, True)
        if d is not None:
            return d
        d = Document._create(descr, docid, None, 0, True)
        d.operation = self
        self._docCache[descr.id(docid)] = (d, 0)
        return d
        
    def get(self, clsOrTable, docid:str, age:int, isfull) -> Document: 
        assert docid is not None and isinstance(docid, str) and len(docid) > 0, "Empty docid on operation.get"
        descr = Document.descr(clsOrTable)
        assert descr is not None, "Not registered document class / table " + clsOrTable
        return self._get(descr, docid, age, isfull)

    def _get(self, descr:DocumentDescr, docid:str, age:int, isfull) -> Document: 
        fid = descr.id(docid)
        d = self._docCache.get(fid, None)
        if d is not None:           # document in operation cache
            assert (isfull and d.isfull) or not isfull, "An operation cannot require restricted state and then full of a same document : " + id
            assert (age == 0 and d[1] == 0) or age != 0 , "An operation cannot require age > 0 and then 0 of a same document : " + id
            return d[0]   # OK
        arch = self._getArch(descr, docid, age, isfull)
        if arch is None:
            return None
        d = Document._createFromArchive(arch)   # build document
        self._docCache[fid] = (d , age)         # store document in operation cache
        d.operation = self
        self._t2 += 1
        self._t3 += 1 if not isfull else len(arch.items)
        return d
    
    def _getArch(self, descr:DocumentDescr, docid:str, age:int, isfull) -> DocumentArchive: 
        fid = descr.id(docid)
        drec = DOperation._getArchive(fid)
        if drec is not None:        # archive found in global cache
            arch = drec[0]
            if arch.version > self.stamp.stamp: # document more recent than the operation stamp
                raise AppExc("XCW", self.opName, self.org, fid)
            now = drec[1]       # lastGet
            lc = drec[2]        # lastCheck
            if (isfull and arch.isfull) or not isfull:      # fullness is compatible
                if now - lc < age * 1000:                   # age is compatible
                    return arch
                # has to be refreshed from DB
                newarch = self.provider.getUpdatedArchive(arch, isfull)
                if newarch is None:
                    DOperation._removeArchive(fid, self.stamp)
                    return None
                if newarch == arch:
                    DOperation._setArchiveLastCheck(fid, now)    # set the last refresh stamp in global cache
                    return newarch

        # archive not found in global cache OR was not full and requested full
        newarch = self.provider.getArchive(descr.table, docid, isfull)
        if newarch is None:         # document does not exist
            DOperation._removeArchive(fid, self.stamp)
            return None
        now = Stamp.epochNow()
        DOperation._storeArchive(newarch, now)             # store it in global cache
        if newarch.version > self.stamp:                   # more recent than the operation stamp
            raise AppExc("XCW", self.opName, self.org, fid)            
        return newarch
               
    def findInIndex(self, DocumentClass:Type, index:str, startCols:Dict[str, Any], resCols:Tuple[str]) -> Iterable[Dict[str, Any]]:
        # startcols : {"c1":"v1", "c2:n2 ... }
        # resCols : ("c3","c4")
        assert (DocumentClass is not None and DocumentClass.__name__ in Document._byCls), "Document class is None or not registered"
        docDescr = DocumentClass._descr
        idx = docDescr.indexes.get(index, None)
        assert idx is not None, "Unregistered index for " + docDescr.table
        assert startCols is not None and isinstance(startCols, dict) and len(startCols) <= len(idx.columns), "Starting columns not given or too many for index " + idx.name + " of "  + docDescr.table
        return self.provider.searchInIndex(idx.name, startCols, resCols)
        
    def validation(self) -> None:
        version = self.stamp.stamp
        if len(self._docCache) == 0:
            return
        vl = ValidationList(version)
        todo = False
        arch2del = list()
        arch2set = list()
        for drec in self._docCache.values():
            d = drec[0]
            if d._age == 0 and d._status >= 0:
                status, arch = vl.add(d, version)
                if status == 0:
                    arch2del.append(d._descr.id())
                elif status >= 2:
                    arch2set.append(arch)
                todo = True
        if todo:
            self._t4 = self.provider.validate(vl)
            now = Stamp.epochNow()
            for fid in arch2del:
                DOperation._removeArchive(fid, version)  # version
            for arch in arch2set:
                DOperation._storeArchive(arch, now)  # lastGet / lastCheck  
        
    def syncs(self, syncs:Iterable[Sync]) -> Iterable[Sync]:
        """
        - **tous les items créés ou modifiés entre `vd` et `vs`.**
        - ***pour les suppressions :***
            - **cas 1 : vd >= dts.** L'archive serveur détient toutes les suppressions effectuées entre `vd` et `vs`, elles sont transmises afin que l'archive distante puisse supprimer les items correspondants.
            - **cas 2 : vd < dts**. L'archive serveur n'a pas toutes les suppressions. Elle transmet en delta la liste des clés des items qui existait à `vd`, existent toujours et n'ont pas été modifiées depuis. L'archive distante peut en déduire que les autres (qu'elle avait en mémoire) et qui ne figurent pas dans cette liste, ont été supprimés à un moment donné antérieur à `dts`.
        """
        
        rsyncs = []
        for sync in syncs:
            descr = Document.descr(sync.table)
            if descr is None or not "docid" in sync or not "version" in sync :
                continue
            docid = sync["docid"]
            vd = sync["version"]
            isfull = sync.get("isfull", True)
            fargs = sync.get("filter", None)
            arch = self._getArch(descr, docid, 5, isfull)
            if arch is None:
                sync["version"] = -1
                del sync["items"]
                del sync["keys"]
                rsyncs.append(sync)
                continue
            if arch.version <= vd:
                del sync["items"]
                del sync["keys"]
                rsyncs.append(sync)
                
            vs = arch.version
            sync["version"] = vs
            cas1 = vd >= arch.dtcas
            sync["items"] = {}
            items = sync["items"]
            if cas1:
                del sync["keys"]
            else:
                sync["keys"] = {}
                keys = sync["keys"]
                
            hdr = arch.hdr()
            c = descr.cls.filterSyncItem(self, fargs, docid, "hdr", tuple(), hdr[0], hdr[1])
            items[("hdr", tuple())] = (hdr[0], c)
            
            for clkeys in arch.items:
                meta = hdr[0]
                v = meta[0]
                s = meta[2]
                if v <= vd:         # item modified / created before vd
                    if not cas1 and s > 0:  # existing item : to put in keys
                        keys[clkeys] = True
                    continue
                # item created / modified / deleted after vd
                if s != 0:  # created / modified
                    c = descr.cls.filterSyncItem(self, fargs, docid, clkeys[0], clkeys[1], meta, hdr[1])
                    items[clkeys] = (meta, c)
                else:       # deleted
                    if cas1:
                        items[clkeys] = (meta, None)
                
            if "fargs" in sync and "temp" in sync["fargs"]:
                del sync["fargs"]["temp"]
            rsyncs.append(sync)
        return rsyncs
        
    def process(self, param:Param) -> Result:
        return None
    
    def __init__(self, execCtx:ExecCtx):
        super().__init__(execCtx)
        self._docCache = {}      # key id(), value: tuple (arch, age)
        self._accTkt  = AccTkt(self.org, self.stamp.stamp, execCtx.contentLength)
        self._t2 = 0 # nombre de documents lus
        self._t3 = 0 # nombre d'items lus
        self._t4 = 0 # nombre d'items écrits.
        self._t5 = 0 # nombre de tâches créées / mise à jour.
        
    def work(self) -> Result: 
        result = self.process(self.param)
        self.execCtx.phase = 2
        self.validation()
        self.execCtx.phase = 3
        """
        Ne pas créer de result pour une task, ni syncs mais un accTkt
        """
        if result is None:
            result = Result(self).setJson({})
        jsonResp = result.getJson()
        if jsonResp is not None:
            x = self.inputData.get("syncs", None)
            if x is not None:
                jsonResp["syncs"] = self.syncs(json.loads(x))
        if self._accTkt.tktid is not None:
            t1 = Stamp.epochNow()
            t0 = self.stamp.epoch
            self._accTkt.t[1] = 0.0
            self._accTkt.t[2] = self._t2
            self._accTkt.t[3] = self._t3
            self._accTkt.t[4] = self._t4
            self._accTkt.t[5] = self._t5
            self._accTkt.t[6] = (t1 - t0) / 1000
        if jsonResp is not None:
            if self._accTkt.tktid is not None:
                jsonResp["accTkt"] = self._accTkt.tktid
        if self._accTkt.tktid is not None:
            self._accTkt.t[1] = result.finalLength() / 1000
            self.provider.setAccTkt(self._accTkt)
        return result
            
    def recordAccData(self, table:str, tktid:str, lcpt:Iterable[float], state):
        self._accTkt.tktid = tktid
        self._accTkt.table = table
        self._accTkt.state = state
        for i, c in enumerate(lcpt):
            if i > 7:
                break
            self._accTkt.f[i] = c
            
    def newFromJson(self, clazz:Type, json_data:str):
        obj = clazz()
        if json_data is not None:
            d = json.loads(json_data)       # d is a dict
            for var in d:
                setattr(obj, var, d[var])
        return obj
    
    def newFromDict(self, clazz:Type, d:Dict):
        obj = clazz()
        if d is not None:
            for var in d:
                setattr(obj, var, d[var])
        return obj
            
class ValidationList:
    def __init__(self, version:int):
        self.upd = (list(), list(),list(),list(),list())
        self.version = version
        
    def add(self, d:Document, version:int) -> Tuple[int, DocumentArchive]:
        # _status : 0:deleted, 1:unmodified, 2:modified, 3:created 4:recreated (after deletion)
        status, upd, arch = d._validate(version)
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
    def __init__(self, org:str, version:int, contenLength:int):
        self.t = [contenLength / 1000,0.0,0,0,0,0,0,0.0]
        self.f = [0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
        self.org = org
        self.v = version
        self.state = {}
        self.tktid = None
        self.table = None
    
