from config import cfg
from root import al, dics, AppExc, Result, Operation



class InfoOP(Operation):
    def __init__(self, execCtx):
        super().__init__(execCtx)
        
    def work(self): #
        return Result().setJson({'inb':cfg.inb, 'opb':cfg.opb, 'uiba':cfg.uiba})
