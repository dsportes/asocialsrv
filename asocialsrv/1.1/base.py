from config import cfg
from execCtx import al, dics, AppExc, Result, Operation

class InfoOP(Operation):
    def __init__(self, execCtx):
        super().__init__(execCtx)
        
    def work(self): #
        return Result(True).setJson({'inb':cfg.inb, 'opb':cfg.opb, 'uiba':cfg.uiba})
