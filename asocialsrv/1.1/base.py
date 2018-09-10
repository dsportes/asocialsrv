from util import Result
from root import Operation

class InfoOP(Operation):
    def __init__(self, execCtx):
        super().__init__(execCtx)
        
    def work(self): 
        return Result(self).setJson(self.respXCH)

class Echo(Operation):
    def __init__(self, execCtx):
        super().__init__(execCtx)
        
    def work(self): 
        #raise AppExc("ATEST", ["toto"])
        return Result(self).setJson(self.param)
