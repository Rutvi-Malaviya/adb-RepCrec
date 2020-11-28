class Lock:
    def __init__(self, var, Ltype=None, trans=[]):
        self.varId = var
        self.lockType = Ltype
        self.transactions = trans

class LockManager:
    def __init__(self, var):
        self.var = var
        self.currentLock = Lock(var)
        self.pendingRequests = []

    def hasQueuedWrite(self, trans_id=None):
        for waitingReq in self.pendingRequests:
            if waitingReq.lockType=='W':
                if trans_id and waitingReq.trans_id==trans_id:
                    continue
                return True

        return False

    def promoteLock(self, trans_id):
        if not self.currentLock:
            print("No lock present")
        elif not self.currentLock.lockType == 'R':
            print("Current lock is not R")
        elif len(self.currentLock.transactions)!=1:
            print("Other transactions having R lock")
        elif trans_id not in self.currentLock.transactions:
            print("Transaction not having R lock")
        else:
            self.currentLock.lockType = "W"