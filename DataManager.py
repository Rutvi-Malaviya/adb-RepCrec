from LockManager import LockManager
from LockManager import Lock
from collections import defaultdict

class Variable:
    def __init__(self, varId, val, isReplicated):
        self.varId = varId
        self.value = val
        self.tempVal = {}
        self.lastWrite = 0
        self.isReadable = True
        self.isReplicated = isReplicated

    def __repr__(self):
        return "{}".format( self.value)

class DataManager:
    def __init__(self,siteId):
        self.siteId = siteId
        self.isUp = True
        self.variableList = {}
        self.lockTable = {}
        self.failTimeStamps = []
        self.recoverTimeStamps = []
        self.upSince = 0

        for i in range(1,21):
            var = 'x'+str(i)
            if i&1==0:
                self.variableList[var] = Variable(var,10*i, True)
                self.lockTable[var] = LockManager(var)
            elif i%10 + 1 == self.siteId:
                self.variableList[var] = Variable(var,10*i, False)
                self.lockTable[var] = LockManager(var)

        # print("site {}: {}".format(siteId, self.variableList))

    def getSiteId(self):
        return self.siteId

    def hasVariable(self, var):
        # print(str(self.siteId) + " " + var + ' - ' + str(var in self.variableList.keys())) 
        return (var in self.variableList.keys())

    def readSnapshot(self, ts):
        snapshotList = {}

        for varId in self.variableList.keys():
            var = self.variableList[varId]
            if var.lastWrite >= self.upSince:
                snapshotList[var.varId] = var.value

        return snapshotList

    def read(self, trans_id, var):
        #  read variables
        tempVar = None
        if var in self.variableList.keys():
            tempVar = self.variableList[var]
        else:
            return None

        if tempVar.isReadable:
            tempLockManager = self.lockTable[var]
            tempLock = tempLockManager.currentLock

            if tempLock:
                if tempLock.lockType == 'R':
                    if trans_id in tempLock.transactions:
                        return tempVar.value
                    elif not tempLockManager.hasQueuedWrite():
                        tempLock.transactions.insert(trans_id)
                        return tempVar.value
                    else:
                        tempLock.pendingRequests.append(Lock(var, "R", [trans_id]))
                        return None
                elif trans_id in tempLock.transactions:
                    return tempVar.value
                else:
                    tempLock.pendingRequests.append(Lock(var, "R", [trans_id]))
                    return None
            else:
                tempLockManager.currentLock = Lock(var, "R", [trans_id])
                return tempVar.value
        else:
            return None


    def write(self, trans_id, var, val):
        tempVar = None
        if var in self.variableList.keys():
            tempVar = self.variableList[var]
        else:
            return None

        tempLockManager = self.lockTable[var]
        tempLock = tempLockManager.currentLock

        if tempLock:
            if tempLock.lockType == 'R':
                if trans_id in tempLock.transactions:
                    if len(tempLock.transactions)==1:
                        if tempLockManager.hasQueuedWrite(trans_id):
                            print("Cannot promote to W-lock, other process is waiting")
                            return None
                        else:
                            tempLockManager.promoteLock(trans_id)
                            tempVar.tempVar[trans_id] = val
                    else:
                        print("Other transactions holding R locks")
                        return None
                else:
                    print("transactions does not hold R locks")
                    return None
            elif tempLock.lockType == 'W':
                if trans_id in tempLock.transactions:
                    tempVar.tempVal[trans_id] = val
                    return
                else:
                    print("Other transaction having W lock")
                    return None

        tempLockManager.currentLock = Lock(var, "W", [trans_id])
        tempVar.tempVal[trans_id] = val

    def getWriteLock(self, trans_id, var):
        tempVar = None
        if var in self.variableList.keys():
            tempVar = self.variableList[var]
        else:
            return None

        tempLockManager = self.lockTable[var]
        tempLock = tempLockManager.currentLock

        if tempLock:
            if tempLock.lockType == 'R':
                if trans_id in tempLock.transactions:
                    if len(tempLock.transactions)==1:
                        if tempLockManager.hasQueuedWrite(trans_id):
                            tempLockManager.pendingRequests.append(Lock(var, 'W', [trans_id]))
                            print("Cannot promote to W-lock, other process is waiting")
                            return False
                        else:
                            return True
                    else:
                        tempLockManager.pendingRequests.append(Lock(var, 'W', [trans_id]))
                        print("Other transactions holding R locks")
                        return False
                else:
                    print("transactions does not hold R locks")
                    return False
            elif tempLock.lockType == 'W':
                if trans_id in tempLock.transactions:
                    return True
                else:
                    tempLockManager.pendingRequests.append(Lock(var, 'W', [trans_id]))
                    print("Other transaction having W lock")
                    return False
        return True

    def dump(self):
        printString = 'Site ' + str(self.siteId) + ' - '
        
        for var in self.variableList.keys():
            printString += var + ': ' + str(self.variableList[var]) + ' '

        print(printString)
            
    def abort(self, trans_id):
        for lockManager in self.lockTable.values():
            tempLock = lockManager.currentLock
            if trans_id in tempLock.transactions:
                tempLock.transactions.remove(trans_id)
                if len(tempLock.transactions) == 0:
                    lockManager.currentLock = None

            removeList = []
            for pending in lockManager.pendingRequests:
                if trans_id in pending.transactions:
                    removeList.append(pending)
                
            for pending in removeList:
                lockManager.pendingRequests.remove(pending)

        for var in self.variableList.values():
            if trans_id in var.tempVal.keys():
                var.tempVal.remove(trans_id)

        self.resolveLockTable()
    
    def commit(self, trans_id, ts):
        for lockManager in self.lockTable.values():
            tempLock = lockManager.currentLock
            if trans_id in tempLock.transactions:
                tempLock.transactions.remove(trans_id)
                if len(tempLock.transactions) == 0:
                    lockManager.currentLock = None

            for pending in lockManager.pendingRequests:
                if trans_id in pending.transactions:
                    print('Cannot commit: unresolved queue locks')
           
        for var in self.variableList.values():
            if trans_id in var.tempVal.keys():
                var.value = var.tempVal[trans_id]
                var.isReadable = True

        self.resolveLockTable()

    def fail(self, ts):
        self.isUp = False
        for manager in self.lockTable.values():
            manager.currentLock = None
            manager.pendingRequests = []

    def recover(self, ts):
        self.isUp = True
        self.upSince = ts
        
        for var in self.variableList:
            if var.isReplicated:
                var.isReadable = False

    def resolveLockTable(self):
        for lockManager in self.lockTable.values():
            if len(lockManager.pendingRequests):
                if not lockManager.currentLock:
                    lock = lockManager.pendingRequests.pop(0)
                    lockManager.currentLock = lock

                removeList = []
                if lockManager.currentLock.lockType == 'R':
                    for pending in lockManager.pendingRequests:
                        if pending.lockType == 'W':
                            if len(lockManager.currentLock.transactions)==1 and (pending.transactions[0] == lockManager.currentLock.transactions[0]):
                                lockManager.promoteLock(pending.transactions[0])
                                removeList.append(pending)
                            else:
                                break
                        else:
                            lockManager.currentLock.transactions.append(pending.transactions[0])
                            removeList.append(pending)

                    for pending in removeList:
                        lockManager.pendingRequests.remove(pending)


    def generateWaitsForGraph(self):

        def current_blocks_queued(currentLock, queuedLock):
            if currentLock.lockType == 'R':
                if queuedLock.lockType == 'R' or (len(currentLock.transactions) == 1 and queuedLock.transactions[0] in currentLock.transactions):
                    return False

                return True
            # current lock is W-lock
            return not currentLock.transactions[0] == queuedLock.transactions[0]

        def queued_blocks_queued(queued_lock_left, queued_lock_right):
            if queued_lock_left.lockType == 'R' and queued_lock_right.lockType == 'R':
                return False
            # at least one lock is W-lock
            return not queued_lock_left.transactions[0] == queued_lock_right.transactions[0]

        graph = defaultdict(set)

        for var, lm in self.lockTable.items():
            if not lm.currentLock or len(lm.pendingRequests)==0:
                continue
            
            for req in lm.pendingRequests:
                if current_blocks_queued(lm.currentLock, req):
                    if lm.currentLock.lockType == 'R':
                        for t_id in lm.currentLock.transactions:
                            if t_id != req.transactions[0]:
                                graph[req.transaction_id].add(t_id)
                    else:
                        if lm.currentLock.transactions[0] != req.transactions[0]:
                            graph[req.transactions[0]].add(lm.current_lock.transactions[0])
            
            for i in range(len(lm.transactions)):
                for j in range(i):
                    # print("queued_blocks_queued({}, {})".format(
                    #     lm.queue[j], lm.queue[i]))
                    if queued_blocks_queued(lm.transactions[j], lm.transactions[i]):
                        # if lm.queue[j].transaction_id != lm.queue[i
                        # ].transaction_id:
                        graph[lm.transactions[i]].add(
                            lm.transactions[j])
        # print("graph {}={}".format(self.site_id, dict(graph)))
        return graph
        