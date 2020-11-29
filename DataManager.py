from LockManager import LockManager
from LockManager import Lock
from collections import defaultdict

class Variable:
    def __init__(self, varId, val, isReplicated):
        '''
        varId: name of the variable
        value: value of the variable
        tempVal: temorary value of the varialbe held by transaction before commit
        lastWrite: last time the varialbe was modified
        isReadable: Indicates if the variable can be read. False in case the site went down 
                    and the variable is replicated
        isReplicated: Indicates if the variable is replicated
        '''

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
        '''
        siteId: Site id
        isUp: Indicates if the site is up or down
        variableList: dictionary containing the name of variable as keys and value of variable as values
        lockTable: Dictionary containing variable name as key and corresponsing lock manager as value
        upSince: time since when the site is up

        The variablesList is initialized in this method
        '''
        self.siteId = siteId
        self.isUp = True
        self.variableList = {}
        self.lockTable = {}
        self.upSince = 0

        for i in range(1,21):
            var = 'x'+str(i)
            if i&1==0:

                # These are even values and are present in all sites and hence are replicated
                self.variableList[var] = Variable(var,10*i, True)
                self.lockTable[var] = LockManager(var)
            elif i%10 + 1 == self.siteId:

                # These values are not replicated since they are present in particular sites
                self.variableList[var] = Variable(var,10*i, False)
                self.lockTable[var] = LockManager(var)

        # print("site {}: {}".format(siteId, self.variableList))

    def getSiteId(self):
        return self.siteId

    def hasVariable(self, var):
        # Returns boolean value indicating if the variable is present in the site
        return (var in self.variableList.keys())

    def dump(self):
        '''
        The method iterates the variable list and prints their name and values
        '''

        printString = 'Site ' + str(self.siteId) + ' - '
        
        for var in self.variableList.keys():
            printString += var + ': ' + str(self.variableList[var]) + ' '

        print(printString)

    
    ####### READ - WRITE OPERATIONS #############
     

    def readSnapshot(self):
        '''
        Returns the snapshot of the variables available on the site
        Returns a dictionary with variable name as keys and variable values as value.
        '''

        snapshotList = {}

        for varId in self.variableList.keys():
            var = self.variableList[varId]

            # Check if the variable is replicated
            if var.isReplicated:

                # If var is replicated, then add the variable only if it is readable
                if var.isReadable:
                    snapshotList[var.varId] = var.value
            else:
                snapshotList[var.varId] = var.value

        return snapshotList

    def read(self, trans_id, var):
        '''
        trans_id: transaction id
        var: variable name

        Read the variable from the site and return the value of possible, otherwise returns None
        '''
        
        tempVar = None
        if var in self.variableList.keys():
            tempVar = self.variableList[var]
        else:
            return None

        # check if the variable is readable
        if tempVar.isReadable:
            tempLockManager = self.lockTable[var]
            tempLock = tempLockManager.currentLock

            if tempLock:

                # check if the current lock is of type read
                if tempLock.lockType == 'R':

                    # check if the transaction already has a read lock on the variable
                    if trans_id in tempLock.transactions:
                        return tempVar.value
                    # Check if there is any write lock in pending which might conflict with this write lock
                    elif not tempLockManager.hasQueuedWrite():
                        tempLock.transactions.append(trans_id)
                        return tempVar.value
                    # If there is a conflict, add the current request to the queue
                    else:
                        tempLock.pendingRequests.append(Lock(var, "R", [trans_id]))
                        return None
                # current lock is a write lock. Check if the lock is held by current transaction
                elif trans_id in tempLock.transactions:
                    return tempVar.value
                # If write lock is not held by current transaction, add the read request to the queue
                else:
                    tempLock.pendingRequests.append(Lock(var, "R", [trans_id]))
                    return None
            else:
                tempLockManager.currentLock = Lock(var, "R", [trans_id])
                return tempVar.value
        else:
            return None


    def write(self, trans_id, var, val):
        '''
        trans_id: transaction id
        var: name of variable to be read
        val: value with which variableis to be written

        The method checks if a write lock can be allocated to the transaction 
        If yes then it allocates the lock and writes the variable value as a temp value
            which will be assigned to the variable when the transaction commits
        '''

        tempVar = None
        if var in self.variableList.keys():
            tempVar = self.variableList[var]
        else:
            return None

        tempLockManager = self.lockTable[var]
        tempLock = tempLockManager.currentLock

        if tempLock:
            # Check if current lock is R
            if tempLock.lockType == 'R':
                if trans_id in tempLock.transactions:
                    # Check if current transaction exists in the lock
                    if len(tempLock.transactions)==1:
                        # If current transaciton is the only transaction then check if there is a queued lock 
                        # that conflicts the current lock
                        if tempLockManager.hasQueuedWrite(trans_id):
                            print("Cannot promote to W-lock, other process is waiting")
                            return None
                        else:
                            # promote the lock to write and write the variable with the given value in tempVar
                            # The variable will be written when the transaction commits
                            tempLockManager.promoteLock(trans_id)
                            tempVar.tempVar[trans_id] = val
                            return None
                    else:
                        print("Other transactions holding R locks")
                        return None
                else:
                    print("transactions does not hold R locks")
                    return None
            elif tempLock.lockType == 'W':
                # If current lock is W then check if the current transaction hold lock and perform approporiate actions
                if trans_id in tempLock.transactions:
                    tempVar.tempVal[trans_id] = val
                    return None
                else:
                    print("Other transaction having W lock")
                    return None

        # If not lock is held on the variable, give the transaction the write lock
        tempLockManager.currentLock = Lock(var, "W", [trans_id])
        tempVar.tempVal[trans_id] = val


########## LOCK ASSIGNMENT OPERATIONS #############

    def getWriteLock(self, trans_id, var):
        '''
        trans_id: transaction id
        var: name of the variable

        The method returns a boolean that indicates if the transaction can be given write lock on the variable
        It checks for conflicts and other conditions and returns a boolean value accordingly
        '''

        tempVar = None
        if var in self.variableList.keys():
            tempVar = self.variableList[var]
        else:
            return False

        tempLockManager = self.lockTable[var]
        tempLock = tempLockManager.currentLock

        if tempLock:
            # Check if current lock is of type read
            if tempLock.lockType == 'R':
                if trans_id in tempLock.transactions:

                    # Check if the current transaction hold the read lock
                    if len(tempLock.transactions)==1:

                        # If current transaciton is the only transaction then check if there is a queued lock 
                        # that conflicts the current lock
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
                    # Other transactions hold the read lock and current transaction does not hold the read lock
                    print("transactions does not hold R locks")
                    return False

            # check if current lock if of type write
            elif tempLock.lockType == 'W':
                if trans_id in tempLock.transactions:
                    return True
                else:
                    tempLockManager.pendingRequests.append(Lock(var, 'W', [trans_id]))
                    print("Other transaction having W lock")
                    return False
        return True

    def resolveLockTable(self):
        '''
        The method iterates through the lock table to find if there is any pending request that can now be fulfilled.
        If it finds such a request, it assigns corresponding lock to the transaction
        '''

        for lockManager in self.lockTable.values():
            if len(lockManager.pendingRequests):
                if not lockManager.currentLock:
                    lock = lockManager.pendingRequests.pop(0)
                    lockManager.currentLock = lock

                removeList = []

                # Check if the currently held lock is of type read
                if lockManager.currentLock.lockType == 'R':
                    for pending in lockManager.pendingRequests:
                        if pending.lockType == 'W':
                            # If the request lock is write and the requesting transaction is the only transaction holding the read lock, promote the lock
                            if len(lockManager.currentLock.transactions)==1 and (pending.transactions[0] == lockManager.currentLock.transactions[0]):
                                lockManager.promoteLock(pending.transactions[0])
                                removeList.append(pending)
                            else:
                                break
                        else:
                            # If the requesting lock is also read assign the lock so that the lock will be shared read lock
                            lockManager.currentLock.transactions.append(pending.transactions[0])
                            removeList.append(pending)

                    for pending in removeList:
                        lockManager.pendingRequests.remove(pending)


######### ABORT AND COMMIT OPERATIONS #############

            
    def abort(self, trans_id):
        '''
        trans_id: transaction id
        
        The method iterates the lock table and checks in lock manager of each variable if the current transaction helds
        or requests locks.
        If such a lock is found, it is removed

        It also checks if the transaction wrote any temp value in the tempVal list and removes it if found
        '''

        for lockManager in self.lockTable.values():
            tempLock = lockManager.currentLock

            # Check if current transaction holds a lock and remove it if yes
            if tempLock and trans_id in tempLock.transactions:
                tempLock.transactions.remove(trans_id)
                if len(tempLock.transactions) == 0:
                    lockManager.currentLock = None

            removeList = []

            # Iterate the pending requests and check if current transaction has any pending requests 
            for pending in lockManager.pendingRequests:
                if trans_id in pending.transactions:
                    removeList.append(pending)
                
            for pending in removeList:
                lockManager.pendingRequests.remove(pending)

        # Remove any tempVal of the variable written by the transaction
        for var in self.variableList.values():
            if trans_id in var.tempVal.keys():
                var.tempVal.pop(trans_id)

        self.resolveLockTable()
    
    def commit(self, trans_id, ts):
        '''
        trans_id: transaction id
        ts: time when transaction commits

        The method checks the locks held by the transaction and release them. Also it checks if the transaction has 
        any pending lock request
        Lastly it commits the temporary values written by the transaction to the value of variable
        '''

        for lockManager in self.lockTable.values():
            tempLock = lockManager.currentLock

            # Release the locks held by transaction 
            if tempLock and trans_id in tempLock.transactions:
                tempLock.transactions.remove(trans_id)
                if len(tempLock.transactions) == 0:
                    lockManager.currentLock = None

            #  Check if the transaction has any pending lock requests
            for pending in lockManager.pendingRequests:
                if trans_id in pending.transactions:
                    print('Cannot commit: unresolved queue locks')
           
        # Write the tempVal written by transaction to the variable value.
        # Make the variable readable which may have been rendered false at site failure
        # Change the lastWrite time of the variable to current time
        for var in self.variableList.values():
            if trans_id in var.tempVal.keys():
                var.value = var.tempVal[trans_id]
                var.isReadable = True
                var.lastWrite = ts

        self.resolveLockTable()


############# FAIL RECOVER OPERATIONS ############

    def fail(self):
        '''
        It changes the isUp variable to indicate that the site is down
        Removes any current locks held on the variable
        Also removes pending request information on the variable
        '''

        self.isUp = False
        for manager in self.lockTable.values():
            manager.currentLock = None
            manager.pendingRequests = []

    def recover(self, ts):
        '''
        ts: timestamp when the site recovers

        It changes the isUp variable to indicate that the site is now available
        Changes readable status of replicated variables to false
        Updates the upSince time to indicate the time from when the site has been active
        '''

        self.isUp = True
        self.upSince = ts
        
        for var in self.variableList:
            if var.isReplicated:
                var.isReadable = False

        
########## DEADLOCK DETECTION ###############
    

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
        