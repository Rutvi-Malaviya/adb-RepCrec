class Lock:
    def __init__(self, var, Ltype=None, trans=[]):
        '''
        varId: name of the variable to be locked
        lockType: type of lock. R: Read W: Write
        transactions: Array of tranactions holding the lock. 
                      It can have only 1 element if lock is of type write
                      Can have multiple elements if lock is of type read
        '''

        self.varId = var
        self.lockType = Ltype
        self.transactions = trans

    def __repr__(self):
        return '[varId: ' + self.varId + ", lockType: " + self.lockType + ", transactions: " + str(self.transactions) + "]"

class LockManager:
    def __init__(self, var):
        '''
        var: name of the variable that is managed
        currentLock: Lock that is currently held on the variable
        pendingRequests: Requests for the lock on variable waiting in queue
        '''

        self.var = var
        self.currentLock = None
        self.pendingRequests = []

    def __repr__(self):
        return '[var: ' + self.var + ", currentLock: " + str(self.currentLock) + ", pending requests: " + str(self.pendingRequests) + "]"


    def removeLocks(self, trans_id):
        '''
        trans_id: transaction id

        Remove the lock when transaction commits 
        '''
        if self.currentLock and (trans_id in self.currentLock.transactions):
            self.currentLock.transactions.remove(trans_id)

            if len(self.currentLock.transactions)==0:
                self.currentLock = None
    
        for req in self.pendingRequests:
            if trans_id in req.transactions:
                print('unresolved locks')
                return False

        return True

    def hasQueuedWrite(self, trans_id=None):
        '''
        trans_id: transaction id

        The method returns true if there is a pending write in the queue, otherwise returns false
        '''

        for req in self.pendingRequests:
            if req.lockType=='W':
                if trans_id and req.transactions[0]==trans_id:
                    continue
                return True

        return False

    def promoteLock(self, trans_id):
        '''
        trans_id; transaction id
        '''

        # Check if a lock exists on the variable
        if not self.currentLock:
            print("No lock present")
        # Check if current lock is read or not
        elif not self.currentLock.lockType == 'R':
            print("Current lock is not R")
        # If current lock is read, check if it can be upgraded
        elif len(self.currentLock.transactions)!=1:
            print("Other transactions having R lock")
        # Check if the only trasaction having read lock on the variable is the required transaction
        elif trans_id not in self.currentLock.transactions:
            print("Transaction not having R lock")
        # Upgrade the lock to W since all criterion matches
        else:
            self.currentLock.lockType = "W"