import re
import sys
from Transaction import Transaction
from DataManager import DataManager
from collections import defaultdict

class Operation:
    def __init__(self, cmd, trans_id, var, val=0):
        '''
        cmd: The operation to be executed: R for read W for write
        trans_id: transaction id of the transaction requesting the operation
        var: variable for which the R or W operation is requested
        val: The value with which variable is updated in case of write operation. (not used in case of read operation)
        isNew: Indicates if the operation is newly created or not
        '''

        self.cmd = cmd
        self.trans_id = trans_id
        self.var = var
        self.val = val
        self.isNew = True

    def __repr__(self):
        return '(' + self.cmd + ',' + self.trans_id + "," + self.var + ',' + str(self.val) + ')'

class TransactionManager:
    def __init__(self):
        '''
        transactionQueue: dictionary of transactions with transaction id as key and transaction object as value
        timestamp: variable to store and increment time
        operationQueue: Array of pending read and write operations. Has operation objects as elements 
        dataManagers: Array of 10 data manages for 10 sites. Has DataManager objects as elements

        The init method takes care of initializing the dataManagers array
        '''
        self.transactionQueue = {}
        self.timestamp = 0
        self.operationQueue = []
        self.dataManagers = []

        for i in range(1,11):
            self.dataManagers.append(DataManager(i))

    def processLine(self, command):
        '''
        command: the input from the user 

        This function parses the command to get the function to be executed 
        It calls processInstruction() to take necessary actions
        Calls executeOperations() to carry out read and write operations present in the operation queue
        takes care of incrementing the timestamp

        '''
        # print(command)
        if command[0]=='/': 
            return 
            
        tokens = re.findall(r"[\w']+",command)

        # print('tokens:',tokens)      

        self.processInstruction(tokens[0],tokens[1:])
        self.executeOperations()
        if self.resolveDeadlock():
            self.executeOperations()

        self.timestamp = self.timestamp + 1

        # print('\n Remaining operatins: ',self.operationQueue)
        
    def processInstruction(self, cmd, args):
        '''
        cmd: command to be executed 
        args: array of arguments required to execute the command.

        begin, beginRO, dump, fail, and recover operations are executed within the function itself.
        beginRO uses a snapshot function to read the values of variable in the database at the beginning of transaction
        R and W commands are added to the operationQueue which will be executed afterwards using the executeOperations() function.
        '''

        if cmd == 'begin':
            # Check if transaction id is valid and add corresponsing transaction object to the 
            # transaction queue 

            if(args[0] in self.transactionQueue.keys()):
                print("transaction id {} already present", args[0])
            else:
                self.transactionQueue[args[0]] = Transaction(self.timestamp, args[0], False)
                print("{} begins".format(args[0]))

        elif cmd == 'beginRO':
            # Check if transaction id is valid and read the database snapshot and add corresponsing
            # transaction object to the transaction queue 

            if(args[0] in self.transactionQueue.keys()):
                print("transaction id {} already present", args[0])
            else:
                varList = self.readSnapshot()
                self.transactionQueue[args[0]] = Transaction(self.timestamp, args[0], True, varList)
                print("{} begins and is read-only".format(args[0]))

        elif cmd == 'W':
             # Insert a Write Operation to the operation queue.

            trans_id = args[0]
            var = args[1]
            val = args[2]
            if(trans_id in self.transactionQueue.keys()):
                self.operationQueue.append(Operation("W", trans_id, var, val))
            else:
                print("Transaction {} not found".format(trans_id))

        elif cmd == 'R':
            #  Insert a Read Operation to the operation queue.

            trans_id = args[0]
            var = args[1]
            if(trans_id in self.transactionQueue.keys()):
                self.operationQueue.append(Operation("R", trans_id, var))
            else:
                print("Transaction {} not found".format(trans_id))

        elif cmd == 'dump':
            # loop through dataManagers and perform dump() on each of them

            for dm in self.dataManagers:
                dm.dump()

        elif cmd == 'end':
            # Check if the transacton can commit or has to abort and take appropriate action

            trans_id = args[0]

            if(trans_id in self.transactionQueue.keys()):
                if (self.transactionQueue[trans_id]).canCommit:
                    self.commit(trans_id, self.timestamp)
                else:
                    self.abort(trans_id, True)
            else:
                print("Transaction {} not found".format(trans_id))

        elif cmd == 'fail':
            # Call the fail method in the corresponding data manager 
            # change the can commit state of the transaction that has accessed that site to false

            siteId = int(args[0])
            dm = self.dataManagers[siteId-1]
            dm.fail()
            print("site {} fails".format(siteId))

            for transaction in self.transactionQueue.values():
                if (not transaction.isReadOnly) and (transaction.canCommit) and (siteId in transaction.accessedSites):
                    transaction.canCommit = False

        elif cmd == 'recover':
            # Call the recover method in the corresponding data manager 

            siteId = int(args[0])
            dm = self.dataManagers[siteId-1]
            dm.recover(self.timestamp)
            print("site {} recovers".format(siteId))

        else:
            print("Invalid operation")

    
    def readSnapshot(self):
        '''
        Reads the snapshot of the database using available the data managers for read only transactions
        The method calls the readSnapshot method of datamanager that returns the dictionary of variables.

        varList: dictionary containing the key values as variable name and values as their value
        '''
        
        varList = {}
        for dm in self.dataManagers:
            if dm.isUp:
                tempList = dm.readSnapshot()

                for var in tempList.keys():
                    varList[var] = tempList[var]

        # print("snapshot read is: ", varList)
             
        return varList
 

 ####### EXECUTE OPERATIONS IN THE QUEUE ###############
 

    def executeOperations(self):
        '''
        The method loops through the operation queue and executes the operations that can be executed.
        It keeps track of the executed operations and removes them from the queue at the end
        Changes the isNew status of operation to false to indicate that it has been processed once.
            This restrains the data manager to add the same lock request multiple times in the pending queue
        '''
       
        removeOperations = []
        for operation in self.operationQueue:
            trans_id = operation.trans_id
            var = operation.var
            isNewOp = operation.isNew

            if trans_id in self.transactionQueue.keys():
                success = False
                
                # operation has to perform read 
                if operation.cmd == "R":

                    # check if the transaction is readonly
                    if self.transactionQueue[trans_id].isReadOnly:

                        # take the transaction from the transaction queue which contains the variable 
                        # information from the snapshot taken at the begin time
                        transaction = self.transactionQueue[trans_id]

                        # Check if the variable exists in the snapshot and complete the operation if it does
                        if var in transaction.dbSnapshot.keys():
                            success = True
                            print("{}: {}".format(var, self.transactionQueue[trans_id].dbSnapshot[var]))
                        else:
                            success = False
                            print("var not found")
                    else:
                        # If transaction is not read only, call the read method to perfrom the read operation
                        success = self.read(trans_id, var, isNewOp)
                else:
                    # operation has to perform write
                    val = operation.val 
                    success = self.write(trans_id, var, val, isNewOp)

                # If the operation was executed successfully add it to the remove queue
                if success:
                    removeOperations.append(operation)
            else:
                # If the transaction id does not exist in the transactionQueue, add the operation to the remove queue
                removeOperations.append(operation)
            
            operation.isNew = False
        # Remove all the operations in the remove queue from the operationQueue
        for operation in removeOperations:
            self.operationQueue.remove(operation)

        # print("Remaining operations: ", self.operationQueue)


########### READ - WRITE OPERATIONS ###############


    def read(self, trans_id, var, isNew):
        '''
        trans_id: transaction id
        var: variable name for which the read is to be performed
        
        The method uses read method in the data manager
        '''
       
        # Check if the transaction exists in the transactionQueue
        if trans_id in self.transactionQueue.keys():
            ts = self.transactionQueue[trans_id].timestamp

            for dm in self.dataManagers:
                # if the data manager is available, check if it has the variable 
                # if it has variable, read the value from the data manager 
                if dm.isUp and dm.hasVariable(var):
                    val = dm.read(trans_id, var, isNew)

                    if val:
                        # If the read was successful update the accessed site for the transaction
                        self.transactionQueue[trans_id].addSite(dm.siteId)

                        # print("{} reads {}.{} = {}".format(trans_id, dm.siteId, var, val))

                        print("{} reads {}: {}".format(trans_id, var, val))
                        return True

        return False

    def write(self, trans_id, var, val, isNew):
        '''
        trans_id: transaction id
        var: variable on which write is to be performed
        val: value with which the variable is to be written

        The method uses the write method in the data manager
        '''

        
        # Check if the transaction exists in the transactionQueue
        if trans_id in self.transactionQueue.keys():
            ts = self.transactionQueue[trans_id].timestamp

            # allSitesDown keeps track whether all sites are down or not 
            # hasAllWriteLocks tracks whether we can get write lock on all the sites having the variable
            allSitesDown = True
            hasAllWriteLocks = True
            # sites=[]
            for dm in self.dataManagers:
                
                if dm.isUp and dm.hasVariable(var):
                    allSitesDown = False
                    gaveLock = dm.getWriteLock(trans_id, var, isNew)
    
                    if not gaveLock:
                        hasAllWriteLocks = False
            
            # write only if all the available sites gives lock on the variable
            if (not allSitesDown) and (hasAllWriteLocks):
                sitesModified = []
                for dm in self.dataManagers:
                    if dm.isUp and dm.hasVariable(var):

                        # perform write operation in the data manager 
                        # Update the sites accessed by the transaction

                        dm.write(trans_id, var, val)
                        self.transactionQueue[trans_id].addSite(dm.siteId)
                        sitesModified.append(dm.getSiteId())

                print("{} writes {} = {} to the sites: {}".format(trans_id, var, val, sitesModified))
                return True

        return False


########## END TRANSACTION OPERATIONS ###############


    def commit(self, trans_id, time):
        '''
        trans_id: transaction id
        time: timestamp at which the transaction commits 

        calls the commit in data managers so that appropriate actions can be taken at each site
        '''

        for dm in self.dataManagers:
            dm.commit(trans_id, time)

        # Remove the transaction from the transacitonQueue
        self.transactionQueue.pop(trans_id)
        print("{} commits".format(trans_id))

    def abort(self, trans_id, hasSiteFailure=False):
        '''
        trans_id: transaction id

        The method calls abort on data managers for the transaction to take appropriate actions 
        '''

        for dm in self.dataManagers:
            dm.abort(trans_id)

        # Remove the transaction from the transactionQueue
        self.transactionQueue.pop(trans_id)
        
        # Check the operationQueue to find any pending operation for the transaction, add them to remove list
        removeList = []
        for operation in self.operationQueue:
            if operation.trans_id == trans_id:
                removeList.append(operation)

        #  Remove the operations stored in the remove list
        for operation in removeList:
            self.operationQueue.remove(operation)

        # print("{} aborts".format(trans_id))

        if hasSiteFailure:
            print("{} aborts due to site failure".format(trans_id))
        else:
            print("{} aborts due to deadlock".format(trans_id))

    
############## DEADLOCK DETECTION ############


    def resolveDeadlock(self):
        '''
        The method iterates over all data managers and gets the waits for graph from it.
        It finds the youngest transaction

        '''
        # Detect deadlocks using cycle detection and abort the youngest transaction in the cycle.
        
        graph = defaultdict(set)

        # Create the graph by updating the conflict edges in the graph dictionary
        
        for dm in self.dataManagers:
            if dm.isUp:
                waitsForGraph = dm.generateWaitsForGraph()
                for node, adjList in waitsForGraph.items():
                    graph[node].update(adjList)

        # print(dict(graph))

        newestTransId = None
        newestTransTs = -1

        for node in list(graph.keys()):
            visited = set()
            if self.hasCycle(node, node, visited, graph):
                if self.transactionQueue[node].timestamp > newestTransTs:
                    newestTransId = node
                    newestTransTs = self.transactionQueue[node].timestamp

        if newestTransId:
            # print("Deadlock detected: aborting {}".format(newestTransId))
            self.abort(newestTransId)
            return True

        return False

    def hasCycle(self, curr, root, visited, graph):
        '''
        curr: node from which graph is traversed
        root: node on which cycle is checked
        visited: set of nodes that have been visited
        graph: dictionary containing conflict edges

        The method traverses the graph in DFS fashion and checks if there is a cycle in the graph
        '''
        
        visited.add(curr)
        for adjNode in graph[curr]:
            if adjNode == root:
                return True
            if adjNode not in visited:
                if self.hasCycle(adjNode, root, visited, graph):
                    return True
        return False