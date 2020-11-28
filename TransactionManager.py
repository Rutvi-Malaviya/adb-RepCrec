import re
from Transaction import Transaction
from DataManager import DataManager
from collections import defaultdict

class Operation:
    def __init__(self, cmd, trans_id, var, val=0):
        self.cmd = cmd
        self.trans_id = trans_id
        self.var = var
        self.val = val

    def __repr__(self):
        return '(' + self.cmd + ',' + self.trans_id + "," + self.var + ',' + self.val + ')'

class TransactionManager:
    def __init__(self):
        self.transaction_queue = {}
        self.timestamp = 0
        self.operation_queue = []
        self.data_managers = []

        for i in range(1,11):
            self.data_managers.append(DataManager(i))

    def process_line(self, command):
        #  Parse input, resolve deadlock, process instructions and operations.
        tokens = re.findall(r"[\w']+",command)
        # print('tokens:',tokens)     
        self.process_instruction(tokens[0],tokens[1:])
        if self.resolve_deadlock():
            self.execute_operations()
        self.timestamp = self.timestamp + 1
            
    def process_instruction(self, cmd, args):
        # Process an instruction. If the instruction is Read or Write, add it to the operation queue, otherwise, execute the instruction directly.

        if cmd == 'begin':
            if(args[0] in self.transaction_queue.keys()):
                print("transaction id {} already present", args[0])
            else:
                self.transaction_queue[args[0]] = Transaction(self.timestamp, args[0], False)
                print("{} begins".format(args[0]))

        elif cmd == 'beginRO':
            if(args[0] in self.transaction_queue.keys()):
                print("transaction id {} already present", args[0])
            else:
                varList = self.read_snapshot()
                self.transaction_queue[args[0]] = Transaction(self.timestamp, args[0], True, varList)
                print("{} begins and is read-only".format(args[0]))

        elif cmd == 'W':
             # Insert a Write Operation to the operation queue.
            trans_id = args[0]
            var = args[1]
            val = args[2]
            if(trans_id in self.transaction_queue.keys()):
                self.operation_queue.append(Operation("W", trans_id, var, val))
            else:
                print("Transaction {} not found".format(trans_id))

        elif cmd == 'R':
            #  Insert a Read Operation to the operation queue.
            trans_id = args[0]
            var = args[1]
            if(trans_id in self.transaction_queue.keys()):
                self.operation_queue.append(Operation("R", trans_id, var))
            else:
                print("Transaction {} not found".format(trans_id))

        elif cmd == 'dump':
            self.dump() 

        elif cmd == 'end':
            trans_id = args[0]

            if(trans_id in self.transaction_queue.keys()):
                if(self.transaction_queue[trans_id].canCommit()):
                    self.commit(trans_id, self.timestamp)
                else:
                    self.abort(trans_id, True)
            else:
                print("Transaction {} not found".format(trans_id))

        elif cmd == 'fail':
            siteId = args[0]
            dm = self.data_managers[siteId-1]
            dm.fail(self.timestamp)
            print("site {} fails".format(siteId))

            for transaction in self.transaction_queue:
                if (not transaction.isReadOnly) and (transaction.canCommit) and (siteId in transaction.accessedSites):
                    transaction.canCommit = False

        elif cmd == 'recover':
            siteId = args[0]
            dm = self.data_managers[siteId-1]
            dm.recover(self.timestamp)
            print("site {} recovers".format(siteId))

        else:
            print("Invalid operation")
 
 
    def execute_operations(self):
        # Go through operation queue and execute any executable operations.
        removeOperations = []
        for operation in self.operation_queue:
            trans_id = operation.trans_id
            var = operation.var
            if trans_id in self.transaction_queue.keys():
                success = False
                
                if operation.cmd == "R":
                    if self.transaction_queue[trans_id].isReadOnly:
                        # success = self.readSnapshot(trans_id, var)
                        transaction = self.transaction_queue[trans_id]
                        if var in transaction.db_snapshot.keys():
                            success = True
                            print("{}: {}".format(var, self.transaction_queue[trans_id].db_snapshot[var]))
                        else:
                            success = False
                            print("var not found")
                    else:
                        success = self.read(trans_id, var)
                else:
                    val = operation.val 
                    success = self.write(trans_id, var, val)

                if success:
                    removeOperations.append(operation)
            else:
                removeOperations.append(operation)
            
        for operation in removeOperations:
            self.operation_queue.remove(operation)

        print("Remaining operations: ", self.operation_queue)

    def read_snapshot(self):
        # Perform read operation for read-only transactions
        varList = {}
        for dm in self.data_managers:
            if dm.isUp:
                tempList = dm.readSnapshot(self.timestamp)

                for var in tempList.keys():
                    varList[var] = tempList[var]

        print("snapshot read is: ", varList)
             
        return varList

    def read(self, trans_id, var):
        # Perform read operation for normal transactions.
        if trans_id in self.transaction_queue.keys():
            ts = self.transaction_queue[trans_id].timestamp

            for dm in self.data_managers:
                if dm.isUp and dm.hasVariable(var):
                    result = dm.read(var)

                    if result.success:
                        self.transaction_queue[trans_id].addSite(dm.siteId)
                        print("{} reads {}.{}".format(trans_id, var, result.val))
                        # print("{}: {}", var, result.val)
                        return True

        return False

    def write(self, trans_id, var, val):
        # Perform write operation for normal transactions
        if trans_id in self.transaction_queue.keys():
            ts = self.transaction_queue[trans_id].timestamp

            allSitesDown = True
            hasAllWriteLocks = True

            for dm in self.data_managers:

                if dm.isUp and dm.hasVariable(var):
                    allSitesDown = False
                    gaveLock = dm.getWriteLock(trans_id, var)

                    if not gaveLock:
                        hasAllWriteLocks = False

            if (not allSitesDown) and (hasAllWriteLocks):
                sitesModified = []
                
                for dm in self.data_managers:
                    if dm.isUp and dm.hasVariable(var):
                        dm.write(trans_id, var, val)
                        self.transaction_queue[trans_id].addSite(dm.siteId)
                        sitesModified.append(dm.getSiteId())
                        # print('here')

                print("Transaction {} writes {} with {} to the sites: {}".format(trans_id, var, val, sitesModified))
                return True

        return False

    #  Handles the commit of a transaction
    def commit(self, trans_id, time):
        for dm in self.data_managers:
            dm.commit(trans_id, time)

        self.transaction_queue.pop(trans_id)
        print("{} commits".format(trans_id))

    def abort(self, trans_id, has_site_failure=False):
        for dm in self.data_managers:
            dm.abort(trans_id, has_site_failure)

        self.transaction_queue.pop(trans_id)
        
        removeList = []
        for operation in self.operation_queue:
            if operation.trans_id == trans_id:
                removeList.append(operation)

        for operation in removeList:
            self.operation_queue.remove(operation)

        # print("{} aborts".format(trans_id))

        if(has_site_failure):
            print("Transaction {} aborts due to site failure".format(trans_id))
        else:
            print("Transaction {} aborts due to deadlock".format(trans_id))

    # Print the details for each site using its data manager
    def dump(self):
        print("dump:")

        for dm in self.data_managers:
            dm.dump()

    
    def resolve_deadlock(self):
        # Detect deadlocks using cycle detection and abort the youngest transaction in the cycle.
        
        graph = defaultdict(set)
        for dm in self.data_managers:
            if dm.isUp:
                waitsForGraph = dm.generateWaitsForGraph()
                for node, adjList in waitsForGraph.items():
                    graph[node].update(adjList)

        # print(dict(blocking_graph))

        oldestTransId = None
        oldestTransTs = -1

        for node in graph.keys():
            visited = set()
            if hasCycle(node, node, visited, graph):
                if self.transaction_queue[node].timestamp > oldestTransTs:
                    oldestTransId = node
                    oldestTransTs= self.transaction_table[node].ts
        if oldestTransId:
            print("Deadlock detected: aborting {}".format(oldestTransId))
            self.abort(oldestTransId)
            return True
        return False

    def hasCycle(self, curr, root, visited, graph):
        # Helper function that detects cycle in blocking graph using dfs.
        visited.add(curr)
        for adjNode in graph[curr]:
            if adjNode == root:
                return True
            if adjNode not in visited:
                if hasCycle(adjNode, root, visited, graph):
                    return True
        return False