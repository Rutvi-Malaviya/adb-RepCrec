
class Transaction:
    def __init__(self, time, id, ro, dbSnapshot = None):
        '''
        timestamp: time when the transaction began
        trans_id: transaction id
        isReadOnly: indicated if the transaction is readonly or not
        willCommit: Indicates if the transaction can commit when end() is called. Aborts if willCommit is false
        accessedSites: set of the sites accessed by the transaction 
        dbSnapshot: Dictionary of the variables at the time the transaction started. Used only for read only transactions.
                    key is the variable id and values are the values of the variable read at the time transaction began
        '''
        self.timestamp = time 
        self.trans_id = id 
        self.isReadOnly = ro
        self.willCommit = True
        self.accessedSites = set()
        self.dbSnapshot = dbSnapshot

    def getStartTime(self):
        return self.timestamp

    def getId(self):
        return self.trans_id

    def canCommit(self):
        return self.willCommit

    def getAccessedSites(self):
        return self.accessedSites

    def addSite(self, site):
        '''
        Adds a site to the accessedSites list
        '''
        self.accessedSites.add(site)