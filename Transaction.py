
class Transaction:
    def __init__(self, time, id, ro, db_snapshot = None):
        self.timestamp = time 
        self.transaction_id = id 
        self.isReadOnly = ro
        self.willCommit = True
        self.accessedSites = set()
        self.db_snapshot = db_snapshot

    def getStartTime(self):
        return self.timestamp

    def getId(self):
        return self.transaction_id

    def canCommit(self):
        return self.willCommit

    def getAccessedSites(self):
        return self.accessedSites

    def addSite(self, site):
        self.accessedSites.add(site)