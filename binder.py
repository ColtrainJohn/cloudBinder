import sys, funKit, psycopg2, psq
from tqdm import tqdm


class Binder:
    """
        Binder takes page, translate it and put it to database
    """
    def __init__(self):
        self.volume, self.pageAmount = funKit.measureVolume()
        print(f'Base volume: {self.volume}\nPage amount: {self.pageAmount}')


    def getPage(self, sesh, pageNum):
        return funKit.takeCloudJson(sesh, pageNum)
        

    def translatePage(self, page):
        return funKit.translate(page)
        

    def loadPage(self, page):
        return funKit.putToDatabase(page)
        

    def doPage(self, sesh, pageNum):
            self.loadPage(self.translatePage(self.getPage(sesh, pageNum)))

            
    def work(self):
        with funKit.doSesh() as sesh:
            for pageNum in tqdm(range(1, self.pageAmount + 1)):
                self.doPage(sesh, pageNum)
        funKit.updateTables()



if __name__ == "__main__":
    bind = Binder()
    funKit.baseExecute(psq.DropTable)
    bind.work()
    # funKit.baseExecute(psq.DeleteDuplicates) #!#!#!#!#!#


