import sys, funKit, config, requests, psycopg2, psq
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
        try:
            self.loadPage(self.translatePage(self.getPage(sesh, pageNum)))
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            
    
    def work(self):
        with funKit.doSesh() as sesh:
            for pageNum in tqdm(range(1, self.pageAmount + 1)):
                self.doPage(sesh, pageNum)
        funKit.updateOthers()


if __name__ == "__main__":
    try: 
        bind = Binder()
        funKit.baseExecute(psq.DropTable)
        bind.work()
        # funKit.baseExecute(psq.DeleteDuplicates)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Database Drop Error: %s" % error)
        sys.exit()
    finally:
        print("BINGO")
        sys.exit()

