import sys, funKit, config, requests, psycopg2, psq


class Binder:
    """
        Binder takes page, translate it and put it to database
    """
    def __init__(self):
        self.volume, self.pageAmount = funKit.measureVolume()


    def getPage(self, sesh, pageNum):
        page = funKit.takeFromCloud(sesh, pageNum)
        return page


    def translatePage(self, page):
        pdPage = funKit.translate(page.json())
        return pdPage


    def loadPage(self, page):
        pageStatusCode = funKit.putToDatabase(page)
        return pageStatusCode


    def doPage(self, pageNum):
        try:
            with funKit.doSesh() as sesh:
                page = self.getPage(sesh, pageNum)
                pageTuples = self.translatePage(page)
                code = self.loadPage(pageTuples)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
        finally:
            print(f'Page {pageNum}: OK')
    
    
    def work(self):
        for num in range(1, self.pageAmount + 1):
            self.doPage(num)

if __name__ == "__main__":
    bind = Binder()
    try: 
        funKit.baseExecute(psq.DropTable)
        bind.work()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Database Drop Error: %s" % error)
        sys.exit()
    funKit.checkBaseContant()

