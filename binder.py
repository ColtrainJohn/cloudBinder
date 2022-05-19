import sys, funKit, psycopg2, psq, schedule, time
from tqdm import tqdm
import pandas as pd


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

            
    # def work(self):
    #     funKit.baseExecute(psq.DropTable)
    #     with funKit.doSesh() as sesh:
    #         for pageNum in tqdm(range(1, self.pageAmount + 1)):
    #             self.doPage(sesh, pageNum)
    #     funKit.updateTables()


    def workAct(self):
        try:
            funKit.baseExecute(psq.DropTable)
            with funKit.doSesh() as sesh:
                for pageNum in tqdm(range(1, self.pageAmount + 1)):
                    self.doPage(sesh, pageNum)
            funKit.updateTables()
            with open('log_schedule', 'a') as log:
                log.write(str(pd.to_datetime('today')) + '\n')
        except Exception as error:
            with open('log_schedule', 'a') as log:
                log.write(str(pd.to_datetime('today')) + '\n' + str(error))
                

    def work(self):
        schedule.every().day.at("00:00").do(self.workAct)
        schedule.every().day.at("12:00").do(self.workAct)
        while pd.to_datetime('today') < pd.to_datetime('2022-05-21'):
            schedule.run_pending()
            time.sleep(60)
        



if __name__ == "__main__":
    bind = Binder()
    bind.workAct()
    # funKit.baseExecute(psq.DeleteDuplicates) #!#!#!#!#!#

