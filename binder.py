import sys, funKit, psycopg2, psq, schedule, time
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


    def workAct(self):
        funKit.workAct(self.pageAmount, self.volume, self.doPage)                

    def work(self):
        schedule.every().day.at("00:00").do(self.workAct)
        schedule.every().day.at("12:00").do(self.workAct)
        while pd.to_datetime('today') < pd.to_datetime('2022-05-29'):
            schedule.run_pending()
            time.sleep(60)
        



if __name__ == "__main__":
    bind = Binder()
    bind.workAct()

