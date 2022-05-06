import schedule, time
from binder import Binder
import pandas as pd


def job(binder):
    try:
        binder.work()
        with open('log_schedule', 'a') as log:
            log.write(str(pd.to_datetime('today')) + '\n')
    except Exception as error:
        with open('log_schedule', 'a') as log:
            log.write(str(pd.to_datetime) + '\n' + str(error))

binder = Binder()
schedule.every().day.at("00:00").do(job, binder)
schedule.every().day.at("12:00").do(job, binder)

while pd.to_datetime('today') < pd.to_datetime('2022-05-08'):
    schedule.run_pending()
    time.sleep(60)

