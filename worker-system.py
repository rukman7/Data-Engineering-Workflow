import sys
sys.path.insert(0, 'C:\\Users\\Rukman\\Desktop\\project') #this the path were datafame class file is placed
import json
from redis import Redis
from calendar import timegm
from queue import Queue
from time import strptime
from dataframe import modify_dataframe
from threading import Lock, Thread

#variables
filePath = 'C:\\Users\\Rukman\\Desktop\\womens-shoes-prices\\7210_1.csv'
threadCount = 5

redisComm= Redis() #uses default port 6379
pipe = redisComm.pipeline()
taskQueue = Queue()
lock = Lock()

def worker_send_record(queue):
    while True:
        row = queue.get()
        pipe.append(str(row['id']), str(row))
        pipe.zadd(str(row['dateAdded'].split('T')[0]),\
                  {str(row['id']) : timestamp_converstion(str(row['dateAdded']))})
        handle_colors(row, pipe)
        pipe.zincrby(str(row['dateAdded']).split('T')[0]+'_brand', 1, str(row['brand']))
        if len(pipe) > 20 or taskQueue.empty():
            try:
                lock.acquire(True)
                res = pipe.execute()
                lock.release()
            except Exception as e:
                 print(e)
                 return 'failure'
        queue.task_done()

def timestamp_converstion(datetimeStamp):
    '''
    Converts the readable time stamp to epoch time.
    '''
    UTCTime = strptime(datetimeStamp, "%Y-%m-%dT%H:%M:%SZ")
    epochTime = timegm(UTCTime)
    return epochTime

def handle_colors(row, pipe):
    colors = row['colors'].split(',')
    for color in colors:
        pipe.zadd(color+':color', {str(row['id']) :\
                                   timestamp_converstion(str(row['dateAdded']))})

if __name__ == "__main__":

    DF = modify_dataframe(filePath)
    rows = DF.getRowsAsList()

    #start processing task from queue in multiple threads
    for i in range(threadCount):
        worker = Thread(target=worker_send_record, args=(taskQueue, ))
        worker.setDaemon(True)
        worker.start()
        
    #queue all the rows so that i will be dequeued & worked upon by the worker functions.
    for row in rows:
        taskQueue.put(row)

    #stop the threads when all the tasks in the queue are completed.
    taskQueue.join()
