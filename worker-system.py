import sys
sys.path.insert(0, 'C:\\Users\\Rukman\\Desktop\\project') #this the path were datafame class file is placed
from redis import Redis
from calendar import timegm
from queue import Queue
import time
from time import strptime
from dataframe import modify_dataframe
from threading import Thread

#variables
filePath = 'C:\\Users\\Rukman\\Desktop\\womens-shoes-prices\\7210_1.csv'
threadCount = 5
pipeline_limit = 200

redisComm= Redis() #uses default port 6379
taskQueue = Queue()

def worker_send_record(queue):
    pipe = redisComm.pipeline()
    while True:
        row = queue.get()
        #'id' as key and 'entire row' as value
        pipe.append(str(row['id']), str(row))

        #'date' as name of the sorted set and 'id' & 'epoch' time of dateadded timestamp as value
        pipe.zadd(str(row['dateAdded'].split('T')[0]),\
                  {str(row['id']) : timestamp_converstion(str(row['dateAdded']))})
        
        #colors - 'color' along with text ':color' is set as name of sorted set
        handle_colors(row, pipe)
        
        #brands - 'brand' along with text '_brand' is set as name of the sorted set
        #increment the sorted set with brand name everytime an item from that brand is added
        pipe.zincrby(str(row['dateAdded']).split('T')[0]+'_brand', 1, str(row['brand']))
        if len(pipe) > pipeline_limit or taskQueue.empty():
            try:
                res = pipe.execute()
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
    
    #csv to data frame, removing rows with null values & duplicates
    DF = modify_dataframe(filePath)

    #returs a list with rows as dictionary
    rows = DF.getRowsAsList()

    #start processing task from queue in multiple threads
    for i in range(threadCount):
        worker = Thread(target=worker_send_record, args=(taskQueue, ))
        worker.setDaemon(True)
        worker.start()
        
    #queue all the rows so that i will be dequeued & worked upon by the worker functions.
    for row in rows:
        taskQueue.put(row)

    #block the threads until all the tasks in the queue are completed.
    taskQueue.join()

