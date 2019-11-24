# Distributed-worker-system

Sample datasets can be found in https://www.kaggle.com/datasets

Abstract:
Design and implement a distributed worker system to update data in Redis using python.

Data:
DataSet to use: https://www.kaggle.com/datafiniti/womens-shoes-prices/
For this task, use the dataset given which is based on women's shoe catalog. 
The dataset contains, "id, brand, colors, dateAdded" as headers. Please assume "id" to be unique. Also ignore records which has null values in any of the columns.


Problem Statement:
There are two parts to this:
1. Database:
You have to use a Redis server to store each of the record, You are free to choose any data structure of redis that will suit the requirements.

2. Spark:
You must use spark dataframes to read the csv dataset, transform it and must insert data into redis parallely, Only the required data for the three REST APIs to function must be stored in redis.


Requirement:
Implement 3 APIs(Restful) to query and fetch data from redis, All requests and responses should be in JSON format.

    /getRecentItem - return the most recent item added on the given date
    Example:
        Input:
            2017-02-03
        Output:
            id: AVpe__eOilAPnD_xSt-H
            brand: Fashion Focus
            color: yellow

    /getBrandsCount - return the count of each brands added on the given date in descending order
    Example:
        Input:
            2017-02-03
        Output:
            +-----------------+-----+
            |            brand|count|
            +-----------------+-----+
            |Personal Identity| 4341|
            |            Mo Mo|  744|
            |  Donald J Pliner|  281|
            |             Gola|   52|
            |           Gubize|   20|
            |             Naot|   10|
            |    Fashion Focus|    6|
            |   Electric Karma|    5|
            |      Evan Picone|    2|
            |    Ros Hommerson|    1|
            |         Patrizia|    1|
             ...............     ...
             ...............     ...
             ...............     ...
            +-----------------+-----+


    /getItemsbyColor - return the top 10 latest items given input as color
    Example:
        Input:
            Blue
        Output:
            [{
            id: AVpe__eOilAPnD_xSt-H
            brand: Fashion Focus
            color: yellow
            date: 2016-11-11T09:50:34Z|
            },
            ....
            ....
            ]
