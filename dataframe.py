import pyspark

class modify_dataframe():

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    
    def __init__(self, filePath):
        self.filePath = filePath
        self.dfObj = modify_dataframe.spark.read.csv(self.filePath, header=True)
        
    def getRowsAsList(self):
        '''
        Drop duplicate rows,
        remove unnecessary columns &
        return list of dictionary objects
        '''
        
        notWants = [column for column in self.dfObj.columns if column not in \
                    ['id', 'dateAdded', 'brand', 'colors']]
        
        for notWant in notWants:
            self.dfObj = self.dfObj.drop(notWant)    
        self.dfObj = self.dfObj.dropna().dropDuplicates().collect()

        rowsAsDict = [row.asDict() for row in self.dfObj]
        
        return rowsAsDict

        
