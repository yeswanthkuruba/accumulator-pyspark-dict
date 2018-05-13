from pyspark import SparkContext  
from pyspark import AccumulatorParam
from pyspark.sql import HiveContext

class DictAccumulatorParam(AccumulatorParam):
    def zero(self,  value):
        dict_={}
        for i in value.keys():
            dict_[i] = set()
        return dict_
    def addInPlace(self, acc1, acc2):
        for i in acc1.keys():
            for val in acc2[i]:
                acc1[i].add(val)
        return acc1

def accumDict(line):
    global dict1
    categorical_columns = b_categorical_columns.value
    temp_dict = {}
    for i in range(len(categorical_columns)):
        temp_dict[categorical_columns[i]]=set([line[i]])
    dict1 += temp_dict
    
def distinctValues():
    sc = SparkContext()
    sqlContext = HiveContext(sc)
    data = sqlContext.sql("Select * from "+table_name)
    categorical_columns = ['a','b','c'] # column names can be given as per requirement
    global b_categorical_columns
    global dict1
    default_dict = {}
    for i in categorical_columns:
        default_dict[i] = set()

    dict1 = sc.accumulator(default_dict,DictAccumulatorParam())
    b_categorical_columns = sc.broadcast(categorical_columns)
    data_ = data[categorical_columns]
    data_.rdd.foreach(accumDict)
    cat_distinct_dict = dict1.value
    return cat_distinct_dict

if __name__ == "__main__":
    cat_distinct_dict = distinctValues()
    print(cat_distinct_dict)
    
