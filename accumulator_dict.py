from pyspark import SparkContext  
from pyspark import AccumulatorParam

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
    categorical_columns = ['a','b','c'] # column names can be given as per requirement
    temp_dict = {}
    for i in range(len(categorical_columns)):
        temp_dict[categorical_columns[i]]=set([line[i]])
    dict1 += temp_dict

    
    
default_dict = {}
for i in categorical_columns:
    default_dict[i] = set()

global dict1
dict1 = sc.accumulator(default_dict,DictAccumulatorParam())

print(dict1)
