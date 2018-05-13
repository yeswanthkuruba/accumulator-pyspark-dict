# accumulator-pyspark-dict

Motivation:- 
Normally we use to get disctinct values of each column in table by using ".distinct()" for each column in a loop.
If your data is in billions or in millions, the time taken by this loop will be very high, as it needs to scan the entire data for as many times as number of columns.
spark has an inbulit feature called accumulator - A shared variable that can be accumulated, i.e., has a commutative and associative "add" operation.

To get the distinct values of each column in a table with one single scan of table can be achived by Custom Accumulators with dictionary. 

Custom Accumulators in Spark using python (pyspark).

Accumulators are shared variable and can be used to maintain the counters or sum across the RDD(Resilient Distributed Dataset).

While writing this page I am assuming that visitors are aware of RDD and apache spark.I am using python and pyspark to demonstrate the accumulator.

To create the custom accumulator,programmer have to subclass AccumulatorParam interface.Programmer has to implement zero and addInPlace method.Accumulator supports those operation which are associative,like + operator.So programmer have to provide the zero vector for this operator.

Usage:-
To bulid a decision tree in pyspark you need to feed the categorical variable columns and there values in a dictionary to the model.
Here you can use this piece of code to create a dict which will have the column names as keys and corresponding distinct values as value.
