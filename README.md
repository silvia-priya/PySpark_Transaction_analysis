# PySpark_Transaction_analysis
This exercise helps to understand the different analysis that can be performed on a transaction file using spark with python

Please find the semantics of the file below.

Transaction_Number -- Unique number for every transaction made. 
Transaction_Date -- Date on which the transaction happened. 
Userno -- Usernumber who made the transaction 
Amount -- Price for which the transaction is made. 
Product -- Name of the product purchased. 
City -- City where the transaction happened. 
Payment -- Payment type.

Use Case:-
===========

Find out the average amount spent using cash and credit to make the transactions.
Find out the month of the year 2013 where transactions happened with the maximum amount.
Find out the city where the transaction happened with the least amount.
List down the top 2 product categories which has the maximum sales.


Tips to know:-
===============

Build all the related RDD's and enclosed them inside a single python file.
Make sure that the final output directory  is not existing before.
When the final output generated is a sequence say list or string then convert them into RDD using sc.parallelize method, so that you can load the output into the final directory.
