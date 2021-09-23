import pyspark
sc=pyspark.SparkContext('local[*]')
sc.setLogLevel('WARN')


#Loading the file into an RDD and splitting the records based on the delimiter 


tran = sc.textFile("/home/hadoop/Learnbay/pyspark/tran/transactions")
avg_credit = tran.map(lambda x:x.encode("ascii","ignore").split(",")).map(lambda x:(int(x[3]),x[6])).filter(lambda x:x[1]=="credit").map(lambda x:x[0]).mean()

avg_cash = tran.map(lambda x:x.encode("ascii","ignore").split(",")).map(lambda x:(int(x[3]),x[6])).filter(lambda x:x[1]=="cash").map(lambda x:x[0]).mean()

list_credit=[]
list_credit.append("Average credit  ")
list_credit.append(avg_credit)
sc.parallelize(list_credit).saveAsTextFile("/home/hadoop/Learnbay/pyspark/tran/avg_credit")


list_cash=[]
list_cash.append("Average cash   ")
list_cash.append(avg_cash)
sc.parallelize(list_cash).saveAsTextFile("/home/hadoop/Learnbay/pyspark/tran/avg_cash")


#Find out the month of the year 2013 where most of the transactions happened.

yearlytran = tran.map(lambda x:x.encode("ascii","ignore").split(",")).map(lambda x:(x[1],x[3])).filter(lambda x:x[0][6:10]=="2013").map(lambda x:(x[0][0:2],x[1])).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],False).take(2)

dict_month={'01':'Jan','02':'Feb','03':'Mar','04':'Apr','05':'May','06':'Jun','07':'Jul','08':'Aug','09':'Sep','10':'Oct','11':'Nov','12':'Dec'}

print("The top 2 months with maximum transactions happened")
list_2_countries={}
for i in yearlytran:
	month=str(i).split(",")[0]
	month=month.replace("(","").replace("'","")
	print(dict_month[month])

list_2_countries={}
with open('/home/hadoop/Learnbay/pyspark/tran_2months','w') as f:
	for i in yearlytran:
		month=str(i).split(",")[0].replace("(","").replace("'","")
        	tot=str(i).split(",")[1].replace(")","").replace("'","")
        	mo=dict_month[month]
        	list_2_countries[mo]=tot

	f.write("The below are the top 2 countiers with maximum amount of transaction happened")	
	for key,value in list_2_countries.items():
		f.write('%s:%s\n'%(key,value))


#Find out the city where transaction happened with the least amount.
												

least_tran_city=tran.map(lambda x:x.encode("ascii","ignore").split(",")).map(lambda x:(x[5],int(x[3]))).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1]).first()

least_city=[]
least_city.append("The city with the least amount of transaction happened is  ")
least_city.append(least_tran_city)
sc.parallelize(least_city).saveAsTextFile("/home/hadoop/Learnbay/pyspark/tran/least_city")


#Top 2 product category with maximum sales

top_2=tran.map(lambda x:x.encode("ascii","ignore").split(",")).map(lambda x:(x[4],int(x[3]))).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],False).take(2)

top_2_product=[]
top_2_product.append("The top 2 products with maximum amount of transaction happened is  ")
top_2_product.append(top_2)
sc.parallelize(top_2_product).saveAsTextFile("/home/hadoop/Learnbay/pyspark/tran/top_2_product")
