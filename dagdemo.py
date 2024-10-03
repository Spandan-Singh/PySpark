# Create DataFrame from data source - csv file
customerDF = spark.read.load("/FileStore/tables/dagdemo/customers.txt", format="csv", sep="\t", inferSchema="true", header="true")

cDF1=customerDF.filter(customerDF['customer_state'] == 'CA')
from pyspark.sql.functions import col, lit
cDF2=cDF1.withColumn('customer_country',lit('USA'))
cDF3=cDF2.groupBy("customer_state").count()

cDF3.show(5)

cDF3.count()
