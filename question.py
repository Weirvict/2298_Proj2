#---------------------------------------------------
#-----------------What times have the highest traffic of sales? Per country?---------------#

#Highest traffic of sales means 
# 1.highest revenue
# 2.highest quantity sale 
# 3.more transaction order? 
#### does it included failure one? Bc placed an order but failure also cause traffic? 


df = df.select("order_id","datetime","country","qty","price","payment_txn_success")

df = df.withColumn("datetime", to_timestamp("datetime"))
df = df.withColumn("hour",hour("datetime"))

df.show(5)
spark.stop()