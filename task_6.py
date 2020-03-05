from pyspark import SparkConf, SparkContext
from pyspark.sql.context import SQLContext



sparkConf = SparkConf().setAppName('Yelp').setMaster('local').setAll([('spark.executor.memory', '4g'),('spark.driver.memory','4g')])
sc = SparkContext(conf = sparkConf)
folder_name = './data/'
input_file_review_name = 'yelp_top_reviewers_with_reviews.csv'
input_file_businesses_name = 'yelp_businesses.csv'
input_file_friendship_name = 'yelp_top_users_friendship_graph.csv'
output_filename = 'result_4.csv'


sqlContext = SQLContext(sc)
df_review = sqlContext.read.format('csv').options(header='true', inferSchema='true', delimiter='\t').load(folder_name + input_file_review_name).toDF('review_id', 'user_id', 'business_id', 'review_text', 'review_date')
df_businesses = sqlContext.read.format('csv').options(header='true', inferSchema='true', delimiter='\t').load(folder_name + input_file_businesses_name).toDF('business_id', 'name', 'address', 'city', 'state', 'postal_code', 'latitude', 'longitude', 'stars', 'review_count', 'categories')

df_review.createOrReplaceTempView('reviews')
df_businesses.createOrReplaceTempView('businesses')

#a) innerjoin review and businesse on businesse id
rev_bus_table = sqlContext.sql('select reviews.user_id from reviews inner join businesses on reviews.business_id = businesses.business_id')

#b) Creating temp table
rev_bus_table.createOrReplaceTempView('newTempTable')

#c) Most reviews top 20 users
sqlContext.sql('select Count(user_id), user_id from newTempTable group by user_id order by Count(user_id) DESC LIMIT 20').show()

