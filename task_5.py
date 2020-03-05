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
df_friendship = sqlContext.read.format('csv').options(header='true', inferSchema='true', delimiter=',').load(folder_name + input_file_friendship_name).toDF('src_user_id', 'dst_user_id')


df_review.show()
df_businesses.show()
df_friendship.show()
