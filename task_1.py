from pyspark import SparkConf, SparkContext

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)
folder_name = "./data/"
input_file_review_name = 'yelp_top_reviewers_with_reviews.csv'
input_file_businesses_name = 'yelp_businesses.csv'
input_file_friendship_name = 'yelp_top_users_friendship_graph.csv'
output_filename = 'result_1.csv'

csv_file_review = sc.textFile(folder_name + input_file_review_name)
csv_file_businesses = sc.textFile(folder_name + input_file_businesses_name)
csv_file_friendship = sc.textFile(folder_name + input_file_friendship_name)
headers_review = csv_file_review.first()
headers_businesses = csv_file_businesses.first()
headers_friendship = csv_file_friendship.first()

#Creating RDDs
review_rdd = csv_file_review.filter(lambda line: line!=headers_review).map(lambda line: line.split('\t'))
businesses_rdd = csv_file_businesses.filter(lambda line: line!=headers_businesses).map(lambda line: line.split('\t'))
friendship_rdd = csv_file_friendship.filter(lambda line: line!=headers_friendship).map(lambda line: line.split(','))

#Counting rows
review_rows = review_rdd.count()
businesses_rows = businesses_rdd.count()
friendship_rows = friendship_rdd.count()

lines = [review_rows, businesses_rows, friendship_rows]
lines_rdd = sc.parallelize(lines)
lines_rdd.repartition(1).saveAsTextFile(folder_name + output_filename)



