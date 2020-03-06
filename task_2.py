from pyspark import SparkConf, SparkContext
import math
import datetime
import base64
import scipy.stats

sparkConf = SparkConf().setAppName("Yelp").setMaster("local").setAll([('spark.executor.memory', '4g'),('spark.driver.memory','4g')])
sc = SparkContext(conf = sparkConf)
folder_name = "./data/"
input_file_review_name = 'yelp_top_reviewers_with_reviews.csv'
input_file_businesses_name = 'yelp_businesses.csv'
input_file_friendship_name = 'yelp_top_users_friendship_graph.csv'
output_filename = 'result_2.csv'

csv_file_review = sc.textFile(folder_name + input_file_review_name)
headers = csv_file_review.first()

#Creating RDDs
review_rdd = csv_file_review.filter(lambda line: line!=headers).map(lambda line: line.split('\t'))
review_rdd.cache()

#a) Distinct user
distinct_users = review_rdd.map(lambda fields: fields[1]).distinct().count()

#b) Average chars per review
total_chars = review_rdd.map(lambda fields: (len((base64.b64encode(fields[3].encode('ascii')).decode('ascii'))),1)).reduce(lambda tot_count1, tot_count2: (tot_count1[0] + tot_count2[0], tot_count1[1] + tot_count2[1]))
average_chars_per_review = math.floor(total_chars[0]/total_chars[1])

#c) Top Businiess IDs
review_per_business = review_rdd.map(lambda fields: (fields[2], 1)).reduceByKey(lambda id1,id2: id1+id2)
top_reviewed = review_per_business.sortBy(lambda field: field[1], False).take(10)

#d) Revies each year
reviews_each_year = review_rdd.map(lambda field: (datetime.datetime.fromtimestamp(int(float(field[4]))).year, 1)).reduceByKey(lambda year1,year2 : year1+year2).collect()

#e) Last and first date
first_review = datetime.datetime.fromtimestamp(int(float(review_rdd.map(lambda field: float(field[4])).reduce(lambda num1,num2: num1 if num1<num2 else num2))))
last_review = datetime.datetime.fromtimestamp(int(float(review_rdd.map(lambda field: float(field[4])).reduce(lambda num1,num2: num1 if num1>num2 else num2))))


#f) ????
numOfReviewsPerUser = review_rdd.map(lambda field: (field[1], 1)).reduceByKey(lambda id1, id2: id1+id2)
charsInReviewsPerUser = review_rdd.map(lambda field: (field[1], len((base64.b64decode(field[3].encode('ascii')).decode('ascii'))))).reduceByKey(lambda id1, id2: id1 + id2)
joined = numOfReviewsPerUser.join(charsInReviewsPerUser).collect()
r = map(lambda x, y: scipy.stats.pearsonr(x, y), joined[0], joined[1])

lines = [distinct_users, average_chars_per_review, top_reviewed, reviews_each_year, first_review, last_review]
lines_rdd = sc.parallelize(lines)
lines_rdd.repartition(1).saveAsTextFile(folder_name + output_filename)

