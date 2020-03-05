from pyspark import SparkConf, SparkContext
import math
import datetime

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf=sparkConf)
folder_name = "./data/"
input_file_review_name = 'yelp_top_reviewers_with_reviews.csv'
input_file_businesses_name = 'yelp_businesses.csv'
input_file_friendship_name = 'yelp_top_users_friendship_graph.csv'

csv_file_review = sc.textFile(folder_name + input_file_review_name)
headers = csv_file_review.first()
lines = csv_file_review.filter(lambda line: line != headers)

# Creating RDDs
review_rdd = lines.map(lambda line: line.split('\t'))
# review_rdd.cache()

# a) Distinct user
#distict_user_rdd = review_rdd.map(lambda fields: fields[1]).distinct()
#distinct_users = distict_user_rdd.count()

# b) Average chars per review
#total_chars = review_rdd.map(lambda fields: (len(fields[3]),1)).reduce(lambda tot_count1, tot_count2: (tot_count1[0] + tot_count2[0], tot_count1[1] + tot_count2[1]))
#average_chars_per_review = math.floor(total_chars[0]/total_chars[1])
#print(average_chars_per_review)

# c) Top Businiess IDs
#review_per_business = review_rdd.map(lambda fields: (fields[2], 1)).reduceByKey(lambda id1,id2: id1+id2)
#top_reviewed = review_per_business.sortBy(lambda field: field[1], False).take(10)

# d) Revies each year
reviews_each_year = review_rdd.map(
    lambda field: (datetime.datetime.fromtimestamp(int(float(field[4]))).year, 1)).reduceByKey(
    lambda year1, year2: year1 + year2).collect()
#print(reviews_each_year)

# e) Last and first date
# first_review = datetime.datetime.fromtimestamp(int(float(review_rdd.map(lambda field: float(field[4])).reduce(lambda num1,num2: num1 if num1<num2 else num2))))
# last_review = datetime.datetime.fromtimestamp(int(float(review_rdd.map(lambda field: float(field[4])).reduce(lambda num1,num2: num1 if num1>num2 else num2))))
# print(first_review)
# print(last_review)

# PCC
# Number of review per UserID
#numberOfReviewsByUsers = review_rdd.map(lambda fields: (fields[1], 1)).reduce(lambda id1, id2: id1 + id2)

def calculatePCC(x, y):
    return x, y
