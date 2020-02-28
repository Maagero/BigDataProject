from pyspark import SparkConf, SparkContext

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)
folder_name = "./data/"
input_file_review_name = 'yelp_top_reviewers_with_reviews.csv'
input_file_businesses_name = 'yelp_businesses.csv'
input_file_friendship_name = 'yelp_top_users_friendship_graph.csv'

csv_file_review = sc.textFile(folder_name + input_file_review_name)

#Creating RDDs
review_rdd = csv_file_review.map(lambda line: line.split('\t'))
distict_user_rdd = review_rdd.map(lambda fields: fields[1]).distinct()

distinct_users = distict_user_rdd.count()

total_chars_and_reviews = review_rdd.map(lambda fields: (len(fields[3]),1)).reduce(lambda (tot_count1,review_count1), (tot_count2,review_count2): (tot_count1 + tot_count2, review_count1 + review_count2))
(average_chars = sc.parallelize(float(total_chars_and_reviews[0]/total_chars_and_reviews[1])).repartition(1))


