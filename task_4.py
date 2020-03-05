from pyspark import SparkConf, SparkContext


sparkConf = SparkConf().setAppName("Yelp").setMaster("local").setAll([('spark.executor.memory', '4g'),('spark.driver.memory','4g')])
sc = SparkContext(conf = sparkConf)
folder_name = "./data/"
input_file_review_name = 'yelp_top_reviewers_with_reviews.csv'
input_file_businesses_name = 'yelp_businesses.csv'
input_file_friendship_name = 'yelp_top_users_friendship_graph.csv'
output_filename = 'result_4.csv'

csv_file_friendship = sc.textFile(folder_name + input_file_friendship_name)
headers = csv_file_friendship.first()


friendship_rdd = csv_file_friendship.filter(lambda line: line!=headers).map(lambda line: line.split(','))

#Helpers in and out for each id
out_rdd = friendship_rdd.map(lambda fields: (fields[1],1)).reduceByKey(lambda id1, id2: id1 + id2).sortBy(lambda field: field[1], False)
in_rdd = friendship_rdd.map(lambda fields: (fields[0],1)).reduceByKey(lambda id1, id2: id1 + id2).sortBy(lambda field: field[1], False)

#a) Top nodes in and out
top_ten_out = out_rdd.take(10)
top_ten_in = in_rdd.take(10)

#b) Mean and median
mean_out = out_rdd.map(lambda field: field[1]).mean()
mean_in = in_rdd.map(lambda field: field[1]).mean()

out_middle = int(out_rdd.count()/2)
in_middle = int(in_rdd.count()/2)

median_out = out_rdd.zipWithIndex().filter(lambda field: field[1]==out_middle).collect()[0][0][1]
median_in = in_rdd.zipWithIndex().filter(lambda field: field[1]==in_middle).collect()[0][0][1]

lines = [top_ten_out, top_ten_in, mean_out, mean_in, median_out, median_in]
lines_rdd = sc.parallelize(lines)
lines_rdd.repartition(1).saveAsTextFile(folder_name + output_filename)

