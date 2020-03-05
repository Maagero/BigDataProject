from pyspark import SparkConf, SparkContext


sparkConf = SparkConf().setAppName("Yelp").setMaster("local").setAll([('spark.executor.memory', '4g'),('spark.driver.memory','4g')])
sc = SparkContext(conf = sparkConf)
folder_name = "./data/"
input_file_review_name = 'yelp_top_reviewers_with_reviews.csv'
input_file_businesses_name = 'yelp_businesses.csv'
input_file_friendship_name = 'yelp_top_users_friendship_graph.csv'
output_filename = 'result_3.csv'

csv_file_businesses = sc.textFile(folder_name + input_file_businesses_name)
headers = csv_file_businesses.first()

#Create RDD
businesses_rdd = csv_file_businesses.filter(lambda line: line!=headers).map(lambda line: line.split('\t'))
businesses_rdd.cache()

#a) Average businesse rating for cities
avarage_city_rating = businesses_rdd.map(lambda fields: (fields[3], (int(fields[8]),1))).reduceByKey(lambda city1, city2: (city1[0] + city2[0], city1[1] + city2[1])).mapValues(lambda value: value[0]/value[1]).collect()

#b) Top 10 categories
top_categories = businesses_rdd.flatMap(lambda field: [(field,1) for field in field[10].split(', ')]).reduceByKey(lambda cat1, cat2: cat1 + cat2).sortBy(lambda field: field[1], False).take(10)

#c) Postal geocenter
postal_geocenter = businesses_rdd.map(lambda fields: (fields[5], [float(fields[6]), float(fields[7]), 1])).reduceByKey(lambda code1, code2: [code1[0] + code2[0], code1[1] + code2[1], code1[2] + code2[2]]).mapValues(lambda value: [value[0]/value[2], value[1]/value[2]]).collect()

lines = [avarage_city_rating, top_categories, postal_geocenter]
lines_rdd = sc.parallelize(lines)
lines_rdd.repartition(1).saveAsTextFile(folder_name + output_filename)
