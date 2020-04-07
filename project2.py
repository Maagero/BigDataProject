from pyspark import SparkConf, SparkContext
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.types import Row
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
import base64
import datetime

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)
spark = SparkSession(sc)

folder_name = "./data/"
input_file_review_name = 'yelp_top_reviewers_with_reviews.csv'

input_file_afinn_name = 'AFINN-111.txt'
input_file_stopwords_name = 'stopwords.txt'
output_filename = 'part2result'

csv_file_review = sc.textFile(folder_name + input_file_review_name)
file_afinn = sc.textFile(folder_name + input_file_afinn_name)
file_stopwords = sc.textFile(folder_name + input_file_stopwords_name)
headers_review = csv_file_review.first()

#Creating RDDs
review_rdd = csv_file_review.filter(lambda line: line!=headers_review).map(lambda line: line.split('\t'))
afinn_rdd = file_afinn.map(lambda line: line.split('\t')).map(lambda line: (line[0], line[1]))

#Create sentiment lexicon and stopword list
lexicon = afinn_rdd.collectAsMap()
stopwords_list = file_stopwords.collect()


#Creating samples for faster testing
sample_review_rdd = review_rdd.sample(False, 0.1, 5)


#Decode the reviews
decoded_reviews = review_rdd.map(lambda line: (line[2], base64.b64decode(line[3]).decode("utf-8")))

#Tokenize with use of dataframe
df = decoded_reviews.toDF()
tokenizer = Tokenizer(inputCol='_2', outputCol='words_token')
tokenized = tokenizer.transform(df)

#Remove stop words
remover = StopWordsRemover(inputCol='words_token', outputCol='words_clean', stopWords=stopwords_list)
stop_word_free = remover.transform(tokenized).select('_1', 'words_clean')

#Find polarity
def polarity(s_list):
    score = 0
    for s in s_list:
        if not lexicon.get(s):
            continue
        score += int(lexicon.get(s))
    return score
polarity_udf = udf(polarity, LongType())

k = 10
def setK(k):
    k = k

df = stop_word_free.select('_1', polarity_udf('words_clean').alias('polarity'))

#Go back to rdd from dataframe
polarity_rdd = df.rdd.map(tuple)
result_rdd = polarity_rdd.reduceByKey(lambda value1, value2: value1 + value2).sortBy(lambda value: value[1], False).take(k)


#Saving the file
sc.parallelize(result_rdd).repartition(1).saveAsTextFile(folder_name + output_filename)
