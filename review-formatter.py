import re
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

# parse reviews and extract "text" and "stars" columns
def parseJson(jsonFile):
    spark = SparkSession \
    .builder \
    .getOrCreate()

    try:
        df = spark.read.json(jsonFile)
        df = df.select("review_id", "stars", "text")
    except:
        print >> sys.stderr, "Unable to load json file"

    return df

# Converts raw star ratings into adjusted bins as follows:
#       4 - 5 -> 5
#       2 - 4 -> 3
#       0 - 2 -> 1
def filterStars(x):
    if x > 4.0:
        return 5
    elif x > 2.0:
        return 3
    else:
        return 1

# format the text to include only the words
def removePunc(text):
    return re.sub(r'[^\w\s]','', text).replace("\n", "").replace("\r", "").lower()

if __name__ == "__main__":
    inputJson = sys.argv[1]
    outputDir = sys.argv[2]

    df = parseJson(inputJson)

    # format text
    formatText = udf(removePunc)
    df = df.withColumn("text", formatText(df["text"]))

    # adjust ratings to fit star bins
    adjustStars = udf(filterStars, IntegerType())
    df = df.withColumn("stars", adjustStars(df["stars"]))

    # write to file in specified format for Yelp classifier
    df.write.save(path=outputDir, format='csv', mode='append', sep=' ')

