from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, unix_timestamp
import pyspark.sql.types as T
import hdfs
import json
from pyspark.sql.functions import udf

if __name__ == '__main__':
    def get_extensions_items(tup):
        if tup is None:
            return '0'
        if 'media' in tup and  'type' in tup['media'][0]:
            media_type = tup['media'][0]['type']
            return media_type
        return '0'
    TWEET_SCHEMA="/net/data/twitter-covid/tweet_schema.json"
    sesh = SparkSession.builder.appName('alyssa_twitter_image_count').getOrCreate()
    sesh.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    sesh.udf.register('getExtensionsItems', get_extensions_items)
    df = sesh.read.json('hdfs://megatron.ccs.neu.edu/user/nir/panel_tweets/*2021*/*/*', schema=T.StructType.fromJson(json.load(open(TWEET_SCHEMA))))
#     with open('plz.txt', 'w') as f:
#         for col in df.columns:
#             f.write(col)
#             f.write('\n')
    extension_udf = udf(get_extensions_items, T.StringType())
    q2 = """unix_timestamp(created_at, 'EEE MMM dd HH:mm:ss ZZZZZ yyyy') BETWEEN unix_timestamp('2021-01-01', 'yyyy-MM-dd') AND unix_timestamp('2021-12-31', 'yyyy-MM-dd')"""
    df2 = df.filter(q2)
    c = df2.select('extended_entities', extension_udf('extended_entities').alias('entity_extensions'))
    c.groupBy('entity_extensions').count().show()
    sesh.stop()

