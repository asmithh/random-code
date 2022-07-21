# from pyspark.sql import SparkSession
# from pyspark.conf import SparkConf
# from pyspark.sql.functions import col, lit, unix_timestamp
# import pyspark.sql.types as T
# import hdfs
# import json
# from pyspark.sql.functions import udf
import numpy as np
import pickle
import pandas as pd
import faiss
from dask import dataframe as dd

if __name__ == '__main__':
#     conf = SparkConf()
#     conf.set('spark.kryoserializer.buffer.mb', '2048')
#     sesh = SparkSession.builder.appName('alyssa_embeddings_to_csv').config(conf=conf).getOrCreate()
#     df = sesh.read.parquet('hdfs://megatron.ccs.neu.edu/user/etorf/followee_sample_embeddings')
    # get vectors & tweet IDs (so we can evaluate clusters later)
    df = dd.read_parquet(
        'hdfs://megatron.ccs.neu.edu/user/etorf/followee_sample_embeddings/', 
        column=['id', 'embed'], 
        sort=True, 
        parquet_file_extension=('.gz.parquet')
        engine='pyarrow'
    )#, engine='fastparquet')
    print(df.columns)
    df = df.to_dask_array()
    print('dask array success')
    df.compute_chunk_sizes()
    print('computed chunk sizes')
#     vec = df.select('embed').collect()
#     tweet_ids = df.select('id').collect()
    vec = df[:, 1]
    tweet_ids = df[:, 0].astype('str')
    vec = vec.astype('float32')
    pickle.dump(tweet_ids, open('/scratch/asmithh/tweet_ids_sample.pkl', 'wb'))
    # np.save('/scratch/asmithh/sample_tweet_vecs.npy', vec)
    print(vec.shape)    
    kmeans = faiss.Kmeans(768, 20, gpu=2)
    kmeans.train(vec)
    # then we grab the cluster assignments for each vector
    dists, assignments = kmeans.index.search(data, 1)
    # put them into buckets by cluster w/ distances from centroids
    clusters = {}
    for tweet_id, dist, assignment in zip(tweet_ids, dists, assignments):
        if assignment[0] in clusters:
            if len(clusters[assignment[0]]) < 100:
                clusters[assignment[0]] = clusters[assignment[0]] + [(tweet_id, dist)]
            elif min([c[1] for c in clusters[assignment[0]]]) > dist:
                clusters[assignment[0]] = [c for c in clusters[assignment[0]] if c[1] < dist] + [(tweet_id, dist)]
        else:
            clusters[assignment[0]] = [(tweet_id, dist)]
    pickle.dump(clusters, open('/scratch/asmithh/full_clusters.pkl', 'wb'))
    # sesh.stop()

