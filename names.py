from pyspark.sql.functions import explode, col, lit, rand, asc, regexp_replace, lower, array, udf, collect_set, rank, collect_list, split, concat, broadcast, trim
from nltk.stem.porter import *
from nltk.stem.snowball import SnowballStemmer
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer

import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

hdfs_mpdDir = "hdfs:///recsys_spotify_2018/mpd.v1/mpd.slice.*.json"
hdfs_challengeDir = "hdfs:///recsys_spotify_2018/challenge.v1/*.json"
hdfs_userDir = "hdfs:///user/XXX"
hdfs_namePath = hdfs_userDir + "/weightedNames"

df = spark.read.json(hdfs_mpdDir, multiLine=True)

playlists = df.select(explode("playlists").alias("playlist"))
pid_track_uri = playlists.select("playlist.pid",explode("playlist.tracks.track_uri").alias("track_uri"))
nameList = playlists.select("playlist.pid","playlist.name")



stemmer = SnowballStemmer('english')

def clean_text(c):
  return c
  c = lower(c)
  c = regexp_replace(c, "^rt ", "")
  c = regexp_replace(c, "(https?\://)\S+", "")
  c = regexp_replace(c, "[^a-zA-Z0-9\\s]", "")
  #c = split(c, "\\s+") tokenization...
  return c

def stem(in_vec):
    out_vec = []
    for t in in_vec:
        t_stem = stemmer.stem(t)
        if len(t_stem) > 2:
            out_vec.append(t_stem)       
    return out_vec

stemmer_udf = udf(lambda x: stem(x), ArrayType(StringType()))

def countIntersection(avec, bvec):
	count = 0
	for x in avec:
		if x in bvec:
			count+=1
	return count

countIntersection_udf = udf(lambda x,y: countIntersection(x,y), IntegerType())

combined_list = nameList.select("pid", concat("name").alias("text"))

cleaned_list = combined_list.select("pid",clean_text(col("text")).alias("text"))
tokenizer = Tokenizer(inputCol="text", outputCol="vector")
vec_text = tokenizer.transform(cleaned_list).select("pid", "vector")
stemmed_text = vec_text.withColumn("text_stemmed", stemmer_udf("vector")).select("pid", "text_stemmed")



callengeDF = spark.read.json(hdfs_challengeDir, multiLine=True)
challengePlaylist = callengeDF.select(explode("playlists").alias("playlists"))
challengeNameList = challengePlaylist.select("playlists.pid","playlists.name")

challenge_combined_list = challengeNameList.select("pid", concat("name").alias("text"))
challenge_cleaned_list = challenge_combined_list.select("pid",clean_text(col("text")).alias("text")).filter("text is not null")
challenge_vec_text = tokenizer.transform(challenge_cleaned_list).select("pid", "vector")
challenge_stemmed_text = challenge_vec_text.withColumn("text_stemmed", stemmer_udf("vector")).select(col("pid").alias("challengePid"),col("text_stemmed").alias("challenge_text_stemmed"))




combined = stemmed_text.crossJoin(broadcast(challenge_stemmed_text))

result = combined.withColumn("weight",countIntersection_udf("text_stemmed","challenge_text_stemmed"))

challenge_playlist_count = result.groupBy("challengePid","pid").sum("weight")
challenge_playlist_count = challenge_playlist_count.select("challengePid","pid",col("sum(weight)").alias("count"))

challenge_playlist_count_filtered = challenge_playlist_count.filter("count > 0")
challenge_track_with_count = challenge_playlist_count_filtered.join(pid_track_uri, on="pid")

final_tracks_sorted = challenge_track_with_count.groupBy("challengePid","track_uri").sum("count").sort(col("challengePid"),col("sum(count)").desc())

final_tracks_sorted.select(col("challengePid").alias("pid"),"track_uri", col("sum(count)").alias("weight")).write.mode('overwrite').parquet(hdfs_namePath)



