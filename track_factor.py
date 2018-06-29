from pyspark.sql.functions import explode, col, lit, rand, asc, regexp_replace, lower, array, udf, collect_set, rank, collect_list, log
from pyspark.sql.window import Window

hdfs_mpdDir = "hdfs:///recsys_spotify_2018/mpd.v1/mpd.slice.*.json"
hdfs_challengeDir = "hdfs:///recsys_spotify_2018/challenge.v1/*.json"
hdfs_userDir = "hdfs:///user/XXX"
hdfs_trackFactorPath = hdfs_userDir + "/factorTracks"

df = spark.read.json(hdfs_mpdDir, multiLine=True)
playlists = df.select(explode("playlists").alias("playlist"))
pid_track_uri = playlists.select("playlist.pid",explode("playlist.tracks.track_uri").alias("track_uri")).groupBy('pid','track_uri').count()
trackList = pid_track_uri.groupBy("track_uri").count()
trackListFactor = trackList.withColumn("factor",1000000/col("count")).select("track_uri","factor")
trackListFactor.write.mode('overwrite').parquet(hdfs_trackFactorPath)
