from pyspark.sql.functions import explode, col, lit, rand, asc, regexp_replace, lower, array, udf, collect_set, rank, collect_list
from pyspark.sql.window import Window

hdfs_mpdDir = "hdfs:///recsys_spotify_2018/mpd.v1/mpd.slice.*.json"
hdfs_challengeDir = "hdfs:///recsys_spotify_2018/challenge.v1/*.json"
hdfs_userDir = "hdfs:///user/XXX"
hdfs_top500 = hdfs_userDir + "/top500"

df = spark.read.json(hdfs_mpdDir, multiLine=True)
callengeDF = spark.read.json(hdfs_challengeDir, multiLine=True)

playlists = df.select(explode("playlists").alias("playlist"))
challengePlaylist = callengeDF.select(explode("playlists").alias("playlists"))

pid_track_uri = playlists.select("playlist.pid",explode("playlist.tracks.track_uri").alias("track_uri"))
topTracks = pid_track_uri.groupBy("track_uri").count().sort(col("count").desc()).limit(750)

challengePids = challengePlaylist.select(col("playlists.pid"))
top750 = challengePids.crossJoin(topTracks)

challenge_pid_track_uri = challengePlaylist.select(col("playlists.pid").alias("challengePid"),explode("playlists.tracks.track_uri").alias("track_uri"))
reduced = top750.join(challenge_pid_track_uri.select(col("challengePid").alias("pid"),"track_uri"), on=["pid", "track_uri"], how="left_anti")
reduced = reduced.sort(col("pid"),col("count").desc())

window = Window.partitionBy(reduced["pid"]).orderBy(reduced["count"].desc())
sortedList = reduced.withColumn("weight", rank().over(window)).where("weight <= 500").sort(col("pid"),col("count").desc())
sortedList = sortedList.select("pid","track_uri",(501-col("weight")).alias("weight"))

sortedList.write.mode('overwrite').parquet(hdfs_top500)
