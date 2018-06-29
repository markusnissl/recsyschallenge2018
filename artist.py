from pyspark.sql.functions import explode, col, lit, rand, asc, regexp_replace, lower, array, udf, collect_set, rank, collect_list
import pyspark.sql.functions as F
from pyspark.sql.window import Window

hdfs_mpdDir = "hdfs:///recsys_spotify_2018/mpd.v1/mpd.slice.*.json"
hdfs_challengeDir = "hdfs:///recsys_spotify_2018/challenge.v1/*.json"
hdfs_userDir = "hdfs:///user/XXX"
hdfs_artistWeights = hdfs_userDir + "/weightedArtist"

callengeDF = spark.read.json(hdfs_challengeDir, multiLine=True)
challengePlaylist = callengeDF.select(explode("playlists").alias("playlists"))

challenge_pid_artist_uri = challengePlaylist.select(col("playlists.pid").alias("challengePid"),"playlists.num_samples",explode("playlists.tracks").alias("tracks")).select("challengePid","num_samples","tracks.artist_uri","tracks.track_uri")
loaded_trackFactor = spark.read.parquet(hdfs_trackFactorPath)
challenge_pid_artist_uri_factor = challenge_pid_artist_uri.join(loaded_trackFactor,on=['track_uri'])

challenge_pid_artist_uri_grouped = challenge_pid_artist_uri_factor.groupBy("challengePid","num_samples","artist_uri").agg(F.sum("factor"),count("artist_uri").alias("count"))

challenge_pid_artist_uri_filterd = challenge_pid_artist_uri_grouped.filter("count > 1").withColumn("percent",col("count")/col("num_samples"))

df = spark.read.json(hdfs_mpdDir, multiLine=True)
playlists = df.select(explode("playlists").alias("playlist"))
artist_uri_track_uri = playlists.select(explode("playlist.tracks").alias("tracks")).select("tracks.artist_uri","tracks.track_uri").distinct()


data = challenge_pid_artist_uri_filterd.join(artist_uri_track_uri, on=["artist_uri"])

finalData = data.groupBy("challengePid","track_uri").sum("sum(factor)").select(col("challengePid").alias("pid"),"track_uri",col("sum(sum(factor))").alias("weight"))

finalData.write.mode('overwrite').parquet(hdfs_artistWeights)
