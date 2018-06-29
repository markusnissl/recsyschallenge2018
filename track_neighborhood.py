from pyspark.sql.functions import explode, col, lit, rand, asc, regexp_replace, lower, array, udf, collect_set, rank, collect_list
from pyspark.sql.window import Window

hdfs_mpdDir = "hdfs:///recsys_spotify_2018/mpd.v1/mpd.slice.*.json"
hdfs_challengeDir = "hdfs:///recsys_spotify_2018/challenge.v1/*.json"
hdfs_userDir = "hdfs:///user/XXX"
hdfs_trackPath = hdfs_userDir + "/weightedTracks"
hdfs_trackFactorPath = hdfs_userDir + "/factorTracks"

df = spark.read.json(hdfs_mpdDir, multiLine=True)
callengeDF = spark.read.json(hdfs_challengeDir, multiLine=True)

playlists = df.select(explode("playlists").alias("playlist"))
challengePlaylist = callengeDF.select(explode("playlists").alias("playlists"))


loaded_trackFactor = spark.read.parquet(hdfs_trackFactorPath)

pid_track_uri = playlists.select("playlist.pid",explode("playlist.tracks.track_uri").alias("track_uri"))
pid_track_uri2 = playlists.select("playlist.pid",explode("playlist.tracks.track_uri").alias("track_uri")).join(loaded_trackFactor,on=['track_uri'])


challenge_pid_track_uri = challengePlaylist.select(col("playlists.pid").alias("challengePid"),explode("playlists.tracks.track_uri").alias("track_uri"))

joined = challenge_pid_track_uri.join(pid_track_uri2, on="track_uri")
challenge_playlist_count = joined.groupBy("challengePid","pid").sum('factor').select("challengePid","pid",col("sum(factor)").alias("count"))
challenge_track_with_count = challenge_playlist_count.join(pid_track_uri, on="pid")
final_tracks_sorted = challenge_track_with_count.groupBy("challengePid","track_uri").sum("count")

final_tracks_sorted.select(col("challengePid").alias("pid"),"track_uri", col("sum(count)").alias("weight")).write.mode('overwrite').parquet(hdfs_trackPath)
