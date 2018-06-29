from pyspark.sql.functions import explode, col, lit, rand, asc, regexp_replace, lower, array, udf, collect_set, rank, collect_list
from pyspark.sql.window import Window

hdfs_userDir = "hdfs:///user/XXX"
userDir = "/home/XXX"

hdfs_challengeDir = "hdfs:///recsys_spotify_2018/challenge.v1/*.json"

hdfs_trackPath = hdfs_userDir + "/weightedTracks"
hdfs_namePath = hdfs_userDir + "/weightedNames"
hdfs_top500Path = hdfs_userDir + "/top500"

#hdfs_artistWeights = hdfs_userDir + "/weightedArtist"
#hdfs_albumWeights = hdfs_userDir + "/weightedAlbum"

loaded_neighborhoodTrackUri_tracks = spark.read.parquet(hdfs_trackPath)
loaded_name_tracks = spark.read.parquet(hdfs_namePath).select("pid","track_uri",(col("weight")/4).alias("weight"))
loaded_top500_tracks = spark.read.parquet(hdfs_top500Path).select("pid","track_uri",(col("weight")/500).alias("weight")) # Check maybe want to reduce weight of this ones :) 
#loaded_album_tracks = spark.read.parquet(hdfs_albumWeights)
#loaded_artist_tracks = spark.read.parquet(hdfs_artistWeights)

loaded_final_tracks = loaded_neighborhoodTrackUri_tracks.union(loaded_name_tracks).union(loaded_top500_tracks).groupBy("pid","track_uri").sum("weight").select("pid", "track_uri", col("sum(weight)").alias("weight"))

#loaded_final_tracks = loaded_neighborhoodTrackUri_tracks.union(loaded_name_tracks).union(loaded_top500_tracks).union(loaded_album_tracks).union(loaded_artist_tracks).groupBy("pid","track_uri").sum("weight").select("pid", "track_uri", col("sum(weight)").alias("weight"))

callengeDF = spark.read.json(hdfs_challengeDir, multiLine=True)
challengePlaylist = callengeDF.select(explode("playlists").alias("playlists"))
challenge_pid_track_uri = challengePlaylist.select(col("playlists.pid").alias("challengePid"),explode("playlists.tracks.track_uri").alias("track_uri"))

reduced = loaded_final_tracks.join(challenge_pid_track_uri.select(col("challengePid").alias("pid"),"track_uri"), on=["pid", "track_uri"], how="left_anti")

window = Window.partitionBy(reduced["pid"]).orderBy(reduced["weight"].desc())
sortedList = reduced.sort(col("pid"),col("weight").desc()).withColumn("rank", rank().over(window)).where("rank <= 500")

playlistSolution = sortedList.groupBy("pid")\
        .agg(collect_list(col("track_uri")).alias("track_uri"))\
        .collect()
        
with open(challenge_submission_file, "w") as f:
    f.write("team_info,main,TUW-Alpha,mail@example.com")
    for playlist in playlistSolution:
        f.write("\n")
        f.write(str(playlist.pid)+", " + ", ".join(playlist.track_uri[0:500]))
