val rawUserArtistData = spark.read.textFile("/home/ahmed/Documents/github/spark-projects/profiledata_06-May-2005/user_artist_data.txt")
rawUserArtistData.take(5).foreach(println)
