



object dp {
def args = dp_sc.args$
/*<script>*/val path = "/home/ahmed/Documents/github/spark-projects/profiledata_06-May-2005"

val rawUserArtistData = spark.read.textFile(path + "/user_artist_data.txt")
rawUserArtistData.take(5).foreach(println)

val userArtistDF = rawUserArtistData
  .map { line =>
    val Array(user, artist, _*) = line.split(' ')
    (user.toInt, artist.toInt)
  }
  .toDF("user", "artist")

userArtistDF.agg(min("user"), max("user"), min("artist"), max("artist")).show()

val rawArtistData = spark.read.textFile(path + "/artist_data.txt")
val artistByID = rawArtistData
  .flatMap { line =>
    val (id, name) = line.span(_ != '\t')
    if (name.isEmpty) {
      None
    } else {
      try {
        Some((id.toInt, name.trim))
      } catch {
        case _: NumberFormatException => None
      }
    }
  }
  .toDF("id", "name")

val rawArtistAlias = spark.read.textFile(path + "/artist_alias.txt")
val artistByID = rawArtistAlias
  .flatMap { line =>
    val Array(artist, alias) = line.split('\t')
    if (artist.isEmpty) {
      None
    } else {
      Some((artist.toInt, alias.toInt))
    }
  }
  .collect()
  .toMap
artistByID.head

artistByID.filter($"id" isin (1208690, 1003926)).show()

import org.apache.spark.sql._
import org.apache.spark.broadcast._
def buildCounts(
    rawUserArtistData: Dataset[String],
    bArtistAlias: Broadcast[Map[Int, Int]]
): DataFrame = {
  rawUserArtistData
    .map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID =
        bArtistAlias.value.getOrElse(artistID, artistID)
      (userID, finalArtistID, count)
    }
    .toDF("user", "artist", "count")
}
val bArtistAlias = spark.sparkContext.broadcast(artistByID)
val trainData = buildCounts(rawUserArtistData, bArtistAlias)
trainData.cache()

import org.apache.spark.ml.recommendation._
import scala.util.Random

val model = new ALS()
  .setSeed(Random.nextLong())
  .setImplicitPrefs(true)
  .setRank(10)
  .setRegParam(0.01)
  .setAlpha(1.0)
  .setMaxIter(5)
  .setUserCol("user")
  .setItemCol("artist")
  .setRatingCol("count")
  .setPredictionCol("prediction")
  .fit(trainData)

model.userFactors.show(1, truncate = false)
/*</script>*/ /*<generated>*/
/*</generated>*/
}

object dp_sc {
  private var args$opt0 = Option.empty[Array[String]]
  def args$set(args: Array[String]): Unit = {
    args$opt0 = Some(args)
  }
  def args$opt: Option[Array[String]] = args$opt0
  def args$: Array[String] = args$opt.getOrElse {
    sys.error("No arguments passed to this script")
  }
  def main(args: Array[String]): Unit = {
    args$set(args)
    dp.hashCode() // hasCode to clear scalac warning about pure expression in statement position
  }
}

