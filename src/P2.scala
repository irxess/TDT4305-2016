import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by CVi on 14.03.2016.
  */
object P2 {
  val daysOfWeek = Array("-1", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

  def main(args: Array[String]) {
    /** **
      * Main program,
      *
      * Filters tweets from file (arg0) on
      * COUNTRY_CODE
      * LANG
      * PLACE_TYPE
      *
      * Originally set up to use ENV-variables to hold the values,
      * this version uses hard coded strings
      */
    //val file = System.getenv("DS_FILE")
    val file = args(0)
    val minusFile = System.getenv("DS_FILE_MINUS")
    //val minusFile = "/path/to/neegative/words.txt"
    val plusFile = System.getenv("DS_FILE_PLUS")
    //val plusFile = "/path/to/positive/words.txt"
    //val outFile = System.getenv("OUT_FILE")
    val outFile = args(1)
    //val country = System.getenv("FILTER_COUNTRY")
    val country = "US"
    //val lang = System.getenv("FILTER_LANG")
    val lang = "en"
    //val placeType = System.getenv("FILTER_PLACE_TYPE")
    val placeType = "city"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    //First set up a filtered version
    val twitterData = sc.textFile(file, 2).map(x => x.split("\t"))
    //Doing it all in one map is way faster than chaining.
    //We could have implemented optional filters, and gotten much more flexible code
    //but this is built for speed
    val tweetFiltered = twitterData.filter(x => x(2).equals(country) && x(5).equals(lang) && x(3).equals(placeType))

    //Build the positive and negative words list.
    val wordsMinus = sc.textFile(minusFile, 2).map(s => s -> -1)
    val wordsPlus = sc.textFile(plusFile, 2).map(s => s -> 1)
    val words = wordsMinus.union(wordsPlus)
    val wordMap = mutable.HashMap[String, Int]()
    words.collect().foreach(x => wordMap += x)

    /*
    Calculate local time, city and scores
     */
    tweetFiltered.map(x => {
      val cal = Calendar.getInstance()
      val time = x(0).toLong + (x(8).toLong * 60)
      cal.setTimeInMillis(time * 1000)
      var score = 0
      //For each word in tweet
      x(10).split(" ").foreach(w => {
        val k = w.toLowerCase
        //If the scoring-map has the word
        // add score.
        if (wordMap contains k) {
          score += wordMap(k)
        }
      })
      ((x(4), daysOfWeek(cal.get(Calendar.DAY_OF_WEEK))), score)
    })
      //Sum up the scores.
      //Depending on data types aggregateByKey may be faster.
      //Not in this case it seemed.
      .reduceByKey((v1, v2) => v1 + v2)
      //.aggregateByKey(0)((v1, v2) => v1 + v2, (v1, v2) => v1 + v2)
      .map(x => x._1._1 + "\t" + x._1._2 + "\t" + x._2)
      .saveAsTextFile(outFile)

    /*val writer = new PrintWriter(new File(out_file+"3.txt" ))

    writer.write(fourPlus.mkString("\n"))
    writer.close()*/

    sc.stop()
  }

}
