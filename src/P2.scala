import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by CVi on 14.03.2016.
  */
object P2 {
  val daysOfWeek = Array("-1", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

  def main(args: Array[String]) {
    val file = System.getenv("DS_FILE")
    val minusFile = System.getenv("DS_FILE_MINUS")
    val plusFile = System.getenv("DS_FILE_PLUS")
    val outFile = System.getenv("OUT_FILE")
    val country = System.getenv("FILTER_COUNTRY")
    val lang = System.getenv("FILTER_LANG")
    val placeType = System.getenv("FILTER_PLACE_TYPE")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val twitterData = sc.textFile(file, 2).map(x => x.split("\t"))
    val tweetFiltered = twitterData.filter(x => x(2).equals(country) && x(5).equals(lang) && x(3).equals(placeType))

    val wordsMinus = sc.textFile(minusFile, 2).map(s => s -> -1)
    val wordsPlus = sc.textFile(plusFile, 2).map(s => s -> 1)
    val words = wordsMinus.union(wordsPlus)

    val wordMap = mutable.HashMap[String, Int]()
    words.collect().foreach(x => wordMap += x)

    tweetFiltered.map(x => {
      val cal = Calendar.getInstance()
      val time = x(0).toLong + (x(8).toLong * 60)
      cal.setTimeInMillis(time * 1000)
      var score = 0
      x(10).split(" ").foreach(w => {
        val k = w.toLowerCase
        if (wordMap contains k) {
          score += wordMap(k)
        }
      })
      ((x(4), daysOfWeek(cal.get(Calendar.DAY_OF_WEEK))), (score, 1))
    }).reduceByKey((v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v1._2))
      .map(x => x._1._1 + "\t" + x._1._2 + "\t" + x._2._1.toString)
      .saveAsTextFile(outFile)

    /*val writer = new PrintWriter(new File(out_file+"3.txt" ))

    writer.write(fourPlus.mkString("\n"))
    writer.close()*/

    sc.stop()
  }

}
