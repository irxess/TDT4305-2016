import java.io._
import java.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by CVi on 14.03.2016.
  */
object P1 {
  def main(args: Array[String]) {
    val file = System.getenv("DS_FILE")
    val cities_file = System.getenv("DS_FILE_CITIES")
    val out_file = System.getenv("OUT_FILE")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val fsData = sc.textFile(file, 2)
    val allOfIt = fsData
    val header = allOfIt.first
    var cities = sc.textFile(cities_file, 2).map(line => new City(line)).collect()

    val data = allOfIt/*.sample(false, 0.05, 100)*/.filter(_ != header).map(line => new CheckIn(line, cities)).cache()//.persist(StorageLevel.MEMORY_AND_DISK)
    //cities = new Array[City](0)

    val dataArray = data.map(ci => (ci.sid, {
      val al = Array(new AbrevCheckIn(ci))
      al
    }
      )
    ).reduceByKey((v1, v2) => {v1 ++ v2})
       .values.map(al => new Session(al)).cache()//.persist(StorageLevel.MEMORY_AND_DISK)

    //println(data.map(ci => ci.uid).distinct.count())
    //println(data.count())
    //println(data.map(ci => ci.sid).distinct.count())
    //println(data.map(ci => ci.country_code).distinct.count())
    //println(data.map(ci => ci.city_name + ci.country_code).distinct.count())
    /*SELECT
	sess,
	ST_MakeLine(ARRAY(SELECT the_geom FROM out3 WHERE o3.sess = out3.sess)) AS the_geom_webmercator
FROM out3 AS o3 GROUP BY sess
  */

    //dataArray.map(sess => (sess.ci_count, 1)).reduceByKey((v1,v2) => v1+v2).saveAsTextFile(out_file)
    val fourPlus = dataArray.filter(sess => sess.ci_count >= 4).map(s => {s.calculate_length(); s})
      .filter(s => s.length > 50.0).takeOrdered(100)(Ordering[Double].on(x => x.length))

   val writer = new PrintWriter(new File(out_file+"3.txt" ))

    writer.write(fourPlus.mkString("\n"))
    writer.close()


    //println(dataArray.first)


    sc.stop()
  }
}
