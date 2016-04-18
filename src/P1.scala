import java.io._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

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
    val cities = sc.textFile(cities_file, 2).map(line => new City(line)).collect()

    // Construct a tree
    val tree = buildKDTree(cities)
    val data = allOfIt.sample(false, 0.15, 100).filter(_ != header).map(line => new CheckIn(line, tree)).persist(StorageLevel.MEMORY_AND_DISK)

    val dataArray = data.map(ci => (ci.sid, {
      val al = Array(new AbrevCheckIn(ci))
      al
    }
      )
    ).reduceByKey((v1, v2) => {v1 ++ v2})
      .values.map(al => new Session(al)).persist(StorageLevel.MEMORY_AND_DISK)

    println(data.map(ci => ci.country_code).distinct.count())
    println(data.map(ci => ci.city_name + ci.country_code).distinct.count())
    data.unpersist()
    /*
    SELECT
    sess,
    ST_MakeLine(ARRAY(SELECT the_geom FROM out3 WHERE o3.sess = out3.sess ORDER BY DATE ASC)) AS the_geom_webmercator
    FROM out3 AS o3 GROUP BY sess
    */

    dataArray.map(sess => (sess.ci_count, 1)).reduceByKey((v1, v2) => v1 + v2).saveAsTextFile(out_file)
    val fourPlus = dataArray.filter(sess => sess.ci_count >= 4).filter(s => {
      s.calculate_length(); s.length
    } > 50.0)
      .takeOrdered(100)(Ordering[Double].on(x => x.length))

    val writer = new PrintWriter(new File(out_file + "3.txt"))

    writer.write(fourPlus.mkString("\n"))
    writer.close()

    sc.stop()
  }

  def buildKDTree(cities: Array[City]): KDTree = {
    lonTree(cities)
  }

  def lonTree(cities: Array[City]): KDTree = {
    if (cities.length == 1) {
      new LonTree(cities(0), Option(null), Option(null))
    } else if (cities.length == 2) {
      val c = cities.sortBy(x => -x.lon)
      new LonTree(c(0), Option(latTree(c.slice(1, 2))), Option(null))
    } else {
      val c = cities.sortBy(x => x.lon)
      val mid = c.length / 2
      new LonTree(c(mid),
        Option(latTree(c.slice(0, mid))),
        Option(latTree(c.slice(mid + 1, c.length)))
      )
    }
  }

  def latTree(cities: Array[City]): KDTree = {
    if (cities.length == 1) {
      new LatTree(cities(0), Option(null), Option(null))
    } else if (cities.length == 2) {
      val c = cities.sortBy(x => -x.lat)
      new LatTree(c(0), Option(lonTree(c.slice(1, 2))), Option(null))
    } else {
      val c = cities.sortBy(x => x.lat)
      val mid = cities.length / 2
      new LatTree(c(mid),
        Option(lonTree(c.slice(0, mid))),
        Option(lonTree(c.slice(mid + 1, c.length)))
      )
    }
  }
}

