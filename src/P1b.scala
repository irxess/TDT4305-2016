import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by CVi on 14.03.2016.
  */
object P1b {
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
    val emptyCity = new City("N/A\t0.0\t0.0\tNA\tNA")

    def closestCity(lon: Double, lat: Double): City = {
      var current = emptyCity
      var distance = Double.PositiveInfinity
      for (i <- cities.indices) {
        if (distance_between(cities(i).lat, cities(i).lon, lat, lon) < distance) {
          distance = distance_between(cities(i).lat, cities(i).lon, lat, lon)
          current = cities(i)
        }
      }
      current
    }

    val dataSet = allOfIt
      //.sample(false, 0.1, 100)
      .filter(_ != header).map(line => {
      val data = line.split("\t")

      (data(1).toInt, //uID _1
        data(2), //sID _2
        getLocalTime(data(3), data(4).toInt), //Time _3
        data(5).toDouble, //lat _4
        data(6).toDouble, //lon _5
        data(7) //cat _6
        )

    }).persist(StorageLevel.MEMORY_AND_DISK)

    print("Users: ")
    println(dataSet.map(ci => ci._1).distinct.count())
    print("Total: ")
    println(dataSet.count())
    print("Sessions: ")
    println(dataSet.map(ci => ci._2).distinct.count())

    /*val citiesSet = dataSet.map(ci => {
      val cit = closestCity(ci._4, ci._5)
      (cit.country_code, cit.name)
    }).distinct.cache()

    print("Cities: ")
    println(citiesSet.count())
    print("Countries: ")
    println(citiesSet.map(ci => ci._1).distinct.count())
    citiesSet.unpersist()*/
    dataSet.unpersist()

    val sessions = dataSet.map(x => (x._2, Array[(Double, Double, LocalDateTime, String, String)]((x._4, x._5, x._3, x._6, x._2))))
      .aggregateByKey(Array[(Double, Double, LocalDateTime, String, String)]())((k, v) => v ++ k, (v, k) => k ++ v)
      //.reduceByKey((a,b) => a++b)
      .filter(sess => sess._2.length > 4).values
    val sessions2 = sessions.map(ci => {
      //I don't care about reverse order. A session is just as long backwards.
      ci.sortWith((x, y) => x._3.compareTo(y._3) > 0)
      var length = 0.0
      for (i <- 1 until ci.length) {
        val a = ci(i)
        val b = ci(i - 1)
        length = length + distance_between(a._1, a._2, b._1, b._2)
      }
      (length, ci)
    })

    val sess_strings = sessions2.mapValues(x => x.map(y => {
      y._1 + "\t" + y._2 + "\t" + y._3 + "\t" + y._4 + "\t" + y._5
    }))
      .filter(x => x._1 > 50.0)
      .takeOrdered(100)(Ordering[Double].on(x => -x._1))
      .map(x => x._2.mkString("\n")) //.mkString("\n")

    val writer = new PrintWriter(new File(out_file + "3.tsv"))
    writer.write(sess_strings.mkString("\n"))
    writer.close()

    /*
  SELECT sessionid,
	ST_MakeLine(
      ARRAY(SELECT the_geom FROM table_50_km AS k5 WHERE o3.sessionid = k5.sessionid ORDER BY ci_at ASC)) AS the_geom_webmercator
  FROM table_50_km AS o3 GROUP BY sessionid
  */

    sc.stop()
  }

  def getLocalTime(utcString: String, localOffset: Int): LocalDateTime = {
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val utcTime = LocalDateTime.parse(utcString, dateFormat)
    utcTime.plusMinutes(localOffset)
  }

  def distance_between(lat_1: Double, lon_1: Double, lat_2: Double, lon_2: Double): Double = {
    haversine(lat_1, lon_1, lat_2, lon_2)
  }

  //Blatantly cooked from https://rosettacode.org/wiki/Haversine_formula#Scala
  def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double) = {
    val R = 6372.8
    val dLat = (lat2 - lat1).toRadians
    val dLon = (lon2 - lon1).toRadians

    val a = Math.pow(Math.sin(dLat / 2), 2) + Math.pow(Math.sin(dLon / 2), 2) * Math.cos(lat1.toRadians) * Math.cos(lat2.toRadians)
    val c = 2 * Math.asin(Math.sqrt(a))
    R * c
  }
}
