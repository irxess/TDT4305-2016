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
    /**
      *
      */
    val file = args(0)
    val cities_file = args(1)
    val out_file = args(2)
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    //Build a KDTree, load the file using spark load and just collect it.
    // Note that it uses a self-building class as datatype.
    val cities = buildKDTree(sc.textFile(cities_file, 2).map(line => new City(line)).collect())

    def closestCity(lon: Double, lat: Double): City = {
      /**
        * This one used to be a lot longer, but it was
        * moved to the KDTree once the KDTree was adopted
        *
        * It is kept here to be a placeholder.
        */
      cities.findNearestNode(lon, lat)
    }
    //Read file
    val fsData = sc.textFile(file, 2)
    val allOfIt = fsData
    val header = allOfIt.first

    //Extract the minimal data required to complete task
    // This is so that we can save on memory in cache, and speed up the all-over process.
    val dataSet = allOfIt
      //Get rid of header
      .filter(_ != header)
      //Then extract
      .map(line => {
      val data = line.split("\t")

      (
        data(1).toInt, //uID _1
        data(2), //sID _2
        getLocalTime(data(3), data(4).toInt), //Time _3; notice the conversion function
        data(5).toDouble, //lat _4
        data(6).toDouble, //lon _5
        data(7) //cat _6
        )

    }).persist(StorageLevel.MEMORY_AND_DISK)

    //Get the number of users (distinct user IDs)
    val usercount = dataSet.map(ci => ci._1).distinct.count()
    //Get the number of check-ins
    val totalCi = dataSet.count()
    //Get the number of sessions (distinct session IDs)
    val sessionCount = dataSet.map(ci => ci._2).distinct.count()

    //Map all check-ins to a city and store only city and country.
    // For memory and speed, make them distinct.
    // Cache this, because we are going to use it several times.
    val citiesSet = dataSet.map(ci => {
      val cit = closestCity(ci._5, ci._4)
      (cit.country_code, cit.name)
    }).distinct.cache()

    //Now count the distinct cities
    val cityCount = citiesSet.count()
    //Extract the country value, and count distinct countries
    val countryCount = citiesSet.map(ci => ci._1).distinct.count()
    //Mark the city-values clear to remove from cache.
    citiesSet.unpersist()

    //Aggregate check-ins to sessions.
    // Firstly key-value map, key is session-id value is an array with the current check-in tuple.
    // Then do an aggregate-by-key that just concatenates the two array-sections.
    // Throw away the key.
    val sessions = dataSet.map(x => (x._2, Array[(Double, Double, LocalDateTime, String, String)]((x._4, x._5, x._3, x._6, x._2))))
      .aggregateByKey(Array[(Double, Double, LocalDateTime, String, String)]())((k, v) => v ++ k, (v, k) => k ++ v)
      .values.cache()

    //Save the histogram for session length (in check-ins)
    sessions.map(x => (x.length, 1)).reduceByKey((n1, n2) => n1 + n2).map(x => x._1 + "\t" + x._2)
      .saveAsTextFile(out_file + "_histogram")

    //Add a length-in-km field
    // Remember that A->B->C path has different length than
    // B->A->C or A->C->B but it does have the same length as C->B->A
    val sessions2 = sessions.filter(sess => sess.length > 4).map(ci => {
      //I don't care about reverse order. A session is just as long backwards, but the order needs to be correct.
      val ci2 = ci.sortWith((x, y) => x._3.compareTo(y._3) > 0)
      var length = 0.0
      for (i <- 1 until ci2.length) {
        val a = ci2(i)
        val b = ci2(i - 1)
        length = length + distance_between(a._1, a._2, b._1, b._2)
      }
      (length, ci2)
    })


    val sess_strings = sessions2.mapValues(x => x.map(y => {
      y._1 + "\t" + y._2 + "\t" + y._3 + "\t" + y._4 + "\t" + y._5
    }))
      .filter(x => x._1 > 50.0)
      .takeOrdered(100)(Ordering[Double].on(x => -x._1))
      .map(x => x._2.mkString("\n"))
    //De don't need the dataset anymore, so clear it for removal.
    dataSet.unpersist()



    //Helper to write this to a file.
    /*val writer = new PrintWriter(new File(out_file + "3.tsv"))
    writer.write(sess_strings.mkString("\n"))
    writer.close()*/

    /*
    SELECT sessionid,
    ST_MakeLine(
        ARRAY(SELECT the_geom FROM table_50_km AS k5 WHERE o3.sessionid = k5.sessionid ORDER BY ci_at ASC)) AS the_geom_webmercator
    FROM table_50_km AS o3 GROUP BY sessionid
    */

    sc.stop()

    print("Users: ")
    println(usercount)
    print("Total: ")
    println(totalCi)
    print("Sessions: ")
    println(sessionCount)

    print("Cities: ")
    println(cityCount)
    print("Countries: ")
    println(countryCount)

    println("Skipped writing sessions file, because it can't run on the cluster like that.")



  }

  def getLocalTime(utcString: String, localOffset: Int): LocalDateTime = {
    /**
      * Convert to localtime
      *
      * Takes a time-string and a minute-offset.
      */
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val utcTime = LocalDateTime.parse(utcString, dateFormat)
    utcTime.plusMinutes(localOffset)
  }

  def distance_between(lat_1: Double, lon_1: Double, lat_2: Double, lon_2: Double): Double = {
    /**
      * Distance formula.
      *
      * Wrappper, so that different formulas can be used.
      */
    haversine(lat_1, lon_1, lat_2, lon_2)
  }

  def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double) = {
    /**
      * Blatantly cooked from https://rosettacode.org/wiki/Haversine_formula#Scala
      */
    val R = 6372.8
    val dLat = (lat2 - lat1).toRadians
    val dLon = (lon2 - lon1).toRadians

    val a = Math.pow(Math.sin(dLat / 2), 2) + Math.pow(Math.sin(dLon / 2), 2) * Math.cos(lat1.toRadians) * Math.cos(lat2.toRadians)
    val c = 2 * Math.asin(Math.sqrt(a))
    R * c
  }

  def buildKDTree(cities: Array[City]): KDTree = {
    /**
      * Start a lat-lon split cycle.
      */
    lonTree(cities)
  }

  def lonTree(cities: Array[City]): KDTree = {
    /**
      * Does a lon split on the dataset and sends the parts for a lat-split
      *
      * Returns a LonTree
      */
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
    /**
      * Does a lat split on the dataset and sends the parts for a lon-split
      *
      * Returns a LatTree
      */
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
