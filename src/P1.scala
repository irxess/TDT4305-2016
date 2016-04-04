import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util
import java.util.Locale

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by CVi on 14.03.2016.
  */
object P1 {
  def main(args: Array[String]) {
    val file = "src/Foursquare_data/dataset_TIST2015.tsv"
    val cities_file = "src/Foursquare_data/dataset_TIST2015_Cities.txt"

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val fsData = sc.textFile(file, 2)
    val allOfIt = fsData
    val header = allOfIt.first
    val cities = sc.textFile(cities_file, 2).map(line => new City(line)).collect()

    val data = allOfIt.filter(_ != header).map(line => new CheckIn(line, cities)).cache()
    val dataArray = data.map(ci => (ci.sid, {
      val al = new util.ArrayList[AbrevCheckIn]()
      al.add(new AbrevCheckIn(ci))
      al}
      )
    ).reduceByKey((v1, v2) => {v1.addAll(v2); v1})
       .values.filter(al => al.size() >= 4)

    val localTimes = data.map( checkin => getLocalTime( checkin.time, checkin.offset ))
    println( localTimes.take(5).deep.mkString("\n"))

//    println(data.map(ci => ci.uid).distinct.count())
//    println(data.count())
//    println(data.map(ci => ci.sid).distinct.count())
//    println(data.map(ci => ci.city.country).distinct.count())
//    println(data.map(ci => ci.city.name + ci.city.country_code).distinct.count())
    //println(dataArray.first)


    sc.stop()
  }

  def getLocalTime( utcString: String, localOffset: Int): LocalDateTime = {
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val utcTime = LocalDateTime.parse( utcString, dateFormat )
    utcTime.plusMinutes( localOffset )
  }
}
