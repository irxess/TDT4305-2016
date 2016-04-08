import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.spark.rdd.RDD

/**
  * Created by CVi on 29.03.2016.
  */
@SerialVersionUID(100L)
class CheckIn(line: String) extends java.io.Serializable {
  private val data = line.split("\t")
  val id = data(0)
  val uid = data(1).toInt
  val sid = data(2)
  val time = getLocalTime(data(3), data(4).toInt)
  val lat = data(5).toDouble
  val lon = data(6).toDouble
  //def cat = data(7)
  //def subCat = data(8)
//  private val city = closeCity
  var city_name:String = _
  //def country = city.country
  var country_code:String = _

  def this(line: String, cities: Array[City]) = {
    this(line)
    val city = closestCity(cities,lon,lat)
    this.city_name = city.name
    //def country = city.country
    this.country_code = city.country_code
  }

  def this(line: String, tree: KDTree) = {
    this(line)
    val c = tree.findNearestNode(this.lat, this.lon)
    this.city_name = c.name
    this.country_code = c.country_code
  }

  override def toString(): String = "(" + id + ", " + sid + ", " + city_name + ")"

  //Blatantly cooked from https://rosettacode.org/wiki/Haversine_formula#Scala
  def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
    val R = 6372.8
    val dLat=(lat2 - lat1).toRadians
    val dLon=(lon2 - lon1).toRadians

    val a = Math.pow(Math.sin(dLat/2),2) + Math.pow(Math.sin(dLon/2),2) * Math.cos(lat1.toRadians) * Math.cos(lat2.toRadians)
    val c = 2 * Math.asin(Math.sqrt(a))
    R * c
  }

  def getLocalTime( utcString: String, localOffset: Int): LocalDateTime = {
    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val utcTime = LocalDateTime.parse( utcString, dateFormat )
    utcTime.plusMinutes( localOffset )
  }

  def distance_between(lat_1: Double, lon_1: Double, lat_2: Double, lon_2: Double): Double ={
    haversine(lat_1, lon_1, lat_2, lon_2)
  }

  def closestCity(cities: Array[City], lon: Double, lat: Double): City ={
    var current = new City("N/A\t0.0\t0.0\tNA\tNA")
    var distance = Double.PositiveInfinity
    for(i <- cities.indices){
      if(distance_between(cities(i).lat, cities(i).lon, lat, lon) < distance){
        distance = distance_between(cities(i).lat, cities(i).lon, lat, lon)
        current = cities(i)
      }
    }
    current
  }
}
