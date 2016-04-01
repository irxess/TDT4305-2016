import org.apache.spark.rdd.RDD

/**
  * Created by CVi on 29.03.2016.
  */
@SerialVersionUID(100L)
class CheckIn(line: String, cities: Array[City]) extends java.io.Serializable {
  val data = line.split("\t")
  //checkin_id	user_id	session_id	utc_time	timezone_offset	lat	lon	category	subcategory
  def id = data(0)
  def uid = data(1).toInt
  def sid = data(2)
  def time = data(3)
  def offset = data(4)
  def lat = data(5).toFloat
  def lon = data(6).toFloat
  def cat = data(7)
  def subCat = data(8)
  def city = closestCity(cities, lon, lat)

  override def toString(): String = "(" + id + ", " + sid + ", " + city + ")"

  //Blatantly cooked from https://rosettacode.org/wiki/Haversine_formula#Scala
  def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
    val R = 6372.8
    val dLat=(lat2 - lat1).toRadians
    val dLon=(lon2 - lon1).toRadians

    val a = Math.pow(Math.sin(dLat/2),2) + Math.pow(Math.sin(dLon/2),2) * Math.cos(lat1.toRadians) * Math.cos(lat2.toRadians)
    val c = 2 * Math.asin(Math.sqrt(a))
    R * c
  }

  def distance_between(lat_1: Float, lon_1: Float, lat_2: Float, lon_2: Float): Float ={
    haversine(lat_1, lon_1, lat_2, lon_2).toFloat
  }

  def closestCity(cities: Array[City], lon: Float, lat: Float): City ={
    var current = new City("N/A\t0.0\t0.0\t\t")
    var distance = Float.PositiveInfinity
    for(i <- cities.indices){
      if(distance_between(cities(i).lat, cities(i).lon, lat, lon) < distance){
        distance = distance_between(cities(i).lat, cities(i).lon, lat, lon)
        current = cities(i)
      }
    }
    current
  }
}
