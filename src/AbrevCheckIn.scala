/**
  * Created by CVi on 01.04.2016.
  */
class AbrevCheckIn(ci: CheckIn) extends java.io.Serializable with Ordered[AbrevCheckIn] {
  val time = ci.time
  val lat = ci.lat
  val lon = ci.lon
  val sid = ci.sid

  def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
    val R = 6372.8
    val dLat=(lat2 - lat1).toRadians
    val dLon=(lon2 - lon1).toRadians

    val a = Math.pow(Math.sin(dLat/2),2) + Math.pow(Math.sin(dLon/2),2) * Math.cos(lat1.toRadians) * Math.cos(lat2.toRadians)
    val c = 2 * Math.asin(Math.sqrt(a))
    R * c
  }

  override def toString(): String = time + "\t" + lat + "\t" + lon + "\t" + sid

  def distance_between(lat_1: Double, lon_1: Double, lat_2: Double, lon_2: Double): Double ={
    haversine(lat_1, lon_1, lat_2, lon_2).toFloat
  }

  override def compare(o: AbrevCheckIn): Int = {
    time.compareTo(o.time)
  }
}
