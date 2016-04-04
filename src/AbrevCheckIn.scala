/**
  * Created by CVi on 01.04.2016.
  */
class AbrevCheckIn(ci: CheckIn) extends java.io.Serializable with Comparable[AbrevCheckIn] {
  def time = ci.time
  def lat = ci.time
  def lon = ci.time
  def sid = ci.sid

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

  override def compareTo(o: AbrevCheckIn): Int = {
    time.compareTo(o.time)
  }
}
