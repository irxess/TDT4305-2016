import java.util
import java.util.Collections

/**
  * Created by CVi on 01.04.2016.
  */
class Session(check_ins: util.ArrayList[AbrevCheckIn]) {
    val ci = check_ins
    val ci_count = ci.size()
    var length = 0.0

    def calculate_length(): Double ={
      Collections.sort(ci)
      for(i <- 1 until ci.size() ){
        val a = ci.get(i)
        val b = ci.get(i-1)
        length = length + a.distance_between(a.lat, a.lon, b.lat, b.lon)
      }
      print(length)
      length
    }
}
