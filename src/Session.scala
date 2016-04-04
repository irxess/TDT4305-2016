

/**
  * Created by CVi on 01.04.2016.
  */
class Session(check_ins: Array[AbrevCheckIn]) extends Serializable {
    val ci = check_ins
    val ci_count = ci.length
    var length = 0.0

    def calculate_length(): Double ={
      if (length == 0.0) {
        ci.sortBy(x => x)
        for (i <- 1 until ci.length) {
          val a = ci(i)
          val b = ci(i - 1)
          length = length + a.distance_between(a.lat, a.lon, b.lat, b.lon)
        }
      }
      length
    }

    override def toString(): String = ci.mkString("\n")
}
