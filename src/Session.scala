import java.util
import java.util.Collections

/**
  * Created by CVi on 01.04.2016.
  */
class Session(check_ins: util.ArrayList[AbrevCheckIn]) {
    def ci = check_ins
    def ci_count = ci.size()
    def length = 0.0

    def calculate_length(): Double ={
        Collections.sort(ci)
        var prev = None
        var cur = None

        1.0
    }
}
