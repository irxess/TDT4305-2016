import java.util

/**
  * Created by CVi on 01.04.2016.
  */
class Session(check_ins: util.ArrayList[AbrevCheckIn]) {
    def ci = check_ins
    def ci_count = ci.size()
    def length = 0.0

    def calculate_length(): Double ={
        0.0
    }
}
