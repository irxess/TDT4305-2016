/**
  * Created by CVi on 29.03.2016.
  */
class City(line: String) extends java.io.Serializable {
  private val data = line.split("\t")
  val name = data(0)
  val lat = data(1).toDouble
  val lon = data(2).toDouble
  val country_code = data(3)
  val country = data(4)

  override def toString(): String = name + ", " + country_code

}
