/**
  * Created by CVi on 29.03.2016.
  */
class City(line: String) extends java.io.Serializable {
  val data = line.split("\t")
  def name = data(0)
  def lat = data(1).toFloat
  def lon = data(2).toFloat
  def country_code = data(3)
  def country = data(4)

  override def toString(): String = name + ", " + country_code

}
