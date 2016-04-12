/**
  * Created by ilsegv on 05/04/16.
  */


abstract class KDTree(c: City, left: Option[KDTree], right: Option[KDTree]) extends Serializable {
    val leftChild = left
    val rightChild = right
    val city = c

    def lat = {city.lat}
    def lon = {city.lon}

    def distance(n: SearchNode): Double = {
        haversine( lat, lon, n.lat, n.lon)
    }

    abstract def shortestDistance(n: SearchNode): Double

    def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
        val R = 6372.8
        val dLat=(lat2 - lat1).toRadians
        val dLon=(lon2 - lon1).toRadians

        val a = Math.pow(Math.sin(dLat/2),2) + Math.pow(Math.sin(dLon/2),2) * Math.cos(lat1.toRadians) * Math.cos(lat2.toRadians)
        val c = 2 * Math.asin(Math.sqrt(a))
        R * c
    }

    abstract def pointIsToTheLeft(point: SearchNode): Boolean

    def nearestNeighbor(point: SearchNode): Unit = {
        // if tree is empty, return Double.max

        val newDistance = distance(point)
        if (newDistance < point.distance) point.distance = newDistance

        var mostPromisingTree: Option[KDTree] = _
        var lessPromisingTree: Option[KDTree] = _
        if (pointIsToTheLeft(point)) {
            mostPromisingTree = leftChild
            lessPromisingTree = rightChild
        } else {
            mostPromisingTree = rightChild
            lessPromisingTree = leftChild
        }

        if (mostPromisingTree.isDefined) {
            mostPromisingTree.get.nearestNeighbor(point)
        }

        if (lessPromisingTree.isDefined) {
            val minDistanceToUnknownArea = shortestDistance(point)
            if (minDistanceToUnknownArea < point.distance) {
                lessPromisingTree.get.nearestNeighbor(point)
            }
        }
    }

}


class LatTree(c: City, left: Option[KDTree], right: Option[KDTree]) extends KDTree(c, left, right) {
    override def pointIsToTheLeft(point: SearchNode): Boolean = { point.lat < lat }

    override def shortestDistance(point: SearchNode): Double = {
        haversine( lat, point.lon, point.lat, point.lon )
    }
}

class LonTree(c: City, left: Option[KDTree], right: Option[KDTree]) extends KDTree(c, left, right) {
    override def pointIsToTheLeft(point: SearchNode): Boolean = { point.lon < lon }

    override def shortestDistance(point: SearchNode): Double = {
        haversine( point.lat, lon, point.lat, point.lon)
    }
}

class SearchNode(a: Double, o: Double) {
    // stuff with the point to search for
    val lat = a
    val lon = o
    var distance = Double.MaxValue
}

