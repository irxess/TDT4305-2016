/**
  * Created by ilsegv on 05/04/16.
  */

class KDTree(cities: Array[City]) extends Serializable {

    class Node(l: Double, o: Double ) extends java.io.Serializable {
        val lat = l
        val lon = o
        var city:City = _
        var leftChild = None: Option[Node]
        var rightChild = None: Option[Node]

        def this(c: City) {
            this(c.lat, c.lon)
            this.city = c
        }

        def insert(n: Node, useLatitude: Boolean) {
            if (useLatitude) {
                if (n.lat < lat) { //latitude
                    // insert to the left
                    if (leftChild.isEmpty) {
                        leftChild = Some(n)
                    } else {
                        leftChild.get.insert( n, !useLatitude )
                    }
                } else {
                    // insert to the right
                    if (rightChild.isEmpty) {
                        rightChild = Some(n)
                    } else {
                        rightChild.get.insert( n, !useLatitude )
                    }
                }
            } else {
                if (n.lon < lon) { //longitude
                    // insert to the left
                    if (leftChild.isEmpty) {
                        leftChild = Some(n)
                    } else {
                        leftChild.get.insert( n, !useLatitude )
                    }
                } else {
                    // insert to the right
                    if (rightChild.isEmpty) {
                        rightChild = Some(n)
                    } else {
                        rightChild.get.insert( n, !useLatitude )
                    }
                }
            }
        }
        def distance(n: Node): Double = {
            haversine( lat, lon, n.lat, n.lon)
        }

        def searchNode(point: Node, useLatitude: Boolean, bestNode: Node, bestDistance: Double): (Node, Double) = {
            var currentDistance = distance(point)
            var currentNode = this
            // Here we should eliminate the current subtree, if it is not interesting
//            if (currentDistance > bestDistance) {
//                return (bestNode, bestDistance)
//            }

            // at this point, currentDistance is better than bestDistance

            // when at a leaf node, return as current best
            if (leftChild.isEmpty && rightChild.isEmpty) {
                return (this, distance(point))
            }

            var newDistance:Double = Double.MaxValue
            var result:Node = this
            var t:(Node, Double) = (result,newDistance)
            // search the most promising subtree
            if (isSmallerInDim(point, useLatitude)) {
                if (leftChild.isDefined){
                    t = leftChild.get.searchNode(point, !useLatitude, this, currentDistance)
                    result = t._1
                    newDistance = t._2
                    if (newDistance < currentDistance) {
                        currentDistance = newDistance
                        currentNode = result
                    }
                }
                if (rightChild.isDefined){
                    t = rightChild.get.searchNode(point, !useLatitude, this, currentDistance)
                    result = t._1
                    newDistance = t._2
                    if (newDistance < currentDistance) {
                        currentDistance = newDistance
                        currentNode = result
                    }
                }
            } else {
                if (rightChild.isDefined) {
                    t = rightChild.get.searchNode(point, !useLatitude, this, currentDistance)
                    result = t._1
                    newDistance = t._2
                    if (newDistance < currentDistance) {
                        currentDistance = newDistance
                        currentNode = result
                    }
                }
                if (leftChild.isDefined){
                    t = leftChild.get.searchNode(point, !useLatitude, this, currentDistance)
                    result = t._1
                    newDistance = t._2
                    if (newDistance < currentDistance) {
                        currentDistance = newDistance
                        currentNode = result
                    }
                }
            }


            return (currentNode, currentDistance)
        }

        def isSmallerInDim(point: Node, useLatitude: Boolean): Boolean = {
            if (useLatitude) {
                if (point.lat < lat) {
                    true
                } else {
                    false
                }
            } else {
                if (point.lon < lon) {
                    true
                } else {
                    false
                }
            }

        }
    }

    private val medianCity = cities( cities.sortBy(city => city.lon).length/2 )
    var rootNode = new Node(medianCity)
    cities.foreach( city => insertNode(city) )

    def insertNode(city: City) = {
        rootNode.insert( new Node(city), true )
    }

    def findNearestNode(lat: Double, lon: Double): City = {
        val point = new Node(lat, lon)
        val (n, _) = rootNode.searchNode(point, true, rootNode, Double.MaxValue)
        n.city
    }

    // Should use haversine in CheckIn instead
    def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
        val R = 6372.8
        val dLat=(lat2 - lat1).toRadians
        val dLon=(lon2 - lon1).toRadians

        val a = Math.pow(Math.sin(dLat/2),2) + Math.pow(Math.sin(dLon/2),2) * Math.cos(lat1.toRadians) * Math.cos(lat2.toRadians)
        val c = 2 * Math.asin(Math.sqrt(a))
        R * c
    }
}




