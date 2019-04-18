package utils

class Point(var id: Long, var coords: Array[Double]) extends Serializable{

  def this() {
    this(-1, null)
  }

  def this(other: Point) {
    this(other.id, other.coords.clone())
  }

  def this(coords: Array[Double]) {
    this(-1, coords)
  }

  def this(line: String) {
    //id 1.0 2.0 3.0
    this()
    val words = line.split(" ")
    this.id = words(0).toLong
    this.coords = words.drop(1).map(_.toDouble)
  }

//  def this(id: Long, coords: String) {
//    this()
//    this.id = id
//    this.coords = coords.split(" ").map(_.toDouble)
//  }

  def distanceTo(other: Point): Double = {
    var sum = 0.0
    for (i <- 0 until this.coords.size) sum += scala.math.pow(this.coords(i)-other.coords(i), 2)
//    scala.math.sqrt(sum)
    sum
  }

  override def toString: String = {
    var strCoords = this.id + "#("
    for(i <- 0 until coords.size-1) strCoords += this.coords(i).toString + ","
    strCoords += this.coords.last.toString
    strCoords += ")"
    strCoords
  }
}
