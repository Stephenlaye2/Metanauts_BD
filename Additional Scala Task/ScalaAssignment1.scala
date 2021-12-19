package ScalaAssignment1

object ScalaAssignment1 {
  val strLst: List[String] = List("apple","Pawpaw", "mango", "pawpaw", "Mango", "Pear", "Pawpaw", "Mango", "Apple", "Mango")
  val lst:List[Int] = List(2, 1, 4, 23, 46, 5, 8, 3, 6, 7, 9)

  def main(args: Array[String]): Unit = {
    numOfOccurrence(strLst)
    evenOddNum(lst)
    removeDuplicate(strLst)
    largestSmallest(lst)
    addAndMultiply(lst)
  }

//  Convert String list to lower case
  def lowerCaseList(lst: List[String]): Seq[String] = {
    var newSeq: Seq[String] = Seq()
    lst.foreach(i => newSeq = newSeq :+ i.toLowerCase())
    newSeq
  }

// #1 Write a Scala program to count the number of occurrences of each element in a given list.
  def numOfOccurrence(strLst: List[String]): Unit = {
    val newSeq: Seq[String] = lowerCaseList(strLst)
    val result = newSeq.groupMapReduce(identity)(_ => 1)(_+_)
    println(result)
  }

// #2 Write a Scala program to find the even and odd numbers from a given list.
  def evenOddNum(lst: List[Int]): Unit = {
//    Print out each even and odd number
    for(i<- lst){
      if(i % 2 == 0){
        println(s"even: $i")
      }else{
        println(s"Odd: $i")
      }
    }
//    Assign Even and odd numbers into their individual list
    val even = lst.filter(_ % 2 == 0)
    val odd = lst.filter(_ % 2 != 0)
    println(s"Even Numbers: $even")
    println(s"Odd Numbers: $odd")

  }

//  #3 Write a Scala program to remove duplicates from a given list.
  def removeDuplicate(list: List[String]): Unit = {
    val lst: Seq[String] = lowerCaseList(list)
    var uniqueLst: List[Any] = List()
    lst.foreach(i => {
      if(!uniqueLst.contains(i)){
        uniqueLst = uniqueLst :+ i
      }
    })
    println(s"Unique List: $uniqueLst")
  }

// # Write a Scala program to find the largest and smallest number from a given list.
  def largestSmallest(lst: List[Int]): Unit = {
    var max: Double = 4.9E-324
    var min: Double = 1.7976931348623157E308
    lst.foreach(i => {
      if(i > max){
        max = i
      }
      if(i < min){
        min = i
      }
    })

    println(s"Max: $max")
    println(s"Min: $min")

  }

//  Write a Scala program to iterate over a list to print the elements and calculate the sum and product of all elements of this list.
  def addAndMultiply(lst: List[Int]): Unit = {
    val addition: Int = lst.sum
    val multiple: Int = lst.product
    println(s"Total Added Value: $addition")
    println(s"Product of Values: $multiple")
  }
}
