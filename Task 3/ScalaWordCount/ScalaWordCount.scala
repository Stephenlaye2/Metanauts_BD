package ScalaWordCount

import scala.io.Source
import scala.util.matching.Regex
import util.control.Breaks._

object ScalaWordCount {

  var fileData: String = ""
  def main(args: Array[String]): Unit = {
    readFile()
    matchData()
    countWords()

  }

//  Read shakespeare.txt file and save the content to fileData
  def readFile(): Unit = {
    val fileReader = Source.fromFile("shakespeare.txt")
    fileReader.foreach(i => fileData = fileData + i.toString.toLowerCase())
    fileReader.close()
  }

//  Return list of words that matched a word pattern
  def matchData() : List[String] ={
    val p = new Regex("[a-zA-Z][-_'a-zA-Z]*")
    var wordLst: List[String] = List()
    val matched = p.findAllMatchIn(fileData)
    matched.foreach(word => wordLst = wordLst :+ word.toString())
    wordLst
  }

//  Return list of all words
  def combinedWords(): List[String] = {
    val matched = matchData()
    var twoDashes = ""
    val cleanWords: List[String] = matched.filter(word=> !word.contains("--"))

//   Separate words with two dashes
    for(word <- matched){
      breakable{
        if(word.contains("--")){
          twoDashes = twoDashes + s"$word--"
          break
        }
      }

    }

//    Convert words with two dashes into a list and add it to the cleanWords list
    val newString = twoDashes.replace("----", "--")
    val cleanLst: Array[String] = newString.split("--")
    val allWords = List.concat(cleanWords, cleanLst.toList)

    allWords
  }

//  Print the total world count and the number of occurence of each word
  def countWords(): Unit ={
    val allWords = combinedWords()
    val wordCount = allWords.groupMapReduce(identity)(_ => 1)(_+_)
    println(s"Total Word Count: ${allWords.length}")
    println(wordCount)
  }

}
