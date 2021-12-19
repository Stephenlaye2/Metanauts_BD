**Given Lists:**

``  val strLst: List[String] = List("apple","Pawpaw", "mango", "pawpaw", "Mango", "Pear", "Pawpaw", "Mango", "Apple", "Mango")
val lst:List[Int] = List(2, 1, 4, 23, 46, 5, 8, 3, 6, 7, 9)``

***1. Write a Scala program to count the number of occurrences of each element in a given list***

**Method: ``numOfOccurrence(strLst)``**

**Result:**

``
Map(apple -> 2, pear -> 1, pawpaw -> 3, mango -> 4)
``

***2. Write a Scala program to find the even and odd numbers from a given list***

**Method: ``evenOddNum(lst)``**

**Result:**

``
even: 2
Odd: 1
even: 4
Odd: 23
even: 46
Odd: 5
even: 8
Odd: 3
even: 6
Odd: 7
Odd: 9
Even Numbers: List(2, 4, 46, 8, 6)
Odd Numbers: List(1, 23, 5, 3, 7, 9)
``


***3. Write a Scala program to remove duplicates from a given list***

**Method: ``removeDuplicate(strLst)``**

**Result:**

``
Unique List: List(apple, pawpaw, mango, pear)
``

***4. Write a Scala program to find the largest and smallest number from a given list***

**Method: ``largestSmallest(lst)``**

**Result:**

``Max: 46.0
Min: 1.0``

***5. Write a Scala program to iterate over a list to print the elements and calculate the sum and product of all elements of this list***

**Method: ``addAndMultiply(lst)``**

**Result:**

``Total Added Value: 114
Product of Values: 383927040``