
object Foo {
  def printWithHello(x: String) = {println("Hello, " + x)}

  def isAllDigits(x: String) = x.map(Character.isDigit(_)).reduce(_&&_)

  def main(args: Array[String]) = {
    println("Here are the args")
    args.foreach(printWithHello)
    val inputs = args.filter(isAllDigits).map( (x:String)=>x.toDouble )
    val avg = inputs.reduce(_+_) / inputs.length
    println("Average value: " + avg.toString)
  }
}

//Foo.main(args)

