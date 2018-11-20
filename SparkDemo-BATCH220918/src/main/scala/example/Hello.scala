package example

object Hello extends Greeting with App {
  println(greeting)
  val msg = "helloworld";
//  msg = "test";
//  val msg = "test";
}

trait Greeting {
  lazy val greeting: String = "hello"
}
