object Test1 {
  def main(args: Array[String]): Unit = {


    def loop(): Unit = {
      for (i <- 1 to 10) {
        def loop(): Unit = {
          println(i)
          if (i > 8) return
          println("world")
          if (i > 5) return
          println("hello")
        }

        loop()
      }
    }

    loop()

  }
}
