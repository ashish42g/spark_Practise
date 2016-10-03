import org.apache.spark.{SparkContext, SparkConf}

object SparkSqlPractise {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]") setAppName ("General")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val people = sc.textFile("/Users/ashish/Applications/spark/spark-1.5.1-bin-hadoop2.6/examples/src/main/resources/people.txt")
      .map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
    people.registerTempTable("people")
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
  }
}
case class Person(name: String, age: Int)