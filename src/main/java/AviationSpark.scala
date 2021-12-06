import jdk.internal.org.jline.keymap.KeyMap.display
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{avg, count, expr, lit, rank, round}
import org.json4s.scalap.scalasig.ClassFileParser.header

object AviationSpark {
  def main(args: Array[String]): Unit = {
    // DATA IMPORT AND SOME BASIC DATA CLEANSING AND JOINS AND CONCAT
    val spark = SparkSession.builder().appName("App").master("local").getOrCreate()
    val passengerDataframe = spark.read.format("csv").option("header","true").option("inferSchema","true").load("src/main/resources/passengers.csv")
    val flightDataframe = spark.read.format("csv").option("header","true").option("inferSchema","true").load("src/main/resources/flightData.csv")
    val flightDataframe_a = flightDataframe.withColumnRenamed("to", "to_loc").withColumnRenamed("from","from_loc")
    flightDataframe_a.createTempView("flightDataframe_a_v")
    passengerDataframe.createTempView("passengerDataframe_v" )
    val flightPassengerDataframe = spark.sql(
      """SELECT firstname, lastname, to_loc,from_loc,flightId, date FROM flightDataframe_a_v FULL JOIN passengerDataframe_v ON flightDataframe_a_v.passengerId = passengerDataframe_v.passengerId""".stripMargin)
    val flightPassengerDataframe_a = flightPassengerDataframe.withColumn("is_uk", expr("to_loc == 'uk'")).sort("is_uk").toDF()
    flightPassengerDataframe_a.toDF()
    flightPassengerDataframe_a.createTempView("flightPassengerDataframe_a_v")
    val flightPassengerDataframe_b = spark.sql(
      """
        SELECT CONCAT(flightPassengerDataframe_a_v.FIRSTNAME, ',', flightPassengerDataframe_a_v.LASTNAME) AS FULL_NAME,
        CONCAT(from_loc, ',', to_loc) as to_from_locations, flightPassengerDataframe_a_v.*
        FROM  flightPassengerDataframe_a_v ; """.stripMargin).toDF()
    flightPassengerDataframe_b.createTempView("flightPassengerDataframe_b_v")
    //// SPARK SQL FOR DATA QUERY
    // MOST FREQUENT FLYING PASSENGERS
    spark.sql(
      """SELECT full_name, count(*) as flightFrequency
      FROM flightPassengerDataframe_b_v
      GROUP BY full_name
      ORDER BY 2 DESC """
      .stripMargin).show()
    // MOST FREQUENT TRAVELLED LOCATIONS [ ROUTES ]
    spark.sql("""
      SELECT to_from_locations, count(to_from_locations) as freq
      FROM flightPassengerDataframe_b_v
      GROUP BY to_from_locations
      ORDER BY 2 DESC """
     .stripMargin).show()
    // MOST COMMON FLIGHTS [flight_id] EACH MONTH
    spark.sql("""
      SELECT to_from_locations, flightId, date
      FROM flightPassengerDataframe_b_v
      GROUP BY flightId,date, to_from_locations
      ORDER BY 2 DESC """
        .stripMargin).show()
    // MOST COMMON FLIGHT TRAVELLERS [flight_id]
    spark.sql("""
      SELECT full_name,to_from_locations
      FROM flightPassengerDataframe_b_v
      GROUP BY to_from_locations,full_name
      ORDER BY 2 DESC"""
      .stripMargin).show(50)
    // TOGETHER TRAVELLERS - IF YOU PLEASE
    val togetherTravellers = spark.sql("""
      SELECT full_name,to_from_locations AS ON_FLIGHT_ROUTE
      FROM flightPassengerDataframe_b_v
      WHERE date = '2017-10-19'
      GROUP BY to_from_locations,full_name
      ORDER BY to_from_locations DESC
      """
      .stripMargin).toDF()
    togetherTravellers.createTempView("togetherTravellers_v")
//    spark.sql("""
//        SELECT *
//        FROM togetherTravellers_v
//        """.stripMargin).show()
// FLIGHTS BY DATE ABOUT 4-6 FLIGHTS A DAY APPARANTLY
    //FREQUENT FLYERS
    spark.sql("""
       -- DAILY PAX
      SELECT full_name, count(to_from_locations) as totalFlights
      FROM flightPassengerDataframe_b_v
      GROUP BY full_name
      ORDER BY 2 DESC"""
      .stripMargin).show(10)
//
//    spark.sql("""
//      SELECT firstname, lastname, count(*) as totalFLights
//      FROM FPView
//      GROUP BY firstname, lastname
//      ORDER BY TotalFlights desc;
//      """)
  }
}
