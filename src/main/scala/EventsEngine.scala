import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Event (product: String, timestamp : Timestamp, amount : Double)
case class Trade (events : List[Event])

/**
  * @Author : Deepak Chourasia
  */
object EventsEngine{

  val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  import sparkSession.implicits._
  val dataFilePath = "src/resources/data.csv"
  val delimiter = "|"
  val linesDS = sparkSession.read.textFile(dataFilePath)


  val recordDS = linesDS.flatMap(line => split(line, delimiter))
  val eventsDS = recordDS.map(mapToEvent).as[Event]
  def getEventsDataset : Dataset[Event] = eventsDS


  val tradeDS = linesDS.map(mapToTrade).as[Trade]

  def getTradesDataset : Dataset[Trade] = tradeDS

  def split(line: String, delimiter: String): List[String] = {
    val records = StringUtils.split(line, delimiter)
    records.toList
  }

  def mapToTrade(line: Any): Trade = {
    if (line.toString() == "") null
    else {
      val records = split(line.toString, delimiter)
      val events = records.map(mapToEvent)
      Trade(events)
    }
  }

  def mapToEvent(record: Any): Event = {
    if (record.toString() == "") null
    else {
      val fields = StringUtils.split(record.toString, ",")
      val product = fields(0).trim
      val timestamp = fields(1).trim
      val amount = fields(2).trim
      Event(product, getTimestamp(timestamp), amount.toDouble)
    }
  }

  def getTimestamp(x: Any): Timestamp = {
    val format = new SimpleDateFormat("ddMMyyyy:HH:mm")
    if (x.toString() == "") null
    else {
      val d = format.parse(x.toString());
      val t = new Timestamp(d.getTime());
      t
    }
  }

  /**
    * @param dataFrame    this data frame is saved as a Hive table.
    * @param tableName    this is the table name provided.
    */
  def saveDataToHiveTable(dataFrame : DataFrame,  tableName : String) = {
      val table = dataFrame.write.saveAsTable(tableName)
      println(table)
  }

  /**
    * This function returns total count for event dataset.
    * @param eventsDataset
    */
  def getTotalNumOfEvents(eventsDataset : Dataset[Event]) = eventsDataset.count()

  /**
    * This function prints the distinct product names in the event dataset.
    * @param eventsDataset
    */
  def printDistinctProducts(eventsDataset : Dataset[Event]): Unit = {
      val distinctProducts = eventsDataset.select(eventsDataset("product")).distinct
      distinctProducts.show()
      /*val distProdAsString = distinctProducts.as[String].collectAsList()
      distProdAsString.toString // in case if we want to return the value*/
  }

  /**
    * This function prints each product and its total trasacted amount in the event.
    * @param eventsDataset
    */
  def printEachProductAmtTotal(eventsDataset : Dataset[Event]) : Unit = {
      val aggrDS = eventsDataset.groupBy("product").agg( sum("amount").as("total"))
      aggrDS.show()
  }

  /**
    * This function sorts and prints the events according to timestamp.
    * @param eventsDataSet
    */
  def printEventsOrderByTime(eventsDataSet : Dataset[Event]) = {
      eventsDataSet.orderBy("timestamp").show()
  }

  /**
    * This fuction prints all the events in a particular trade for a given event.
    * @param randomEvent
    * @param tradesDataset
    */
  def printAllEventsForAnEventInTrade(randomEvent : Event, tradesDataset : Dataset[Trade]) = {
      val filteredTrade = tradesDataset.filter(trade => {
          trade.events.contains(randomEvent)
      })
      filteredTrade.flatMap(trade => trade.events).show()
  }

  /**
    * This function prints minimum amount for each product in all events.
    * @param eventsDataSet
    */
  def printMinAmtForEachProduct(eventsDataSet : Dataset[Event]) = {
      val aggrDS = eventsDataSet.groupBy("product").agg( min("amount").as("minimum amount"))
      aggrDS.show()
  }

  /**
    * This function prints maximum amount for each product in all events.
    * @param eventsDataSet
    */
  def printMaxAmtForEachProduct(eventsDataSet : Dataset[Event]) = {
      val aggrDS = eventsDataSet.groupBy("product").agg( max("amount").as("maximum amount"))
      aggrDS.show()
  }

  /**
    * This function prints total amount for each trade.
    * @param tradesDataSet
    */
  def printTotalAmountForEachTrade(tradesDataSet : Dataset[Trade]) = {
      val aggrDS = tradesDataSet.map(trade => trade.events.map(event => event.amount))
      val aggAmountDS = aggrDS.map(amountList=> amountList.reduce(_+_))
      aggAmountDS.select(bround(aggAmountDS("value"), 2).as("Sum/Trade")).show()
  }

  /**
    * This function prints minimum and maximum amount for each trade.
    * @param tradesDataSet
    */
  def printMinMaxAmtForEachTrade(tradesDataSet : Dataset[Trade]) = {
    val aggrDS = tradesDataSet.map(trade => trade.events.map(event => event.amount))
    val aggAmountDS = aggrDS.map(amountList => (amountList.min,amountList.max))
    aggAmountDS.select(aggAmountDS("_1").as("minimum") , aggAmountDS("_2").as("maximum")).show()
  }
}
