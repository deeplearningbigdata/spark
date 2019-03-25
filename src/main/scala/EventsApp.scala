import scala.util.Failure

/**
  * @Author : Deepak Chourasia
  */

object EventsApp extends App{
      try {
            /* Get events dataset from the events processing engine and see if all data is present in the engine */
            val eventsDataset = EventsEngine.getEventsDataset
            val tradeDataset = EventsEngine.getTradesDataset



            /* Below section will produce results for queries asked in the assignment */

            /*1.	What are the total number of events?*/
            println("Total number of events are :" + EventsEngine.getTotalNumOfEvents(eventsDataset))

            /*2.	What are the distinct product types?*/
            EventsEngine.printDistinctProducts(eventsDataset)

            /*3.	For each product type, what is the total amount? */
            EventsEngine.printEachProductAmtTotal(eventsDataset)

            /*4.	Can we see the events ordered by the time they occurred?*/
            EventsEngine.printEventsOrderByTime(eventsDataset)

            /*5.	For a given event, get the related events which make up the trade?*/
            val randomEvent = eventsDataset.head()
            EventsEngine.printAllEventsForAnEventInTrade(randomEvent, tradeDataset)

            /*6.	Data needs to be saved into Hive for other consumers?*/
            EventsEngine.saveDataToHiveTable(eventsDataset.toDF(), "Events")
            EventsEngine.saveDataToHiveTable(tradeDataset.toDF(), "Trades")
            /*7.	How do we get the minimum amount for each product type?*/
            EventsEngine.printMinAmtForEachProduct(eventsDataset)

            /*8.	How do we get the maximum amount for each product type?*/
            EventsEngine.printMaxAmtForEachProduct(eventsDataset)

            /*9.	How do we get the total amount for each trade, and the max/min across trades?*/
            EventsEngine.printTotalAmountForEachTrade(tradeDataset)
            EventsEngine.printMinMaxAmtForEachTrade(tradeDataset)

      }catch {
            case ex: Exception => {
                  println(s"Exception is : $ex")
                  Failure(ex)
            }
            case unknown: Exception => {
                  println(s"Unknown exception: $unknown")
                  Failure(unknown)
            }
      }
}
