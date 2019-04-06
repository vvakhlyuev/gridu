import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object MainOps {

  // Example purchases: Prod3,33,2017-06-04 01:00,Category4,1.1.248.37,1605221
  // Example locations: 1605221,en,AS,Asia,TH,Thailand,61,"Changwat Uthai Thani",,,"Uthai Thani",,Asia/Bangkok
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Ops").setMaster("local")
    val sc = new SparkContext(conf)

    val purchasesStaging = sc.textFile("hdfs://192.168.56.101:8020/tmp/purchases_staging/*/*/*/").cache()
    val locationsStaging = sc.textFile("hdfs://192.168.56.101:8020/tmp/locations_staging/")

    //    topCategories(purchasesStaging)
    topProducts(purchasesStaging)
    //    topCountries(purchasesStaging, locationsStaging)

    sc.stop()
  }

  private def topCategories(purchasesStaging: RDD[String]) = {
    val topCategories = purchasesStaging
      .map(s => {
        val ar = s.split(",");
        (ar(3), 1)
      })
      .reduceByKey(_ + _)
      .map(_.swap)
      .top(10)
    topCategories.foreach(println)
  }

  private def topProducts(purchasesStaging: RDD[String]) = {
    val topProducts = purchasesStaging
      .map(s => {
        val ar = s.split(",");
        ((ar(0), ar(3)), 1) // ((product, category), count)
      })
      .reduceByKey(_ + _)
      .map(x => (x._1._2, (x._1._1, x._2)))
      .aggregateByKey(ArrayBuffer.empty[(String, Int)])(sumCategories, sumCategoriesAccs)
      .collect()
    topProducts.foreach {
      case (s: String, a: ArrayBuffer[(String, Int)]) => println(s + ": " + a.mkString(","))
    }
  }

  private def sumCategoriesAccs = {
    (acc1: ArrayBuffer[(String, Int)], acc2: ArrayBuffer[(String, Int)]) => (acc1 ++ acc2).take(10)
  }

  private def sumCategories = {
    (acc: ArrayBuffer[(String, Int)], categoryCount: (String, Int)) => {
      acc += categoryCount;
      acc.sortBy(x => (x._2, x._1));
      acc.take(10);
    }
  }

  private def topCountries(purchasesStaging: RDD[String], locationsStaging: RDD[String]) = {
    // key both by geoname_id
    val p = purchasesStaging.map(s => s.split(",")).keyBy(_ (5));
    val l = locationsStaging.map(s => s.split(",")).keyBy(_ (0));

    val spendingsByCountry = p.join(l) // join on geoname_id
      .map { case (geoname_id, (purch, locs)) => (locs(5), purch(1).toDouble) } // map (country_name, item_price)
      .reduceByKey(_ + _)
      .map(_.swap)
      .top(10)
    spendingsByCountry.foreach(println)
  }
}