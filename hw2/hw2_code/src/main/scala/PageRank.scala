import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster("local[*]")
            .set("spark.driver.memory", "1g")
            .set("spark.executor.memory", "2g")

        val sc = new SparkContext(conf)

        val links = sc
          .textFile(links_file, num_partitions)
          .map(l=>l.split(": "))
          .flatMap( arr => {
              val node = arr(0)
              val neighbors_str = arr(1)
              val neighbors = neighbors_str.split(" ")
              neighbors.map(neighbor => (node.toLong, neighbor.toLong))
          })

        val titles = sc
          .textFile(titles_file, num_partitions)
          .zipWithIndex()
          .map(t => (t._2.toLong+1,t._1))

        /* PageRank */
        val d = 0.85
        val N = titles.count
        val numoutlinks = sc
          .textFile(links_file, num_partitions)
          .map(l => l.split(": "))
          .map(l => (l(0).toLong,l(1).split(" ").length))
        var pageranks = titles
          .map(t => (t._1,100.0/N))
        var prsum = pageranks.takeSample(false,pageranks.count.toInt,1)
          .map(p => p._2)
          .sum

        for (i <- 1 to iters) {
            val y_pageranks = pageranks
              .join(links)
              .map(yp => (yp._1, (yp._2._1, yp._2._2)))
            val y_pagerkanks_numoutlinks = y_pageranks
              .join(numoutlinks)
              .map(ypn => (ypn._1, (ypn._2._1._1, ypn._2._1._2, ypn._2._2)))
            val y_pageranks_div_numoutlinks = y_pagerkanks_numoutlinks
              .map(ypn => (ypn._2._2, ypn._2._1 / ypn._2._3))
              .reduceByKey(_ + _)
            pageranks = pageranks
              .leftOuterJoin(y_pageranks_div_numoutlinks)
              .map(p => (p._1, (1-d)/N*100 + d*p._2._2.getOrElse(0.0)) )
            prsum = pageranks.takeSample(false,pageranks.count.toInt,1)
              .map(p => p._2)
              .sum
        }

        /* Normalize final PageRanks */
        pageranks = pageranks
          .map(p => (p._1, p._2*(100.0/prsum)))

        println("[ PageRanks ]")
        titles
          .join(pageranks)
          .map(tp => (tp._2._2, (tp._1, tp._2._1)))
          .sortByKey(false)
          .map(tp => (tp._2._1, (tp._2._2, tp._1)))
          .take(11)
          .foreach(println)
    }
}
