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
            .map(t=>(t._2.toLong+1,t._1))

        val outLinksCount = links
            .map(l => (l._1, 1))
            .reduceByKey( (a, b) => (a + b) )

        val N = titles.count()
        val initRank = 100.toDouble / N

        var PageRanks = titles
            .map(t => (t._1, initRank))

        val d = 0.85

        var zeros = titles
            .map(t => (t._1, 0.toDouble))

        /* PageRank */
        for (i <- 1 to iters) {
            PageRanks = PageRanks
                .join(links)
                .join(outLinksCount)
                .map{case (out,((pageRank,in),count)) => (in, pageRank / count)}
                .union(zeros)
                .reduceByKey( (a, b) => (a + b) )
                .mapValues(p => (1 - d) / N * 100 + d * p)
        }
        val s = PageRanks
            .values
            .sum()

        PageRanks = PageRanks
            .mapValues(p => p / s * 100)

        println("[ PageRanks ]")
        // TODO
        titles
            .join(PageRanks)
            .sortBy(_._2._2, ascending = false)
            .take(10)
            .foreach(println)

    }
}
