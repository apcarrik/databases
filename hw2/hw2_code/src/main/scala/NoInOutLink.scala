import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {
    def main(args: Array[String]) {
        val input_dir = "sample_input"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        

        val conf = new SparkConf()
            .setAppName("NoInOutLink")
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

        /* No Outlinks */
        val no_outlinks_idx = titles.map(t=>(t._1))
          .subtract(links.map(l=>(l._1)))
          .toArray()
        val no_outlinks = titles
          .filter(t => no_outlinks_idx.contains(t._1))
        println("[ NO OUTLINKS ]")
        no_outlinks
          .take(10)
          .foreach(println)

        /* No Inlinks */
        val no_inlinks_idx = titles.map(t=>(t._1))
          .subtract(links.map(l=>(l._2)))
          .toArray()
        val no_inlinks = titles
          .filter(t => no_inlinks_idx.contains(t._1))
        println("\n[ NO INLINKS ]")
        no_inlinks
          .take(10)
          .foreach(println)
    }
}
