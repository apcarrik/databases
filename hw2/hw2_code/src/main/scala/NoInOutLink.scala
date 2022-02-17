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
            // TODO

        val titles = sc
            .textFile(titles_file, num_partitions)
            // TODO

        /* No Outlinks */
        val no_outlinks = ???
        println("[ NO OUTLINKS ]")
        // TODO

        /* No Inlinks */
        val no_inlinks = ???
        println("\n[ NO INLINKS ]")
        // TODO
    }
}
