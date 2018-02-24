import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
object task1{

  def main(args: Array[String]): Unit = {

    //Initialize
    val conf = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("../Assignment_02/Data/books.csv",8)
    val support = 1200.toFloat
    val reduce = data.partitions.size
    val header = data.first()
    val reviewer_basket = data.filter(x => x != header).map(x => x.split(",")).map(x => (x(0), (x(1), x(2)))).groupByKey().map(x => (x._1, x._2.map(y => y._1).toList.distinct))

    val cand = reviewer_basket.mapPartitionsWithIndex((i,part)=>{
      val rb = part.toArray
      var fsample = rb.flatMap(x => x._2.map(x=>(Set(x),1))).groupBy(_._1).mapValues(_.length).filter(_._2 >= support/reduce).toArray

      val single = fsample.map(x=>x._1)
      var candidate = construct(single).map(_.toList)

      while(!candidate.isEmpty){
        val set = rb.map(x => for {p <- candidate if p.forall(x._2.contains)} yield (p.aggregate(Set(p(0)))((x, y) => x ++ Set(y), (x, y) => x ++ y), 1)).flatMap(x => x).groupBy(_._1).mapValues(_.length).filter(_._2>=support/reduce).toArray
        //val set = rb.map(x=>construct(x._2.map(x=>Set(x)).toArray)).map(x=>x.filter(y=>candidate.contains(y))).flatMap(x=>x).map(x=>(x,1)).groupBy(_._1).mapValues(_.length).filter(_._2>=support/reduce).toArray
        fsample = fsample ++ set
        val pairs = set.map(_._1)
        candidate = construct(pairs).map(_.toList)
      }
      fsample.iterator

    }).reduceByKey(_+_).map(_._1.toList).collect


    val freqItem = reviewer_basket.mapPartitionsWithIndex((i,part)=>{
      val rb = part.toArray
      val p = rb.map(x => for {p <- cand if p.forall(x._2.contains)} yield (p.aggregate(Set(p(0)))((x, y) => x ++ Set(y), (x, y) => x ++ y), 1,x._1)).flatMap(x => x).groupBy(_._1).mapValues(_.length).toArray
      p.iterator
    }).reduceByKey(_+_).filter(x=>x._2>=support).map(_._1.toList).collect

    val orderedFreqItem = freqItem//.map(_.sorted)//.sorted
    var s = 1
    val writer = new PrintWriter(new File("output1.txt"))
    while(orderedFreqItem.exists(_.length==s)){
      writer.write(orderedFreqItem.filter(_.length==s).map(_.sorted.mkString("(",",",")")).sorted.mkString(",")+"\n"+"\n")
      s = s+1
    }

    writer.close()
    //orderedFreqItem.foreach(println)
    println(orderedFreqItem.size)

  }



  //construct candidate pairs
  def construct(set: Array[Set[String]]):Array[Set[String]] ={//we can use set here to avoid duplicate see which runs faster
    //=============the yield part too slow=================
    /*val pairs = for{
      (a,ina) <- set.zipWithIndex
      (b,inb) <- set.zipWithIndex
      if inb>ina} yield a++b*/
    //=====================================================
    val pairs = set.flatMap(x=>set.filter(_!=x).map(y=>x++y))
    val elements = pairs.distinct
    val count = pairs.map(x=>(x,1)).groupBy(_._1).mapValues(_.length).toArray
    //val count = elements.map(x=>(x,pairs.count(_ == x)))
    return count.filter(x=>x._2>=choose(choose(set(0).size+1,set(0).size),2)).map(_._1)
  }
  //n choose k
  def choose(n: Int, k: Int): Int ={
    if (k == 0 || k == n) 1
    else choose(n - 1, k - 1) + choose(n - 1, k)
  }
}