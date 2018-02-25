import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._

import org.apache.spark.rdd.RDD
object task1{

  def main(args: Array[String]): Unit = {

    //Initialize
    val conf = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val cas = args(0)
    val dataset = args(1)
    val sup = args(2)
    val data = sc.textFile("../Assignment_02/Data/"+dataset,2)
    val support = sup.toFloat
    val reduce = data.partitions.size
    val header = data.first()

    //create basket RDD[(ID,basket)] by cas
    var basket:RDD[(String,List[String])] = sc.emptyRDD
    if(cas == "1") {
      basket = data.filter(x => x != header).map(x => x.split(",")).map(x => (x(0), (x(1), x(2)))).groupByKey().map(x => (x._1, x._2.map(y => y._1).toList.distinct))
    }else if (cas =="2") {
      basket = data.filter(x => x != header).map(x => x.split(",")).map(x => (x(1), (x(0), x(2)))).groupByKey().map(x => (x._1, x._2.map(y => y._1).toList.distinct))
    }

    //First level of map reduce, create candidate item sets
    val cand = basket.mapPartitionsWithIndex((i,part)=>{
      //For each partitions, count all the candidate items

      //count singleton
      val rb = part.toArray
      var fsample = rb.flatMap(x => x._2.map(x=>(Set(x),1))).groupBy(_._1).mapValues(_.length).filter(_._2 >= support/reduce).toArray
      val single = fsample.map(x=>x._1)
      var candidate = construct(single).map(_.toList)

      //count pairs, triplets...etc
      while(!candidate.isEmpty){

        //val set = rb.map(x => for {p <- candidate if p.forall(x._2.contains)} yield (p.aggregate(Set(p(0)))((x, y) => x ++ Set(y), (x, y) => x ++ y), 1)).flatMap(x => x).groupBy(_._1).mapValues(_.length).filter(_._2>=support/reduce).toArray

        val set = rb.map(x=>candidate.filter(y=>y.forall(x._2.contains)).map(a=>(a.toSet,1))).flatMap(x=>x).groupBy(_._1).mapValues(_.length).filter(_._2>=support/reduce).toArray

        //concat output array
        fsample = fsample ++ set
        val pairs = set.map(_._1)
        candidate = construct(pairs).map(_.toList)

      }

      //output array Array[(items,partial_count)]
      fsample.iterator


    }).reduceByKey(_+_).map(_._1.toList).collect

    //Second level of map reduce, get all the candidate sets and count them through
    val freqItem = basket.mapPartitionsWithIndex((i,part)=>{
      val rb = part.toArray
      //count every sets
      val p = rb.map(x => for {p <- cand if p.forall(x._2.contains)} yield (p.aggregate(Set(p(0)))((x, y) => x ++ Set(y), (x, y) => x ++ y), 1,x._1)).flatMap(x => x).groupBy(_._1).mapValues(_.length).toArray
      p.iterator
    }).reduceByKey(_+_).filter(x=>x._2>=support).map(_._1.toList).collect

    //output in lexicographical order
    val orderedFreqItem = freqItem
    var s = 1
    var set:String = null
    if(dataset.charAt(0) == 's') {
      set = "Jeffrey_Jow_SON_"+dataset.split('.')(0).capitalize+".case"+cas+".txt"
    }else if(dataset.charAt(0)=='b'){
      set = "Jeffrey_Jow_SON_"+dataset.split('.')(0).capitalize+".case"+cas+"-"+sup+".txt"
    }

    val writer = new PrintWriter(new File(set))
    while(orderedFreqItem.exists(_.length==s)){
      writer.write(orderedFreqItem.filter(_.length==s).map(_.sorted.mkString("(",",",")")).sorted.mkString(",")+"\n"+"\n")
      s = s+1
    }

    writer.close()
    //orderedFreqItem.foreach(println)
    println(orderedFreqItem.size)

  }



  //construct candidate pairs
  def construct(set: Array[Set[String]]):Array[Set[String]] ={
    val pairs = set.flatMap(x=>set.filter(_!=x).map(y=>x++y))
    val elements = pairs.distinct
    val count = pairs.map(x=>(x,1)).groupBy(_._1).mapValues(_.length).toArray
    return count.filter(x=>x._2>=choose(choose(set(0).size+1,set(0).size),2)).map(_._1)
  }
  //n choose k
  def choose(n: Int, k: Int): Int ={
    if (k == 0 || k == n) 1
    else choose(n - 1, k - 1) + choose(n - 1, k)
  }
}