import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
import scala.collection.mutable.{ListBuffer, Map, Set=> mutSet}
import org.apache.spark.rdd.RDD
object task1{

  def main(args: Array[String]): Unit = {

    //Initialize
    val conf = new SparkConf().setAppName("task1").setMaster("local[*]").set("spark.driver.memory", "4g")
    val sc = new SparkContext(conf)
    val cas = args(0)
    val dataset = args(1)
    val sup = args(2)
    val data = sc.textFile("../Assignment_02/Data/"+dataset,4)
    val support = sup.toFloat
    var reduce= data.partitions.size.toFloat
    var partSize:Float = 0
    val header = data.first()

    //create basket RDD[(ID,basket)] by cas
    var basket:RDD[(String,List[String])] = sc.emptyRDD
    if(cas == "1") {
      basket = data.filter(x => x != header).map(x => x.split(",")).map(x => (x(0), (x(1), x(2)))).groupByKey().map(x => (x._1, x._2.map(y => y._1).toList.distinct))
      partSize = basket.collect().size
      println(reduce)
    }else if (cas =="2") {
      basket = data.filter(x => x != header).map(x => x.split(",")).map(x => (x(1), (x(0), x(2)))).groupByKey().map(x => (x._1, x._2.map(y => y._1).toList.distinct))
      partSize = basket.collect().size
      println(reduce)
    }

    //First level of map reduce, create candidate item sets
    val cand = basket.mapPartitionsWithIndex((i,part)=>{
      //For each partitions, count all the candidate items
      //var reduce1 = reduce/part.size
      //println("reduce",Array(part.size)(0))
      //count singleton
      //val a =part.toArray
      //reduce = partSize/(a.length.toDouble)
      //println(reduce)
      var len:Int = 2
      val rb = part.toArray
      reduce = partSize/(rb.length.toFloat)
      println(reduce)
      //var count = 0
      //rb.foreach(x=>count+=1)
      //println("reduce",count)
      var fsample = rb.flatMap(x => x._2.map(x=>(Set(x),1))).groupBy(_._1).mapValues(_.length).filter(_._2 >= (support/reduce)).toArray
      val single = fsample.map(x=>x._1)

      //var hashMap = hashBucket(rb.flatMap(x=>combine(x._2.toSet,len)),len).filter(_._2>=(support/reduce)).map(_._1)
      //var hashMap2= hashBucket2(rb.flatMap(x=>combine(x._2.toSet,len))).filter(_._2>=(support/reduce)).map(_._1)
     // println(hashBucket(rb.flatMap(x=>conBucket(x._2.toSet,i))).filter(_._2>=support/reduce).map(_._2).reduce(_+_))
      //hashMap.foreach(println)
      var candidate = construct(single).map(_.toList)//.filter(x=>hashMap.contains(hash(x,len)))//.filter(x=>hashMap2.contains(hash2(x)))

      println(candidate.size,i,len)
      //count pairs, triplets...etc
      while(!candidate.isEmpty){

        val set = rb.map(x=>candidate.filter(y=>y.forall(x._2.contains)).map(a=>(a.toSet,1))).flatMap(x=>x).groupBy(_._1).mapValues(_.length).filter(_._2>=(support/reduce)).toArray
        //val set = candidate.map(x=>(x.toSet,rb.filter(y=>x.forall(y._2.contains)).length)).filter(_._2>=support/reduce)

        //concat output array


        fsample = fsample ++ set
        val pairs = set.map(_._1)

        len = len+1
        //hashMap = hashBucket(rb.flatMap(x=>combine(x._2.toSet,len)),len).filter(_._2>=(support/reduce)).map(_._1)
        //hashMap2= hashBucket2(rb.flatMap(x=>combine(x._2.toSet,len))).filter(_._2>=(support/reduce)).map(_._1)
        //hashMap = hashBucket(rb.flatMap(x=>conBucket(x._2.toSet,i))).filter(_._2>=support/reduce).map(_._1)
        println("pairs",pairs.size,i,len)
        candidate = combination(pairs,len)//.filter(x=>hashMap.contains(hash(x,len)))
        //candidate = construct(pairs).map(_.toList).filter(x=>hashMap.contains(hash(x,len)))//.filter(x=>hashMap2.contains(hash2(x)))

        println(candidate.size,i,len)
      }

      //output array Array[(items,partial_count)]
      fsample.iterator
    }).reduceByKey(_+_).map(_._1.toList).collect

    //Second level of map reduce, get all the candidate sets and count them through
    val freqItem = basket.mapPartitionsWithIndex((i,part)=>{
      val rb = part.toArray
      //count every sets
      //val set = rb.map(x=>candidate.filter(y=>y.forall(x._2.contains)).map(a=>(a.toSet,1))).flatMap(x=>x).groupBy(_._1).mapValues(_.length).toArray
      //var l:List[String] =
      val p = rb.map(x=>cand.filter(y=>y.forall(x._2.contains)).map(a=>(a.toSet,1))).flatMap(x => x).groupBy(_._1).mapValues(_.length).toArray

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
    //val pairs = set.combinations(2).map(x=>x(0)++x(1))
    //val elements = pairs.distinct
    //pairs.map(x=>(x,1))
    val count = pairs.map(x=>(x,1)).groupBy(_._1).mapValues(_.length).toArray
    return count.filter(x=>x._2>=choose(choose(set(0).size+1,set(0).size),2)).map(_._1)
    //pairs.distinct
  }
  def conBucket(set: Set[String],count: Int):Array[Set[String]] ={
    val pairs = set.flatMap(x=>set.filter(_!=x).map(y=>Set(x)++Set(y)))
    pairs.toArray
  }
  def hashBucket(set: Array[Set[String]],i: Int): Array[(Int,Int)] ={
    //val hash = set.map(x=>(x.toList.map(x=>x.toInt).reduce(_+_)%10000,1))
    val hash = set.map(x=>(hash1(x.toList)%20000,1))
    val bucket = hash.groupBy(_._1).mapValues(_.length).toArray
    bucket
  }
  def hashBucket2(set: Array[Set[String]]): Array[(Int,Int)] ={
    val hash = set.map(x=>(x.toList.map(x=>x.toInt).reduce(_+_)%10000,1))
    val bucket = hash.groupBy(_._1).mapValues(_.length).toArray
    bucket
  }
  def hash1(c:List[String]):Int = {
    c.map(str=>{
      var count = 0
      for(i<-0 until str.length())
        count +=(str.charAt(i).toInt)<<i
    count}).sum
  }

  def hash(set:List[String],i:Int):Int={
    //set.map(x=>x.toInt).reduce(_+_)%10000
    hash1(set)%20000
  }
  def hash2(set:List[String]):Int={
    set.map(x=>x.toInt).reduce(_+_)%10000
  }
  //n choose k
  def choose(n: Int, k: Int): Int ={
    if (k == 0 || k == n) 1
    else choose(n - 1, k - 1) + choose(n - 1, k)
  }
  def combine(in: Set[String],i:Int): Array[Set[String]] = {
    val combinations = in.toList.combinations(i).toArray
    combinations.map(x=>x.toSet)
    //combinations.distinct
  }
  def combination(c: Array[Set[String]], itr: Int): Array[List[String]] = {
    var candidates = c.map(x=>x.toList).toList
    val singles = candidates.flatten.distinct
    var newCombinations = mutSet.empty[List[String]]
    candidates.map(comb => {
      singles.foreach( s => {
        var temp = ListBuffer(comb : _*)
        if(!comb.contains(s)){
          temp += s
        }
        if(temp.size == itr)
          newCombinations += temp.toList
      })
    })
    return newCombinations.toArray
  }
}