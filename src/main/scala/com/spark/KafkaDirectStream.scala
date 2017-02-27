package com.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import kafka.common.TopicAndPartition
import kafka.utils.ZkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import kafka.message.MessageAndMetadata

object KafkaDirectStream {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val topicset = "dilip,abhishek".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092", "group.id" -> "spark-kafka-consumer")
    val checkpointDir = "D:\\tmp\\checkpointLogs"

    val ssc = StreamingContext.getOrCreate(checkpointDir, initilizeStreamingContext(topicset, kafkaParams, checkpointDir) _)
    ssc.start()
    ssc.awaitTermination()

    sys.addShutdownHook {
      println("SHUTDOWN HOOK CALLED!!")
      ssc.stop(true, true)
    }

  }
  def initilizeStreamingContext(topicset: Set[String], kafkaParams: Map[String, String], checkpointDir: String)(): StreamingContext = {

    val sparkConf = new SparkConf().setAppName("SparkKafkaStreamService").setMaster("local[*]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //create direct kafka stream
    val messages = createCustomDirectKafkaStream(ssc, kafkaParams, "localhost:2181", "/kafka4", topicset).map(_._2)
    messages.foreachRDD(rdd => {
      if (rdd.take(1).length == 0) {
        println("Empty Record......")
      } else {
        rdd.collect().foreach(println)
      }

    })
    //ssc.checkpoint(checkpointDir)
    ssc
  }

  //createDirectStream method overloaded
  def createCustomDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], zkHosts: String, zkPath: String, topics: Set[String]): InputDStream[(String, String)] = {

    //val topic = topics.last
    @transient lazy val zkClient = new ZkClient(zkHosts, 30000, 30000)
    println("zookeeper..." + zkClient)
    val storedOffsets = readOffsets(zkClient, zkHosts, zkPath)
    val kafkaStream = storedOffsets match {
      case None => //start from the latest offsets
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(fromOffsets) => //start from previously saved offsets
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    //save the offsets
    kafkaStream.foreachRDD(rdd => saveOffsets(zkClient, zkHosts, zkPath, rdd))
    kafkaStream

  }

  //Read the previously saved offsets from zookeeper
  private def readOffsets(zkClient: ZkClient, zkHosts: String, zkPath: String): Option[Map[TopicAndPartition, Long]] = {
    println("Reading offsets from zookeeper....." + zkHosts + "   ," + zkPath)
    val stopwatch = new Stopwatch()
    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)
    offsetsRangesStrOpt match {
      case Some(offsetsRangerStr) =>
        println("Read offset ranges..:" + offsetsRangerStr)
        val offsets = offsetsRangerStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(topic,partitionStr, offsetStr) => (TopicAndPartition(topic.toString, partitionStr.toInt) -> offsetStr.toLong) }
          .toMap

        println("Done reading offsets from zookeeper Took" + stopwatch)
        Some(offsets)
      case None =>
        println("No offsets found in zookeeer. Took" + stopwatch)
        None
    }

  }

  private def saveOffsets(zkClient: ZkClient, zkHosts: String, zkPath: String, rdd: RDD[_]): Unit = {

    println("Saving offsets to zookeeper")
    val stopwatch = new Stopwatch()
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetsRanges.foreach(offsetRange => println("using..:" + offsetRange))
    val offsetsRangerStr = offsetsRanges.map(offsetRange => s"${offsetRange.topic}:${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    println("writing offsets to zookeeper zkClient=" + zkClient + "  zkHost" + zkHosts + " offset Range--  " + offsetsRangerStr)
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangerStr)
    println("done updating offsets in Zookeeper. Took" + stopwatch)
  }

  class Stopwatch {
    private val start = System.currentTimeMillis()
    override def toString() = { System.currentTimeMillis() - start } + "ms"
  }

}