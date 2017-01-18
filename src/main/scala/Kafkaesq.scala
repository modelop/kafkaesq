
import scala.collection.mutable.ArrayBuffer

import java.lang.Runnable
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer,ProducerRecord}

import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter
import scala.io.Source

import kafka.client.ClientUtils
import kafka.consumer.SimpleConsumer
import kafka.api.{FetchRequestBuilder,Request}
import kafka.common.TopicAndPartition

import org.apache.kafka.common.utils.Utils

// Based on SimpleConsumerShell.scala from Kafka tools

object Kafkaesq {

  val CLIENT_ID = "kafkaesq"
	val MAX_WAIT_MS = 30000

  def usage = {
    println("""Usage: kafkaesq [ options ] <input-topic> <output-topic>
              |Options:
              |  --input-file <file-name>       (no default)
              |  --output-file <file-name>      (no default)
              |  --bootstrap <host:port,...>    localhost:9092
              |  --count <num>                  (# of inputs)
              |  --batch-size <num>             1
              |  --batch-pause <milliseconds>   0
              |  --verbose                      false
              |  --statistics                   false""".stripMargin)
    sys.exit(1)
  }

  class Options {
    var inputTopic: String = ""
    var outputTopic: String = ""
    var inputFile: Option[String] = None
    var outputFile: Option[String] = None
    var bootstrap: String = "localhost:9092"
    var count: Option[Int] = None
    var batchSize = 1
    var batchPause = 0
    var verbose = false
    var statistics = false
  }

  val options = new Options

  def main(args: Array[String]) = {
    val topics: ArrayBuffer[String] = new ArrayBuffer

    def parseOpts(argList: List[String]): Unit = argList match {
      case "--input-file" :: file :: more =>
        if (!(new File(file).exists)) {
          println(s"$file not found")
          sys.exit(1)
        }
        options.inputFile = Some(file)
        parseOpts(more)
      case "--output-file" :: file :: more =>
        options.outputFile = Some(file)
        parseOpts(more)
      case "--bootstrap" :: bootstrap :: more =>
        options.bootstrap = bootstrap
        parseOpts(more)
      case "--count" :: num :: more =>
        options.count = Some(num.toInt)
        parseOpts(more)
      case "--batch-size" :: num :: more =>
        options.batchSize = num.toInt
        parseOpts(more)
      case "--batch-pause" :: num :: more =>
        options.batchPause = num.toInt
        parseOpts(more)
      case "--verbose" :: more =>
        options.verbose = true
        parseOpts(more)
      case "-v" :: more =>
        options.verbose = true
        parseOpts(more)
      case "--statistics" :: more =>
        options.statistics = true
        parseOpts(more)
      case typo :: more if typo.startsWith("-") =>
        println(s"Option $typo ignored")
        parseOpts(more)
      case topic :: more =>
        topics += topic
        parseOpts(more)
      case Nil => ()
      case _ => usage 
    }

    parseOpts(args.toList)

    if (topics.length != 2)
      usage
    options.inputTopic = topics(0)
    options.outputTopic = topics(1)

    run()
  }

  def run() {

    // Collect inputs
    options.inputFile match {
      case Some(file) =>
        val src = Source.fromFile(file)
        val inputs = src.getLines.toArray
        src.close
        if (options.verbose)
          println(s"${inputs.length} inputs read from $file")
        run1(inputs)

      case None =>
        val buffer = new ArrayBuffer[String]
        while (true) {
          val x = Console.readLine("* ")
          if (x == null) {
            if (options.verbose)
              println(s"${buffer.length} inputs read from console")
            run1(buffer.toArray)
          }
          buffer += x
        }
    }
  }

  def run1(inputs: Array[String]) = {
		if (options.count.isEmpty)
        options.count = Some(inputs.length)

    val consumer = make_consumer()
    val x = TopicAndPartition(options.outputTopic, 0)
    val savedOffset = consumer.earliestOrLatestOffset(x, -1, Request.DebuggingConsumerId)
    if (options.verbose)
      println(s"The latest offset for ${options.outputTopic}:0 is ${savedOffset}")

    val producer = make_producer()
    if (options.verbose)
      println(s"Producer connected to Kafka at ${options.bootstrap}")

		run2(inputs, producer, consumer, savedOffset)
	}

	case class Position(var index: Int, var left: Int);

	class Stats {
		var count = 0;
		var total = 0.0;
		var total2 = 0.0;
		
		def add(x: Double) = {
			count += 1
			total += x
			total2 += x*x
		}

    // See https://opendatagoup.atlassian.net/wiki/display/IN/Kafka+Test+Client
		def avg = total/count
		def stdDev = Math.sqrt((total2 - 2*avg*total + count*avg*avg)/(count - 1))
	}

	def run2(inputs: Array[String],
					 producer: KafkaProducer[String,String],
					 consumer: SimpleConsumer,
					 savedOffset: Long) = {

    if (options.verbose)
      println(s"Batch size is ${options.batchSize}")

    var sink: Option[BufferedWriter] = None
    if (!options.outputFile.isEmpty)
      sink = Some(new BufferedWriter(new FileWriter(new File(options.outputFile.get))))

		val latencyStats = new Stats
		val throughputStats = new Stats

		if (options.statistics) {
			println("           Latency (s)   Throughput (msg/s)")
			println("Batch     Avg   StdDev         Avg   StdDev")
			println("-------------------------------------------")
		}

		val position = new Position(0, options.count.get)
		var offset = savedOffset
		while (position.left > 0) {
			val batchStarted = System.currentTimeMillis

			val n = sendBatch(options.batchSize, producer, options.inputTopic, inputs, position)
			val sendTime = System.currentTimeMillis - batchStarted
			val trimSize = if (options.batchSize > n) n else options.batchSize
			offset = readBatch(trimSize, consumer, options.outputTopic, offset, !options.statistics, sink)

			val l = (System.currentTimeMillis - batchStarted)/1000.0
			latencyStats.add(l)
			throughputStats.add(trimSize/l)

			if (options.statistics)
				println(f"${latencyStats.count}%5d${latencyStats.avg}%8.3f${latencyStats.stdDev}%9.3f${throughputStats.avg}%12.1f${throughputStats.stdDev}%9.1f")

			if (options.batchPause > 0)
				Thread.sleep(options.batchPause)
		}

    if (!sink.isEmpty)
      sink.get.close

		producer.close
		consumer.close
		if (options.verbose)
			println("Disconnected")
	}

	def sendBatch(batchSize: Int,
								producer: KafkaProducer[String,String],
							  topic: String,
								inputs: Array[String],
								position: Position): Int = {
		var sent = 0
		while (sent < batchSize && position.left > 0) {
      val r = new ProducerRecord[String,String](topic, 0, "", inputs(position.index))
      producer.send(r)
			sent += 1
			position.left -= 1
			position.index += 1
			if (position.index >= inputs.length)
				position.index = 0
		}
		return sent
	}

	val builder = new FetchRequestBuilder()
								 .clientId(CLIENT_ID)
								 .maxWait(MAX_WAIT_MS)
								 .minBytes(1)

	def readBatch(batchSize: Int,
							  consumer: SimpleConsumer,
								topic: String,
								startOffset: Long,
								printData: Boolean,
                sink: Option[BufferedWriter]): Long = {
		var n = batchSize
		var offset = startOffset
		while (n > 0) {
			val maxBytes = 4*1024*1024
      println(s"Requesting messages at offset $offset")
    	val fetchReq = builder.addFetch(topic, 0, offset, maxBytes).build
      val fetchResp = consumer.fetch(fetchReq)
      val messages = fetchResp.messageSet(topic, 0)
			for (x <- messages) {
				offset = x.nextOffset
				val msg = x.message
				val datum = new String(Utils.readBytes(msg.payload))
        if (!sink.isEmpty) {
          sink.get.write(datum)
          sink.get.newLine
        } else if (printData)
					println(datum)
				n -= 1
			}
		}
		return offset
	}

  def make_consumer(): SimpleConsumer = {
    val brokers = ClientUtils.parseBrokerList(options.bootstrap)
    val topicMeta = ClientUtils.fetchTopicMetadata(Set(options.outputTopic),
                                brokers, CLIENT_ID, 3000).topicsMetadata
    assert(topicMeta.size == 1)
    assert(topicMeta.head.topic.equals(options.outputTopic))
    val partMeta = topicMeta.head.partitionsMetadata
    val partMetaOpt = partMeta.find(p => p.partitionId == 0)
    if (partMetaOpt.isEmpty) {
      println(s"${options.outputTopic}:0 does not exist yet [retry in 1 sec]")
      Thread.sleep(1000)
      return make_consumer()
    }
    val leaderOpt = partMetaOpt.get.leader
    if (leaderOpt.isEmpty) {
      println(s"Leader for ${options.outputTopic}:0 not elected yet [retry in 1 sec]")
      Thread.sleep(1000)
      return make_consumer()
    }
      
    val hp = leaderOpt.get
    if (options.verbose)
      println(s"Consumer connects to ${hp.host}:${hp.port}")
    return new SimpleConsumer(hp.host, hp.port, 3000, 65536, CLIENT_ID)
  }

  def make_producer(): KafkaProducer[String,String] = {
    val props = new Properties
    props.put("bootstrap.servers", options.bootstrap)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    return new KafkaProducer[String,String](props)
  }
}

