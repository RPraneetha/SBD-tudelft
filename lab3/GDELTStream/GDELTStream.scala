package GDELTStream

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores

import scala.collection.JavaConversions._

object GDELTStream extends App {
  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-server:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally,
  // write the result to a new topic called gdelt-histogram.
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

  // extract (key, topic) pairs from the data
  val allNames: KStream[String, String] = records.mapValues((key, line) => line.split("\t"))
    .filter((key, line) => line.length > 23 && line(23) != "")
    .flatMapValues((key, col) => col(23).split(",[0-9;]+").filter(!_.contains("ParentCategory")))

  // create persistent KeyValueStore
  val countStoreSupplier : StoreBuilder[KeyValueStore[String,Long]] =
    Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("allNames_count"), Serdes.String, Serdes.Long)
      .withLoggingDisabled()
  val countStore = countStoreSupplier.build

  builder.addStateStore(countStoreSupplier)

  // create and apply the transformer
  val histTransformer: Transformer[String, String, (String, Long)] = new HistogramTransformer

  // if this line causes a compilation error, it's trying to use the Scala classes instead of the Java ones (?)
  // replace it with the line below it to use the proper Scala-compatible version instead!
  val transformer: KStream[String, Long] = allNames.transform(getSupplier(histTransformer), "allNames_count")
  /*
  val transformer: KStream[String, Long] = allNames.transform(histTransformer, "allNames_count")
  */
  transformer.to("gdelt-histogram")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

  /**
    * Adjusted from the Kafka KStream scala source code
    */
  def getSupplier(transformer: Transformer[String, String, (String, Long)]): TransformerSupplier[String, String, KeyValue[String, Long]] = {
    new TransformerSupplier[String, String, KeyValue[String, Long]] {
      override def get(): Transformer[String, String, KeyValue[String, Long]] = {
        new Transformer[String, String, KeyValue[String, Long]] {
          override def transform(key: String, value: String): KeyValue[String, Long] = {
            transformer.transform(key, value) match {
              case (k1, v1) => KeyValue.pair(k1, v1)
              case _ => null
            }
          }

          override def init(context: ProcessorContext): Unit = transformer.init(context)

          override def close(): Unit = transformer.close()
        }
      }
    }
  }

}

// This transformer should count the number of times a name occurs
// during the last hour. This means it needs to be able to
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, (String, Long)] {
  var context: ProcessorContext = _
  var keyValueStore : KeyValueStore[String, Long] = _

  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context
    this.keyValueStore = context.getStateStore("allNames_count").asInstanceOf[KeyValueStore[String, Long]]
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): (String, Long) = {
    //  1. Add a new record to the histogram and initialize its count;
    keyValueStore.putIfAbsent(name, 0L)

    //  2. Change the count for a record if it occurs again; and
    val newCount = keyValueStore.get(name) + 1
    keyValueStore.put(name, newCount)

    //  3. Decrement the count of a record an hour later.
    context.schedule(1000 * 60 * 60L, PunctuationType.WALL_CLOCK_TIME, new Punctuator {
      // make sure it can't go into the negatives
      def punctuate(timestamp: Long): Unit = keyValueStore.put(name, Math.max(0, keyValueStore.get(name) - 1))
    })
    
    (name, newCount)
  }

  // Close any resources if any
  def close() {}
}
