package com.edureka.listener

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerTaskGettingResult
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.scheduler.SparkListenerUnpersistRDD
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate
import org.apache.spark.scheduler.SparkListenerTaskStart
import org.apache.spark.scheduler.SparkListenerExecutorRemoved
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerBlockUpdated
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerExecutorAdded
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded
import org.apache.spark.util.JsonProtocolWrapper
import org.apache.spark.SparkContext
import org.apache.kafka.clients.producer.KafkaProducer
import com.edureka.producer.ProducerUtil

 import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class MySparkListener extends SparkListener {

  var producer: KafkaProducer[String, String] = null;
  
  val topic = "FLUME-BATCH220918-TOPIC-1";
  
  private def publishEventInternal(event: SparkListenerEvent) 
  {
    var appId = SparkContext.getOrCreate().applicationId
    val eventJson = JsonProtocolWrapper.sparkEventToJson(event);
    
    val completeJson = 
      ("timestamp" -> System.currentTimeMillis()) ~
      ("application-id" -> appId) ~
      ("spark-event" -> eventJson)
      
    val publishJson = compact(render(completeJson));  
    producer = ProducerUtil.getProducer();
    
    ProducerUtil.send(producer, topic, appId,publishJson.toString());
    

  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    publishEventInternal(event);
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    publishEventInternal(event);
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    publishEventInternal(event);
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    publishEventInternal(event);
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    publishEventInternal(event);
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    publishEventInternal(event);
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    publishEventInternal(event);
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    publishEventInternal(event);
    ProducerUtil.close(producer);
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    publishEventInternal(event);
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    publishEventInternal(event);
  }

}