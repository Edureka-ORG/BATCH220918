package com.edureka

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object NetworkWordCount {
  
  def main(args: Array[String]): Unit = 
  {
    
    val sparkConf = new SparkConf().setAppName("Network-Word-Count").setMaster("local[*]").set("spark.submit.deployMode","client");
    
    val sc = new SparkContext(sparkConf);
    
    val ssc = new StreamingContext(sc,Seconds(5));
    
    val inputDStream = ssc.socketTextStream("localhost", 44444, StorageLevel.MEMORY_AND_DISK);
    
    val textStream = ssc.textFileStream("")
    
    val words = inputDStream.flatMap(iLine => iLine.split(" "));
    
    words.countByValue().print();
    
    ssc.start();
    
    ssc.awaitTermination();
    
    
    
    
  }
}