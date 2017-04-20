package nlp

import java.time.format.DateTimeFormatter

import com.cybozu.labs.langdetect.DetectorFactory
import util._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.util.Try
import java.io.InputStream
import scala.io.Source
import org.apache.spark.serializer.KryoSerializer
import twitter4j.Status
import com.typesafe.config.ConfigFactory
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.SolrInputField
import org.apache.solr.client.solrj.response.{ QueryResponse, UpdateResponse }
import org.apache.solr.client.solrj.SolrServerException
import scala.util.control.Exception.Catch
import akka.actor.ActorSystem

import java.util.logging.Logger
import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.Protos.FrameworkInfo
import javax.xml.transform.Result
import java.util.List
import org.apache.spark.streaming.dstream.DStream

object TwitterSentimentAnalysis {
  
  
    val config = ConfigFactory.load("application.conf")
    val url = config.getString("solr.url")
    val collection_name = config.getString("solr.collection")
    val url_final = url + collection_name

    
    def main(args: Array[String]): Unit = {
 


    DetectorFactory.loadProfile("src/main/resources/profiles")

    val filters = args.takeRight(args.length - 4)

    // system properties for Twitter4j library to get twitter streams
    System.setProperty("twitter4j.oauth.consumerKey", "TWooPFlG2twzYNm34eaUJTaPs")
    System.setProperty("twitter4j.oauth.consumerSecret", "5TbY9OmCm9JLY0GWWpOrKjp5CuODmAXqI4XX3QiI3c7Q7986f3")
    System.setProperty("twitter4j.oauth.accessToken", "843828759342039040-Il2PZGfjwyJZlNbhXRBIUktBxjlcko0")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "X4A6N82uyjZWJpb76w73dcHrtsVM209bWLFzyvTZC5FIv")

    //spark config
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[4]")
      .set("spark.streaming.unpersist", "true")
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      .set("spark.eventLog.enabled", "false")
      .set("spark.executor.memory", "6g")

    val ssc = new StreamingContext(conf, Seconds(1))

    val tweets = TwitterUtils.createStream(ssc, None, filters)

    tweets.map(status => 
    {
    //solr client
    val solrClient = new HttpSolrClient.Builder(url_final).build()
    //solr document
    val sdoc = new SolrInputDocument()
    // get solr fields from status
    val text=status.getText()
    val created_at=status.getCreatedAt
    val userName=status.getUser.getName
    val userLocation=status.getUser.getLocation
    val hashtags=status.getHashtagEntities.map(_.getText)
    val retweetCount=status.getRetweetCount
    val retweetText=Option(status.getRetweetedStatus).map(text => text.getText)
    val language= detectLanguage(text)
    val sentiment=SentimentAnalysisUtils.detectSentiment(text).toString
    val location=Option(status.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" })
   
    //add fields to doc
    sdoc.addField("text", text)
    sdoc.addField("created_at", created_at)
    sdoc.addField("userName", userName)
    sdoc.addField("hashtags", hashtags)
    sdoc.addField("language", language)
    sdoc.addField("sentiment", sentiment)
    sdoc.addField("location", location)
    sdoc.addField("retweet Count", retweetCount)
    sdoc.addField("retweetText", retweetCount)
    sdoc.addField("user_location", userLocation)
      
    //commit updates
    solrClient.add(sdoc)
  
  }).print()
  
  
  
  
   // if we want to save data in files
  
   /*tweets.foreachRDD { (rdd, time) =>
      rdd.map(t => {
        Map("country code"->Option(t.getPlace.getCountryCode))
      }).saveAsTextFile("C:/Users/ea/Desktop/tweets/countryCode")
    }*/

    ssc.start()
    // stop server after nn seconds 
    // use awaitTermination 
    ssc.awaitTerminationOrTimeout(30 * 1000)
  }
  // detect language from profiles
  def detectLanguage(text: String): String = {

    Try {
      val detector = DetectorFactory.create()
      detector.append(text)
      detector.detect()
    }.getOrElse("unknown")

  }
  

}
