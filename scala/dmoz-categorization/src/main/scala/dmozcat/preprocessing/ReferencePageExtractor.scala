package dmozcat.preprocessing

import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.net.URL
import scala.collection.mutable.ArrayBuffer
import org.xml.sax.Attributes
import org.xml.sax.helpers.DefaultHandler
import de.l3s.boilerpipe.extractors.ArticleExtractor
import javax.xml.parsers.SAXParserFactory
import java.util.concurrent.FutureTask
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

object ReferencePageExtractor extends App {
    val dataDir = "data"
    val structXml = new File(dataDir, "structure.rdf.u8")
    val contentXml = new File(dataDir, "content.rdf.u8")
    val outputCsv = new File(dataDir, "ref-pages.csv")
    val extractor = new ReferencePageExtractor(structXml, contentXml, outputCsv)
    extractor.extract()
}

class ReferencePageExtractor(structXml: File, contentXml: File, outputCsv: File) {
    
    def extract(): Unit = {
        val factory = SAXParserFactory.newInstance()
        // parse structure RDF to get list of topics
        val structParser = factory.newSAXParser()
        val structHandler = new DmozStructHandler()
        structParser.parse(structXml, structHandler)
        val topicSet = structHandler.topicSet()
        // parse content RDF to get list of URLs for each topic
        val contentParser = factory.newSAXParser()
        val contentHandler = new DmozContentHandler(topicSet)
        contentParser.parse(contentXml, contentHandler)
        val contentUrls = contentHandler.contentUrls()
        // download pages and write to file
        val writer = new PrintWriter(new FileWriter(outputCsv), true)
        contentUrls.foreach(topicUrl => {
            val topic = topicUrl._1
            val url = topicUrl._2
            val downloadTask = new FutureTask(new Callable[String]() {
                def call(): String = {
                    try {
                        val text = ArticleExtractor.INSTANCE
                            .getText(new URL(url))
                            .replaceAll("\n", " ")
                        Console.println("Downloading %s for topic: %s"
                            .format(url, topic))
                        text
                    } catch {
                        case e: Exception => "__ERROR__"
                    }
                }
            })
            new Thread(downloadTask).start()
            try {
                val text = downloadTask.get(60, TimeUnit.SECONDS)
                if (!text.equals("__ERROR__")) {
                    writer.println("%s\t%s\t%s".format(topic, url, text))
                } else {
                    Console.println("Download Error, skipping")
                }
            } catch {
                case e: TimeoutException => Console.println("Timed out, skipping")
            }
        })
        writer.flush()
        writer.close()
    }
}

class DmozStructHandler extends DefaultHandler {
    
    val contentTopics = Set("narrow", "symbolic")
    
    var isRelevant = false
    val topics = ArrayBuffer[String]()
    
    def topicSet(): Set[String] = topics.toSet
    
    override def startElement(uri: String, localName: String, 
            qName: String, attrs: Attributes): Unit = {
        if (!isRelevant && qName.equals("Topic")) {
            val numAttrs = attrs.getLength()
            val topicName = (0 until numAttrs)
                .filter(i => attrs.getQName(i).equals("r:id"))
                .map(i => attrs.getValue(i))
                .head
            if (topicName.equals("Top/Health/Medicine/Medical_Specialties"))
                isRelevant = true
        }
        if (isRelevant && contentTopics.contains(qName)) {
            val numAttrs = attrs.getLength()
            val contentTopicName = (0 until numAttrs)
                .filter(i => attrs.getQName(i).equals("r:resource"))
                .map(i => attrs.getValue(i))
                .map(v => if (v.indexOf(':') > -1)
                    v.substring(v.indexOf(':') + 1) else v)
                .head
            topics += contentTopicName
        }
    }
    
    override def endElement(uri: String, localName: String, 
            qName: String): Unit = {
        if (isRelevant && qName.equals("Topic")) isRelevant = false
    }
}

class DmozContentHandler(topics: Set[String]) extends DefaultHandler {
    
    var isRelevant = false
    var currentTopicName: String = null
    val contents = ArrayBuffer[(String, String)]()
    
    def contentUrls(): List[(String, String)] = contents.toList
    
    override def startElement(uri: String, localName: String, 
            qName: String, attrs: Attributes): Unit = {
        if (!isRelevant && qName.equals("Topic")) {
            val numAttrs = attrs.getLength()
            val topicName = (0 until numAttrs)
                .filter(i => attrs.getQName(i).equals("r:id"))
                .map(i => attrs.getValue(i))
                .head
            if (topics.contains(topicName)) {
                isRelevant = true
                currentTopicName = topicName
            }
        }
        if (isRelevant && qName.equals("link")) {
            val numAttrs = attrs.getLength()
            val link = (0 until numAttrs)
                .filter(i => attrs.getQName(i).equals("r:resource"))
                .map(i => attrs.getValue(i))
                .head
            contents += ((currentTopicName, link))
        }
    }
    
    override def endElement(uri: String, localName: String, 
            qName: String): Unit = {
        if (isRelevant && qName.equals("Topic")) {
            isRelevant = false
            currentTopicName = null
        }
    }
}
