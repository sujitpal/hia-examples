package dmozcat.vectorize

import edu.knowitall.tool.sentence.OpenNlpSentencer
import edu.knowitall.tool.postag.OpenNlpPostagger
import edu.knowitall.tool.tokenize.OpenNlpTokenizer
import edu.knowitall.tool.chunk.OpenNlpChunker

import scala.collection.mutable.ArrayBuffer

class NGramExtractor {

    val sentencer = new OpenNlpSentencer
    val postagger = new OpenNlpPostagger
    val tokenizer = new OpenNlpTokenizer
    val chunker = new OpenNlpChunker
    
    def ngrams(keyText: (String, String)): List[(String, String)] = {
        val key = keyText._1
        val text = keyText._2
        // segment text into sentences
        val sentences = sentencer.segment(text)
        // extract noun phrases from sentences
        val nounPhrases = sentences.flatMap(segment => {
            val    sentence = segment.text
            val chunks = chunker.chunk(sentence)
            chunks.filter(chunk => chunk.chunk.endsWith("-NP"))
                .map(chunk => (chunk.string, chunk.chunk))
                .foldLeft(List.empty[String])((acc, x) => x match {
                    case (s, "B-NP") => s :: acc
                    case (s, "I-NP") => acc.head + " " + s :: acc.tail
                }).reverse
        })
        // extract ngrams (n=1,2,3) from noun phrases
        val ngrams = nounPhrases.flatMap(nounPhrase => {
            val words = nounPhrase.toLowerCase.split(" ")
            words.size match {
                case 0 => List()
                case 1 => words
                case 2 => words ++ words.sliding(2).map(_.mkString("_"))
                case _ => words ++ 
                    words.sliding(2).map(_.mkString("_")) ++
                    words.sliding(3).map(_.mkString("_"))
            }
        })
        ngrams.map(ngram => (key, ngram)).toList
    }
}
