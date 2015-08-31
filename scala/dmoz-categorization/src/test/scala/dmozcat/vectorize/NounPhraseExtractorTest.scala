package dmozcat.vectorize

import org.junit.Test
import org.junit.Assert

class NounPhraseExtractorTest {

    @Test
    def testSegmentation(): Unit = {
        val t2n = new NGramExtractor
        val ngrams = t2n.ngrams(("foo", "I am Sam. Sam I am. I like green eggs and ham."))
        ngrams.foreach(kn => Console.println(kn._2))
        Assert.assertTrue(ngrams.size == 14)
    }
    
//    @Test
//    def testMultiSplit(): Unit = {
//        val xs = List(("I", "B-NP"), ("green", "B-NP"), ("eggs", "I-NP"), 
//                ("and", "I-NP"), ("ham", "I-NP"), ("Sam", "B-NP"))
//        val fxs = xs.foldLeft(List.empty[String])((acc, x) => x match {
//            case (s, "B-NP") => s :: acc
//            case (s, "I-NP") => List(acc.head, s).mkString(" ") :: acc.tail
//        }) 
//        Console.println(fxs.reverse)
//    }
}
