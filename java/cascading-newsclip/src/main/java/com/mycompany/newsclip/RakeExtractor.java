package com.mycompany.newsclip;

import java.io.File;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.Bag;
import org.apache.commons.collections15.bag.HashBag;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NumberUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java implementation of the RAKE algorithm, based on a
 * Python/NLTK version I did previously.
 */
public class RakeExtractor {

  private static final String STOPWORD_INDICATOR = "__";
  private static final Logger LOGGER = LoggerFactory.getLogger(RakeExtractor.class);
  
  public static final RakeExtractor INSTANCE = new RakeExtractor();
  protected Set<String> stopwords;
  
  private RakeExtractor() {
    stopwords = new HashSet<String>();
    try {
      String stopText = FileUtils.readFileToString(
          new File("src/main/resources/stopwords.txt"));
      for (String word : stopText.split("\n")) {
        stopwords.add(word);
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
  }
  
  public List<String> extract(String text) {
    List<String> sentences = parseSentences(text);
    List<List<String>> phrases = generateCandidatePhrases(sentences);
    Map<String,Float> wordScores = calculateWordScores(phrases);
    final Map<String,Float> phraseScores = calculatePhraseScores(
      phrases, wordScores);
    List<String> ophrases = new ArrayList<String>();
    ophrases.addAll(phraseScores.keySet());
    Collections.sort(ophrases, new Comparator<String>() {
      @Override
      public int compare(String phrase1, String phrase2) {
        return phraseScores.get(phrase2).compareTo(
          phraseScores.get(phrase1));
      }
    });
    int numPhrases = ophrases.size();
    return ophrases.subList(0, numPhrases/3);
  }

  protected List<String> parseSentences(String text) {
    List<String> sentences = new ArrayList<String>();
    if (StringUtils.isNotEmpty(text)) {
      BreakIterator sit = BreakIterator.getSentenceInstance();
      sit.setText(text);
      int index = 0;
      while (sit.next() != BreakIterator.DONE) {
        sentences.add(text.substring(index, sit.current()));
        index = sit.current();
      }
    }
    return sentences;
  }

  protected List<List<String>> generateCandidatePhrases(
      List<String> sentences) {
    List<List<String>> phrases = new ArrayList<List<String>>();
    for (String sentence : sentences) {
      List<String> owords = new ArrayList<String>();
      List<String> words = parseWords(
        StringUtils.lowerCase(sentence));
      for (String word : words) {
        if (stopwords.contains(word)) {
          owords.add(STOPWORD_INDICATOR);
        } else {
          owords.add(word);
        }
      }
      List<String> phrase = new ArrayList<String>();
      for (String word : owords) {
        if (STOPWORD_INDICATOR.equals(word) || 
            word.matches("\\p{Punct}")) {
          phrases.add(phrase);
          phrase = new ArrayList<String>();
        } else {
          phrase.add(word);
        }
      }
    }
    return phrases;
  }

  protected List<String> parseWords(String sentence) {
    List<String> words = new ArrayList<String>();
    BreakIterator wit = BreakIterator.getWordInstance();
    wit.setText(sentence);
    int index = 0;
    while (wit.next() != BreakIterator.DONE) {
      String word = sentence.substring(index, wit.current());
      if (StringUtils.isNotBlank(word)) {
        words.add(word);
      }
      index = wit.current();
    }
    return words;
  }

  @SuppressWarnings("deprecation")
  protected Map<String,Float> calculateWordScores(
      List<List<String>> phrases) {
    Bag<String> wordFreq = new HashBag<String>();
    Bag<String> wordDegree = new HashBag<String>();
    for (List<String> phrase : phrases) {
      int degree = -1;
      for (String word : phrase) {
        if (NumberUtils.isNumber(word)) continue;
        else degree++;
      }
      for (String word : phrase) {
        wordFreq.add(word);
        wordDegree.add(word, degree); // other words
      }
    }
    for (String word : wordFreq.uniqueSet()) {
      wordDegree.add(word, wordFreq.getCount(word)); // itself
    }
    Map<String,Float> wordScores = new HashMap<String,Float>();
    for (String word : wordFreq.uniqueSet()) {
      float score = (float) wordDegree.getCount(word) / 
        (float) wordFreq.getCount(word);
      wordScores.put(word, score);
    }
    return wordScores;
  }

  protected Map<String, Float> calculatePhraseScores(
      List<List<String>> phrases,
      Map<String,Float> wordScores) {
    Map<String,Float> phraseScores = new HashMap<String,Float>();
    for (List<String> phrase : phrases) {
      float phraseScore = 0.0F;
      for (String word : phrase) {
        if (wordScores.containsKey(word)) {
          phraseScore += wordScores.get(word);
        }
      }
      phraseScores.put(StringUtils.join(phrase, " "), phraseScore);
    }
    return phraseScores;
  }
}
