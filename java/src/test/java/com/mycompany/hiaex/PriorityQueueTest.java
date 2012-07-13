package com.mycompany.hiaex;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.junit.Test;

/**
 * Test for PriorityQueue API.
 */
public class PriorityQueueTest {

  private class Pair {
    public String text;
    public Integer count;
    public Pair(String text, Integer count) {
      this.text = text;
      this.count = count;
    }
  }
  
  @Test
  public void testOperations() throws Exception {
    PriorityQueue<Pair> queue = new PriorityQueue<Pair>(5, new Comparator<Pair>() {
      public int compare(Pair p1, Pair p2) {
        return p1.count.compareTo(p2.count);
      }
    });
    queue.add(new Pair("foo", 10));
    queue.add(new Pair("bar", 9));
    queue.add(new Pair("baz", 11));
    queue.add(new Pair("barf", 7));
    queue.add(new Pair("garf", 3));
    queue.add(new Pair("glub", 15));
    while (! queue.isEmpty()) {
      Pair rp = queue.remove();
      System.out.println(rp.text + " " + rp.count);
    }
  }
}
