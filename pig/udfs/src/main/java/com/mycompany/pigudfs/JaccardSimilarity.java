package com.mycompany.pigudfs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * Takes a Tuple of two Bags of Tuples and computes their 
 * Jaccard Similarity. Jaccard Similarity between two Sets
 * A and B is computed as:
 *  Jaccard(A, B) = intersect(A, B) / union(A, B)
 * Input is a Tuple of Bags of Tuples, something like this:
 * ({(2),(3)},{(3),(4)}),
 * ({(2),(3)},{(5)}),
 * ({(3),(4)},{(5)}), etc.
 */
public class JaccardSimilarity extends EvalFunc<Double> {

  @Override
  public Double exec(Tuple input) throws IOException {
    if (input == null || input.size() != 2) {
      return null;
    }
    try {
      Set<Tuple> s1 = convertToSet((DataBag) input.get(0));
      Set<Tuple> s2 = convertToSet((DataBag) input.get(1));
      Set<Tuple> u = new HashSet<Tuple>();
      u.addAll(s1);
      u.addAll(s2);
      Set<Tuple> i = new HashSet<Tuple>();
      i.addAll(s1);
      i.retainAll(s2);
      return (double) i.size() / (double) u.size();
    } catch (Exception e) {
      System.err.println("Can't process input: " + e.getMessage());
      e.printStackTrace(System.err);
      return null;
    }
  }

  private Set<Tuple> convertToSet(DataBag dataBag) throws Exception {
    Set<Tuple> set = new HashSet<Tuple>();
    for (Iterator<Tuple> it = dataBag.iterator(); it.hasNext(); ) {
      Tuple t = it.next();
      set.add(t);
    }
    return set;
  }
}
