package com.mycompany.pigudfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Expects a Bag of Tuples. Reorders the Tuples by count
 * so frequently occuring Tuples occur in the Bag first.
 * Examples:
 * {(100,200),(100,300),(100,200)}) =>
 *   {(100,200),(100,300)}
 */
public class OrderByCountDesc extends EvalFunc<DataBag> {

  private BagFactory factory = BagFactory.getInstance();
  
  @Override
  public DataBag exec(Tuple input) throws IOException {
    Object value = input.get(0);
    if (value instanceof DataBag) {
      DataBag ibag = (DataBag) value;
      // deduplicate and count tuples in the bag
      final Map<Tuple,Integer> tupleMap = new HashMap<Tuple,Integer>();
      for (Iterator<Tuple> it = ibag.iterator(); it.hasNext(); ) {
        Tuple tuple = it.next();
        if (tupleMap.containsKey(tuple)) {
          Integer cnt = tupleMap.get(tuple) + 1;
          tupleMap.put(tuple, cnt);
        } else {
          tupleMap.put(tuple, new Integer(1));
        }
      }
      // sort the Tuples by count
      List<Tuple> tuples = new ArrayList<Tuple>();
      tuples.addAll(tupleMap.keySet());
      Collections.sort(tuples, new Comparator<Tuple>() {
        @Override
        public int compare(Tuple t1, Tuple t2) {
          Integer cnt1 = tupleMap.get(t1);
          Integer cnt2 = tupleMap.get(t2);
          return cnt2.compareTo(cnt1);
        }});
      // return it
      return factory.newDefaultBag(tuples);
    } else {
      throw new ExecException("Input must be a Bag of Tuples");
    }
  }
  
  @Override
  public Schema outputSchema(Schema input) {
    try {
      Schema.FieldSchema leftElementFs = 
        new Schema.FieldSchema("mid_l", DataType.INTEGER);
      Schema.FieldSchema rightElementFs = 
        new Schema.FieldSchema("mid_r", DataType.INTEGER);
      Schema tupleSchema = new Schema(
        Arrays.asList(leftElementFs, rightElementFs));
      Schema.FieldSchema tupleFs = 
        new Schema.FieldSchema("tuple", tupleSchema, 
        DataType.TUPLE);
      Schema bagSchema = new Schema(tupleFs);
      Schema.FieldSchema bagFs = 
        new Schema.FieldSchema("mid_pairs", bagSchema, 
        DataType.BAG);
      return new Schema(bagFs);
    } catch (Exception e) {
      return null;
    }
  }
}
