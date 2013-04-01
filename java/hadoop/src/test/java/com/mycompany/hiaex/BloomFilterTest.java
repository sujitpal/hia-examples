package com.mycompany.hiaex;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test bloomfilter API.
 */
public class BloomFilterTest {

  @Test
  public void testCreateSetGet() throws Exception {
    BloomFilter bf = new BloomFilter(15000, 2, Hash.MURMUR_HASH);
    bf.add(new Key("larry".getBytes()));
    bf.add(new Key("curly".getBytes()));
    bf.add(new Key("moe".getBytes()));
    Assert.assertTrue(bf.membershipTest(new Key("larry".getBytes())));
    Assert.assertFalse(bf.membershipTest(new Key("batman".getBytes())));
  }
  
  @Test
  public void testOrMultiple() throws Exception {
    BloomFilter bf1 = new BloomFilter(15000, 2, Hash.MURMUR_HASH);
    bf1.add(new Key("larry".getBytes()));
    bf1.add(new Key("curly".getBytes()));
    bf1.add(new Key("moe".getBytes()));
    BloomFilter bf2 = new BloomFilter(15000, 2, Hash.MURMUR_HASH);
    bf2.add(new Key("batman".getBytes()));
    bf2.add(new Key("robin".getBytes()));
    bf2.add(new Key("joker".getBytes()));
    BloomFilter bf3 = new BloomFilter(15000, 2, Hash.MURMUR_HASH);
    bf3.or(bf1);
    bf3.or(bf2);
    Assert.assertTrue(bf3.membershipTest(new Key("larry".getBytes())));
    Assert.assertTrue(bf3.membershipTest(new Key("batman".getBytes())));
    Assert.assertFalse(bf3.membershipTest(new Key("spiderman".getBytes())));
  }
  
  @Test
  public void testSerDeser() throws Exception {
    // build bloom filter and persist to disk
    BloomFilter bf1 = new BloomFilter(15000, 2, Hash.MURMUR_HASH);
    bf1.add(new Key("larry".getBytes()));
    bf1.add(new Key("curly".getBytes()));
    bf1.add(new Key("moe".getBytes()));
    BloomFilter bf2 = new BloomFilter(15000, 2, Hash.MURMUR_HASH);
    bf2.add(new Key("batman".getBytes()));
    bf2.add(new Key("robin".getBytes()));
    bf2.add(new Key("joker".getBytes()));
    BloomFilter bf3 = new BloomFilter(15000, 2, Hash.MURMUR_HASH);
    bf3.or(bf1);
    bf3.or(bf2);
    File bloomfile = new File("/tmp/bloomfilter.ser");
    bf3.write(new DataOutputStream(new FileOutputStream(bloomfile)));
    // read this back
    BloomFilter bf4 = new BloomFilter(15000, 2, Hash.MURMUR_HASH);
    bf4.readFields(new DataInputStream(new FileInputStream(bloomfile)));
    Assert.assertTrue(bf4.membershipTest(new Key("larry".getBytes())));
    Assert.assertTrue(bf4.membershipTest(new Key("batman".getBytes())));
    Assert.assertFalse(bf4.membershipTest(new Key("spiderman".getBytes())));
    bloomfile.delete();
  }
}
