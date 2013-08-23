package com.mycompany.kevinbacon.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.sound.sampled.LineEvent;

public class DataSampling {

  private static int numRecords = 20166343;
  
  public static void main(String[] args) throws Exception {
    Random random = new Random();
    List<Integer> recnums = new ArrayList<Integer>();
    for (int i = 0; i < 100; i++) {
      recnums.add(Math.round(random.nextFloat() * numRecords));
    }
    Collections.sort(recnums);
    BufferedReader reader = new BufferedReader(
      new FileReader(new File("data/input/actor-movie.csv")));
    PrintWriter writer = new PrintWriter(
      new FileWriter(new File("data/input/actor-movie-mini.csv")), 
      true);
    String line;
    int lno = 0;
    int i = 0;
    boolean foundKb = false;
    while ((line = reader.readLine()) != null) {
      if (lno < recnums.get(i)) {
        // add a few random Kevin Bacon lines to seed the process
        if ((! foundKb) && (line.startsWith("Bacon, Kevin"))) {
          writer.println(line);
          foundKb = true;
        }
        lno++;
        continue;
      }
      writer.println(line);
      lno++;
      i++;
      if (i >= recnums.size()) break;
    }
    writer.flush();
    writer.close();
    reader.close();
  }
}
