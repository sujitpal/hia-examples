package com.mycompany.kevinbacon.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.util.StringUtils;

public class Imdb2Csv {
  
  public static void main(String[] args) {
    
    try {
      Pattern actorPattern = Pattern.compile(
        "(.*?)\\s\\(.*?\\)"); 
      Pattern moviePattern = 
        Pattern.compile("(.*?)\\s\\(\\d{4}\\).*$");
      String[] inputs = new String[] {
        "data/landing/actors.list",
        "data/landing/actresses.list"
      };
      PrintWriter output = new PrintWriter(
        new FileWriter("data/input/actor-movie.csv"), true);
      for (String input : inputs) {
        boolean header = true;
        boolean data = false;
        boolean footer = false;
        String actor = null;
        String movie = null;
        BufferedReader reader = new BufferedReader(
          new FileReader(new File(input)));
        String line = null;
        while ((line = reader.readLine()) != null) {
          // loop through lines until we hit this pattern
          // Name\tTitles
          // ----\t-------
          if (line.startsWith("----\t")) header = false;
          // skip the footer, it occurs after a long 40 dash
          // or so standalone line (pattern below works if
          // you are already in the data area).
          if (data && line.startsWith("--------------------")) 
            footer = true;
          if (! header && ! footer) {
            data = true;
            if (line.trim().length() > 0 && ! line.startsWith("----\t")) {
              String[] cols = line.replaceAll("\t+", "\t").split("\t");
              if (! line.startsWith("\t")) {
                Matcher ma = actorPattern.matcher(cols[0]);
                if (ma.matches()) actor = ma.group(1);
                Matcher mm = moviePattern.matcher(cols[1]);
                if (mm.matches()) movie = mm.group(1);
              } else {
                Matcher mm = moviePattern.matcher(cols[1]);
                if (mm.matches()) movie = mm.group(1);
              }
              // if line contains non-ascii chars, skip this line
              // the reasoning is that this is perhaps non-English
              // movie which we don't care about.
              if (isNonAscii(actor) || isNonAscii(movie)) continue;
              if (actor != null && movie != null)
                output.println(dequote(actor) + "\t" + dequote(movie));
            }
          }
        }
        reader.close();
      }
      output.flush();
      output.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private static boolean isNonAscii(String s) {
    if (s == null) return true;
    char[] cs = s.toCharArray();
    for (int i = 0; i < cs.length; i++) {
      if (cs[i] > 127) return true;
    }
    return false;
  }

  private static String dequote(String s) {
    String st = s.trim();
    if (st.startsWith("\"") && st.endsWith("\"")) 
      return st.substring(1, st.length()-1);
    else return st;
  }
}
