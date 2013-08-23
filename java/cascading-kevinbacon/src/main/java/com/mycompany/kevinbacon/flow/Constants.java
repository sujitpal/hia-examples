package com.mycompany.kevinbacon.flow;

import cascading.tuple.Fields;

public interface Constants {

  public Fields actorField = new Fields("actor");
  public Fields movieField = new Fields("movie");
  public Fields kbnumField = new Fields("kbnum");
  public Fields countField = new Fields("count");
  public Fields inputFields = new Fields("actor", "movie");
  public Fields detailFields = new Fields("actor", "kbnum");
  public Fields summaryFields = new Fields("kbnum", "count");
  
}
