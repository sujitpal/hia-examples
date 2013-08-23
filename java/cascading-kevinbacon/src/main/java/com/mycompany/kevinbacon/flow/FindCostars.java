package com.mycompany.kevinbacon.flow;

import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.InnerJoin;
import cascading.tuple.Fields;

/**
 * This subassembly returns a pipe containing costars at 
 * the next degree of separation. Following functionality
 * is implemented.
 * 
 * (1) Join with original pipe of (actor, movie) tuples and
 *     the pipe containing actors found in previous step
 *     to find all movies acted in by the actors.
 * (2) Dedup the movies pipe.
 * (3) Join with original pipe of (actor, movie) tuples and
 *     the movies pipe to find all costars of the actors.
 * (4) Dedup the actors pipe.
 * (5) Add a new column with the current Kevin Bacon number
 *     (degree of separation).
 */
public class FindCostars extends SubAssembly {

  private static final long serialVersionUID = 3450219986636439710L;

  private Fields movieResultFields = 
    new Fields("actor", "movie", "actor1");
  private Fields actorResultFields = 
    new Fields("actor", "movie", "movie1");

  public FindCostars(Pipe allPairs, Pipe actors, int kbNumber) {
    // join with original pipe on actor to produce pipe of
    // all movies acted on by the actors in pipe actor
    actors = new Retain(actors, Constants.actorField);
    Pipe movies = new HashJoin(
      allPairs, Constants.actorField, 
      actors, Constants.actorField,
      movieResultFields, new InnerJoin());
    movies = new Retain(movies, Constants.movieField);
    movies = new Unique(movies, Constants.movieField);
    // now find all the actors for these movies, these
    // will be the costars for the incoming actors in 
    // actorPipe. Finally insert the Bacon number for
    // costars at this degree of separation.
    Pipe costars = new HashJoin(
      allPairs, Constants.movieField, 
      movies, Constants.movieField, 
      actorResultFields, new InnerJoin());
    costars = new Retain(costars, Constants.actorField);
    costars = new Unique(costars, Constants.actorField);
    Insert insfun = new Insert(Constants.kbnumField, kbNumber);
    costars = new Each(costars, insfun, Constants.detailFields);
    setTails(costars);
  }
}
