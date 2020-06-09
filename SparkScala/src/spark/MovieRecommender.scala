package spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt

object MovieRecommender {
  
  def loadMovieNames(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8");
    codec.onMalformedInput(CodingErrorAction.REPLACE);
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE);
    
    val lines = Source.fromFile("../ml-100k/u.item").getLines();
    val movieNames = lines
    .map(_.split('|'))
    .map(fields => (fields(0).toInt, fields(1)))
    .toMap;
    
    movieNames;
  }
  
  type MovieRating = (Int, Double);
  type UserRatings = (Int, (MovieRating, MovieRating));
  
  def filterDuplicates(userRatings: UserRatings): Boolean = {
    val movie1 = userRatings._2._1;
    val movie2 = userRatings._2._2;
    
    val id1 = movie1._1;
    val id2 = movie2._1;
    
    id1 < id2;
  }
  
  def makePairs(userRatings: UserRatings) = {
    val movie1 = userRatings._2._1;
    val movie2 = userRatings._2._2;
    
    val movie1ID = movie1._1;
    val movie2ID = movie2._1;
    val rating1 = movie1._2;
    val rating2 = movie2._2;
    
    ((movie1ID, movie2ID), (rating1, rating2));
  }
   
  type PairRatings = Iterable[(Double, Double)];
  
  // similarity = cos(θ) = A . B / ||A||||B||
  // more info on https://en.wikipedia.org/wiki/Cosine_similarity
  def cosineSimilarity(pairRatings: PairRatings): (Double, Int) = {
    val mappedRatings = pairRatings.map(x => (x._1 * x._1, x._2 * x._2, x._1 * x._2));
    val summationCosineSimilarity = mappedRatings.fold((0.0, 0.0, 0.0))((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3));
    
    val (sumA, sumB, numerator) = summationCosineSimilarity;
    val magnitudes = sqrt(sumA) * sqrt(sumB);
    val ocurrences = pairRatings.size;
    
    val cosineSimilarity = if(ocurrences > 0) (numerator / magnitudes, ocurrences) else (0.0, 0);
    
    cosineSimilarity;
  }
  
  type Score = (Double, Int);
  type Movies = (Int, Int);
  type PairMovies = (Movies, Score);
  
  def filterChosenMovie(movies: PairMovies, movieID: Int): Boolean = {
    val (movieID1, movieID2) = movies._1;
    movieID1 == movieID || movieID2 == movieID;
  }
  
  def pickMovie(movies: PairMovies, movieID: Int): (Int, Score) = {
    val (movieID1, movieID2) = movies._1;
    val movID = if(movieID1 == movieID) movieID2 else movieID1;
    (movID, movies._2)
  }
  
  type MovieWithGenres = (Int, (Score, Array[Int]));
  type MovieCountGenres = (Int, (Score, Array[Int]));
  
  def sumGenres(movie: MovieWithGenres, selectedMovieIdGenres: Array[Int]): MovieCountGenres = {
    val actualGenres = movie._2._2;
    val sumGenres = selectedMovieIdGenres
    .zip(actualGenres)
    .map(x => x._1 + x._2)
      
    (movie._1,((movie._2._1._1, movie._2._1._2), sumGenres))
  }
  
  type GenreScore = (Int, (Double, Int, Double));
  
  def genreScore(movie: MovieCountGenres, selectedMovieIdGenres: Array[Int]): GenreScore = {
    val countSimilarGenres = movie._2._2.count(c => c == 2).toDouble;
    val genreScore = countSimilarGenres / selectedMovieIdGenres.count(c => c == 1);
    
    (movie._1,(movie._2._1._1, movie._2._1._2, genreScore))
  }
  
  
  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR);
    
    val movieNames = loadMovieNames();
        
    val sc = new SparkContext("local[*]", "MovieRecommender");
    
    val data = sc.textFile("../ml-100k/u.data");
    
    // userID => (movie ID, rating)
    val ratings = data
    .map(_.split("\t"))
    .map(x => (x(0).toInt, (x(1).toInt, x(2).toDouble)));
    
    // Self-join to find every combination by the same user
    // userID => ((movieID, rating), (movieID, rating))
    val joinedRatings = ratings.join(ratings);

    // Filter out duplicate pairs like (A, B) == (B, A) and also (A, A)
    val filteredRatings = joinedRatings.filter(filterDuplicates); 

    // (movie1, movie2) => (rating1, rating2)
    val pairedMovies = filteredRatings.map(makePairs);

    // (movie1, movie2) => (rating1, rating2), (rating1, rating2) ...
    val groupByMoviePair = pairedMovies.groupByKey();

    // Compute similarities
    // (movie1, movie2) => (score, ocurrences)
    val allSimilarities = groupByMoviePair.mapValues(cosineSimilarity).cache();
      
    
    // Filter movies and get good data based on input thresholds
    // Get args data (movieID, thresholdRating, thresholdOcurrences)
    val movieID = args(0).toInt;
    val thresholdRating = args(1).toDouble;
    val thresholdOcurrences = args(2).toDouble;

    // Filter movies which are not paired with chosen one and pick the other
    // (movie) => (score, ocurrences)
    val recomendations = allSimilarities
    .filter(filterChosenMovie(_, movieID))
    .map(pickMovie(_, movieID));
    
    // Load genres
    val item = sc.textFile("../ml-100k/u.item");
    
    // (movie) => (genres:Array[Int])
    val genres = item
    .map(_.split('|'))
    .map(x => (x(0).toInt, x.drop(5).map(_.toInt)));
     
    // Get genres of selected movie
    val selectedMovieIdGenres = genres.filter(x => x._1 == args(0).toInt).map(x => x._2).take(1).flatten;
    
    // Join genres
    // (movie) => (score, ocurrences, genres: Array[Int]))
    val joinGenres = recomendations.join(genres);
    
    // Add genre similarity score
    // (movie) => (score, genreScore, ocurrences)
    val recommendationsWithGenres = joinGenres
    .map(sumGenres(_, selectedMovieIdGenres))
    .filter(f => f._2._2.contains(2))
    .map(genreScore(_, selectedMovieIdGenres));
    
    // Filter by thresholds, sort and collect
    val goodRecomendations = recommendationsWithGenres
    .filter(x => x._2._1 > thresholdRating && x._2._2 > thresholdOcurrences)
    .sortBy(x => (x._2._3, x._2._1, x._2._2), false)
    .take(20);
    
    // Show recommended movies
    println("Recommendations for " + movieNames(movieID));
    for(r <- goodRecomendations){
      println(movieNames(r._1) + "\tscore: " + r._2._1 + "\tgenre: " + r._2._3 + "\tstrenght: " + r._2._2);
    }
    
    println("End");
  }
  
}