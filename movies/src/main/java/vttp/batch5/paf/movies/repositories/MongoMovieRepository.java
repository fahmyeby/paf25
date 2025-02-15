package vttp.batch5.paf.movies.repositories;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

import jakarta.json.JsonObject;
import vttp.batch5.paf.movies.models.StatsDir;

@Repository
public class MongoMovieRepository {

    @Autowired
    private MongoTemplate mongoTemplate;

    // parse to mongo doc
    private Document toMongoDocument(JsonObject movie) {
      Document doc = new Document();
      doc.put("imdb_id", getStringValue(movie, "imdb_id"));
      doc.put("title", getStringValue(movie, "title"));
      doc.put("director", getStringValue(movie, "director"));
      doc.put("overview", getStringValue(movie, "overview"));
      doc.put("tagline", getStringValue(movie, "tagline"));
      doc.put("genres", getStringValue(movie, "genres"));
      doc.put("imdb_rating", getDoubleValue(movie, "imdb_rating"));
      doc.put("imdb_votes", getIntValue(movie, "imdb_votes"));
      return doc;
  }

  // helper get methods
  private String getStringValue(JsonObject json, String key) {
      return json.containsKey(key) && !json.isNull(key)
              ? json.getString(key)
              : "";
  }

  private double getDoubleValue(JsonObject json, String key) {
      return json.containsKey(key) && !json.isNull(key)
              ? json.getJsonNumber(key).doubleValue()
              : 0.0;
  }

  private int getIntValue(JsonObject json, String key) {
      return json.containsKey(key) && !json.isNull(key)
              ? json.getJsonNumber(key).intValue()
              : 0;
  }
    // TODO: Task 2.3
    // You can add any number of parameters and return any type from the method
    // You can throw any checked exceptions from the method
    // Write the native Mongo query you implement in the method in the comments
    //
    //    native MongoDB query here
    //

    /*
    db.createCollection("imdb")
    db.imdb.createIndex({ "imdb_id": 1 }, { unique: true })
    db.imdb.insertMany([{
        imdb_id: String,
        title: String,
        director: String,
        overview: String,
        tagline: String,
        genres: String,
        imdb_rating: Number,
        imdb_votes: Number
    }])
     */
    public void batchInsertMovies(List<JsonObject> movies) {
        checkCollectionAndIndex();
        List<WriteModel<Document>> writes = movies.stream()
                .map(this::insertModel)
                .toList();
        if (!writes.isEmpty()) {
            try {
                mongoTemplate.getCollection("imdb")
                        .bulkWrite(writes);
            } catch (Exception e) {
                logError(movies.stream()
                        .map(m -> getStringValue(m, "imdb_id"))
                        .toList(), e);
                throw e;
            }
        }
    }

    private WriteModel<Document> insertModel(JsonObject movie) {
        Document doc = toMongoDocument(movie);
        return new InsertOneModel<Document>(doc);
    }

    private void checkCollectionAndIndex() {
        if (!mongoTemplate.collectionExists("imdb")) {
            mongoTemplate.createCollection("imdb");
        }
        if (!mongoTemplate.collectionExists("error")) {
            mongoTemplate.createCollection("error");
        }
        IndexOperations indexOps = mongoTemplate.indexOps("imdb");
        IndexDefinition indexDefinition = new Index()
                .on("imdb_id", Sort.Direction.ASC)
                .unique();
        indexOps.ensureIndex(indexDefinition);
    }

    // TODO: Task 2.4
    // You can add any number of parameters and return any type from the method
    // You can throw any checked exceptions from the method
    // Write the native Mongo query you implement in the method in the comments
    //
    //    native MongoDB query here
    //
    /* db.errors.insert({
      ids: ["a0", "a1", "a2", "a3", "a4"],
      error: "Error message here",
      timestamp: new Date()
  })
     */
    public void logError(List<String> imdbIds, Exception ex) {
        Document errorDoc = new Document()
                .append("ids", imdbIds)
                .append("error", ex.getMessage())
                .append("timestamp", new Date());
        mongoTemplate.getCollection("error").insertOne(errorDoc);
    }

    // TODO: Task 3
    // Write the native Mongo query you implement in the method in the comments
    //
    //    native MongoDB query here
    //
    /*
    db.imdb.aggregate([
  {
    $match: {
      directors: { $ne: "" }
    }
  },{
    $group: {
      _id: "$directors",
      movies_count: { $sum: 1 }
    }
  },{
    $project: {
      _id: 0,
      director_name: "$_id",
      movies_count: 1
    }
  },{
    $sort: { movies_count: -1 }
  },{
    $limit: 10  
  }
])*/
    public List<StatsDir> getTopDirectors(Integer limit) {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("directors").ne("")),
                Aggregation.group("directors").count().as("movies_count"),
                Aggregation.project()
                        .andExpression("_id").as("director_name")
                        .andExpression("movies_count").as("movies_count"),
                Aggregation.sort(Sort.Direction.DESC, "movies_count"),
                Aggregation.limit(limit));
        AggregationResults<Document> results = mongoTemplate.aggregate(aggregation, "imdb", Document.class);
        List<StatsDir> directors = new ArrayList<>();
        for (Document doc : results.getMappedResults()) {
            StatsDir stats = new StatsDir(
                    doc.getString("director_name"),
                    doc.getInteger("movies_count"),
                    0.0,
                    0.0,
                    0.0);
            directors.add(stats);
        }
        return directors;
    }

    // db.imdb.find(
    // { "directors": "<name>" },
    // { "imdb_id": <imdb id>, "_id": <ObjectId> })
    public List<String> getMovieIdsByDirector(String directorName) {
        Query query = Query.query(Criteria.where("directors").is(directorName));
        query.fields().include("imdb_id");
        return mongoTemplate.find(query, Document.class, "imdb")
                .stream()
                .map(doc -> doc.getString("imdb_id"))
                .toList();
    }

    
}
