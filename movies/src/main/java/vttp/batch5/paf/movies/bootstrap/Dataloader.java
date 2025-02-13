package vttp.batch5.paf.movies.bootstrap;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Component
public class Dataloader implements CommandLineRunner {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MySQLMovieRepository mysqlRepo;

    //TODO: Task 2
    @Override
    public void run(String... args) throws Exception {
        if (!dataLoaded()) {
            loadData();
        }
    }

    private boolean dataLoaded() {
        try {
            String sql = "select count(*) from imdb";
            Long mysqlCount = jdbcTemplate.queryForObject(sql, Long.class);
            if (mysqlCount != null && mysqlCount > 0) {
                return true;
            }
        } catch (Exception e) {
        }
        try {
            long mongoCount = mongoTemplate.getCollection("imdb").countDocuments();
            return mongoCount > 0;
        } catch (Exception e) {
            return false;
        }
    }

  private void loadData() {
        try {
            ClassPathResource resource = new ClassPathResource("data/movies_post_2010.zip");
            try (InputStream is = resource.getInputStream();
                 ZipInputStream zipIn = new ZipInputStream(is)) {
                
                List<JsonObject> currentBatch = new ArrayList<>();
                ZipEntry entry;
                
                while ((entry = zipIn.getNextEntry()) != null) {
                    if (entry.getName().endsWith(".json")) {
                        System.out.println("Processing JSON file: " + entry.getName());
                        createJsonFile(zipIn, currentBatch);
                    }
                }
                
                if (!currentBatch.isEmpty()) {
                    System.out.println("Processing final batch of " + currentBatch.size() + " movies");
                    processBatch(currentBatch);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load movie data: " + e.getMessage(), e);
        }
    }

    private void createJsonFile(ZipInputStream zipIn, List<JsonObject> currentBatch) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(zipIn));
        String line;
        int lineCount = 0;

        while ((line = reader.readLine()) != null) {
            try (JsonReader jsonReader = Json.createReader(new StringReader(line))) {
                JsonObject movieObj = jsonReader.readObject();
                String releaseDateStr = movieObj.getString("release_date", "");
                if (checkReleaseDate(releaseDateStr)) {
                    currentBatch.add(movieObj);
                    if (currentBatch.size() == 25) {
                        processBatch(currentBatch);
                        currentBatch.clear();
                    }
                }
            }
            lineCount++;
            if (lineCount % 100 == 0) {
                System.out.println("Processed: " + lineCount + " lines");
            }
        }
    }
    private void processBatch(List<JsonObject> batch) {
        try {
            mysqlRepo.batchInsertMovies(new ArrayList<>(batch));
        } catch (Exception e) {
            System.err.println("Error processing batch: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private boolean checkReleaseDate(String dateStr) {
        try {
            LocalDate releaseDate = LocalDate.parse(dateStr);
            return !releaseDate.isBefore(LocalDate.of(2018, 1, 1));
        } catch (Exception e) {
            return false;
        }
    }
}
