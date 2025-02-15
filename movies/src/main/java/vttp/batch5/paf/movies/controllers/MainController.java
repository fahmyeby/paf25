package vttp.batch5.paf.movies.controllers;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import vttp.batch5.paf.movies.models.StatsDir;
import vttp.batch5.paf.movies.services.MovieService;

@RestController
@RequestMapping("/api")
public class MainController {

    @Autowired
    private MovieService movieService;

    @Value("${username}")
    private String userName;

    @Value("${group}")
    private String userBatch;

    // TODO: Task 3
    @GetMapping("/summary")
    public ResponseEntity<List<StatsDir>> getSummary(@RequestParam Integer count) {
        List<StatsDir> result = movieService.getProlificDirectors(count);
        return ResponseEntity.ok(result);
    }
    // TODO: Task 4

    @GetMapping("/summary/pdf")
    public ResponseEntity<byte[]> report(@RequestParam Integer cout) {
        byte[] pdfReport = movieService.generatePDFReport(userName, userBatch, cout);
        if (pdfReport == null) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok().body(pdfReport);
    }
}
