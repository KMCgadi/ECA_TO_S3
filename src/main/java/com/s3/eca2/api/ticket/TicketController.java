package com.s3.eca2.api.ticket;

import com.s3.eca2.domain.ticket.Ticket;
import com.s3.eca2.domain.ticket.TicketService;
import com.s3.eca2.s3uploader.S3Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.nio.file.Paths;

@RestController
@RequestMapping("/rest/api/v1/s3/tickets")
public class TicketController {
    @Autowired
    S3Service s3Service;
    private final TicketToParquetConverter ticketToParquetConverter;
    private final TicketService ticketService;

    public TicketController(TicketService ticketService, TicketToParquetConverter ticketToParquetConverter) {
        this.ticketService = ticketService;
        this.ticketToParquetConverter = ticketToParquetConverter;
    }

    @GetMapping("/{entityId}")
    public Ticket selectOne(@PathVariable long entityId) {
        return ticketService.find(entityId);
    }

    @GetMapping("/makeParquet")
    public ResponseEntity<String> selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end) {

        ZonedDateTime date = ZonedDateTime.now(ZoneId.of("Asia/Seoul"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        String formattedDate = date.format(formatter);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", formattedDate + ".parquet").toString();
        System.out.println("경로체크: "+outputPath);

        try {
            List<Ticket> tickets = ticketService.findTicketsByDate(start, end);
            ticketToParquetConverter.writeTicketsToParquet(tickets, outputPath);

            String s3Key = "cs/dev/eca_cs_ticket_tm/base_dt=" + formattedDate;
            s3Service.uploadFileToS3(outputPath, s3Key);

            return ResponseEntity.ok("Parquet file created and uploaded successfully to: " + s3Key);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Failed to create and upload Parquet file.");
        }
    }

    @GetMapping("/getParquet")
    public ResponseEntity<String> selectByPath(@RequestParam("s3key") String s3key) {
        return ResponseEntity.ok(s3Service.readParquetFromS3(s3key));
    }
}
