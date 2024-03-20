package com.s3.eca2.api.ticket;

import com.s3.eca2.domain.ticket.Ticket;
import com.s3.eca2.domain.ticket.TicketService;
import com.s3.eca2.api.s3.S3Service;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.nio.file.Paths;

@RestController
@RequestMapping("/rest/api/v1/s3/tickets")
public class TicketController {
    private final S3Service s3Service;
    private final TicketToParquetConverter ticketToParquetConverter;
    private final TicketService ticketService;
    private static final Logger logger = LoggerFactory.getLogger(TicketController.class);

    public TicketController(TicketService ticketService, TicketToParquetConverter ticketToParquetConverter, S3Service s3Service) {
        this.ticketService = ticketService;
        this.ticketToParquetConverter = ticketToParquetConverter;
        this.s3Service = s3Service;
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

        try {
            List<Ticket> tickets = ticketService.findTicketsByDate(start, end);
            ticketToParquetConverter.writeTicketsToParquet(tickets, outputPath);

            String s3Key = "cs/dev/eca_cs_ticket_tm/base_dt=" + formattedDate;
            s3Service.uploadFileToS3(outputPath, s3Key);

            return ResponseEntity.ok("Parquet file created and uploaded successfully to: " + s3Key);
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            return ResponseEntity.internalServerError().body("Failed to create and upload Parquet file.");
        }
    }

    @GetMapping("/getParquet")
    public ResponseEntity<String> selectByPath(@RequestParam("s3key") String s3key) {
        return ResponseEntity.ok(s3Service.readParquetFromS3(s3key));
    }
}
