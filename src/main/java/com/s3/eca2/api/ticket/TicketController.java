package com.s3.eca2.api.ticket;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.ticket.Ticket;
import com.s3.eca2.domain.ticket.TicketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

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

    @PostMapping("/makeParquet")
    public ResponseEntity<String> selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate start,
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate end) {

        DateTimeFormatter fileNameFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        DateTimeFormatter pathFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForFileName = start.format(fileNameFormatter);
        String formattedDateForPath = start.format(pathFormatter);

        int pageNumber = 0;
        final int pageSize = 400000; // 한 페이지 당 처리할 데이터 수를 줄입니다.
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        try {
            while (true) {
                Date startDate = Date.from(start.atStartOfDay(ZoneId.systemDefault()).toInstant());
                Date endDate = Date.from(end.atStartOfDay(ZoneId.systemDefault()).toInstant());

                Page<Ticket> ticketsPage = ticketService.findTicketsByDate(startDate, endDate, pageable);
                List<Ticket> tickets = ticketsPage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "eca_cs_ticket_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                ticketToParquetConverter.writeTicketsToParquet(tickets, outputPath);

                String s3Key = "cs/prod/eca_cs_ticket_tm/base_dt=" + formattedDateForPath +
                        "/eca_cs_ticket_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                if (!ticketsPage.hasNext() || tickets.isEmpty()) {
                    break; // 조회된 데이터가 없거나 마지막 페이지에 도달하면 종료
                }
                pageNumber++;
                pageable = pageable.next();
            }
            return ResponseEntity.ok("Parquet files created and uploaded successfully.");
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            return ResponseEntity.internalServerError().body("Failed to create and upload Parquet files.");
        }
    }

    @GetMapping("/getParquet")
    public ResponseEntity<String> selectByPath(@RequestParam("s3key") String s3key) {
        return ResponseEntity.ok(s3Service.readParquetFromS3(s3key));
    }
}
