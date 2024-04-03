package com.s3.eca2.api.ticketOrder;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.ticketOrder.TicketOrder;
import com.s3.eca2.domain.ticketOrder.TicketOrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/rest/api/v1/s3/ticket-order")
public class TicketOrderController {
    private final S3Service s3Service;
    private final TicketOrderService ticketOrderService;
    private final TicketOrderToParquetConverter ticketOrderToParquetConverter;
    private static final Logger logger = LoggerFactory.getLogger(TicketOrderController.class);

    public TicketOrderController(TicketOrderService ticketOrderService, TicketOrderToParquetConverter ticketOrderToParquetConverter, S3Service s3Service) {
        this.ticketOrderService = ticketOrderService;
        this.ticketOrderToParquetConverter = ticketOrderToParquetConverter;
        this.s3Service = s3Service;
    }

    @GetMapping("/{ticketOrderEid}")
    public TicketOrder selectOne(@PathVariable long ticketOrderEid) {
        return ticketOrderService.find(ticketOrderEid);
    }

    @PostMapping("/makeParquet")
    public ResponseEntity<String> selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end) {

        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Seoul")).minusDays(1);
        DateTimeFormatter fileNameFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        DateTimeFormatter pathFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForFileName = now.format(fileNameFormatter);
        String formattedDateForPath = now.format(pathFormatter);

        int pageNumber = 0;
        final int pageSize = 400000;
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        try {
            while (true) {
                Page<TicketOrder> ticketOrderPage = ticketOrderService.findTicketOrderByDate(start, end, pageable);
                List<TicketOrder> ticketOrders = ticketOrderPage.getContent();

                String outputPath = Paths.get(System.getProperty("user.dir"), "temp",
                        "eca_cs_ticket_order_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet").toString();
                ticketOrderToParquetConverter.writeTicketOrderToParquet(ticketOrders, outputPath);

                String s3Key = "cs/prod/eca_cs_ticket_order_tm/base_dt=" + formattedDateForPath +
                        "/eca_cs_ticket_order_tm_" + formattedDateForFileName + "_" + (pageNumber + 1) + ".parquet";
                s3Service.uploadFileToS3(outputPath, s3Key);

                if (!ticketOrderPage.hasNext() || ticketOrders.isEmpty()) {
                    break;
                }
                pageNumber++;
                pageable = pageable.next();
            }
            return ResponseEntity.ok("Parquet files created and uploaded successfully");
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            return ResponseEntity.internalServerError().body("Failed to create and upload Parquet files");
        }
    }

    @GetMapping("/getParquet")
    public ResponseEntity<String> selectByPath(@RequestParam("s3key") String s3key) {
        return ResponseEntity.ok(s3Service.readParquetFromS3(s3key));
    }
}
