package com.s3.eca2.api.ticketOrder;

import com.s3.eca2.api.s3.S3Service;
import com.s3.eca2.domain.ticketOrder.TicketOrder;
import com.s3.eca2.domain.ticketOrder.TicketOrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
                                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end, @RequestParam int fileNum) {

        ZonedDateTime date = ZonedDateTime.now(ZoneId.of("Asia/Seoul"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formattedDateForFileName = date.format(formatter);
        DateTimeFormatter formatterForPath = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDateForPath = date.format(formatterForPath);
        String outputPath = Paths.get(System.getProperty("user.dir"), "temp", "eca_cs_ticket_order_tm" + formattedDateForFileName + "_" + fileNum + ".parquet").toString();

        try {
            List<TicketOrder> ticketOrders = ticketOrderService.findTicketOrderByDate(start, end);
            ticketOrderToParquetConverter.writeTicketOrderToParquet(ticketOrders, outputPath);

            String s3Key = "cs/dev/eca_cs_ticket_order_tm/base_dt=" + formattedDateForPath + "/eca_cs_ticket_order_tm" + formattedDateForFileName + "_" + fileNum + ".parquet";
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
