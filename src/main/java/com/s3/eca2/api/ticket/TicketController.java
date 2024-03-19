package com.s3.eca2.api.ticket;

import com.s3.eca2.domain.ticket.Ticket;
import com.s3.eca2.domain.ticket.TicketService;
import com.s3.eca2.s3uploader.S3Uploader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/rest/api/v1/s3/tickets")
public class TicketController {
    @Autowired
    S3Uploader s3Uploader;
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
    public String selectByDate(@RequestParam("start") @DateTimeFormat(pattern = "yyyy-MM-dd") Date start,
                               @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd") Date end , int temp) {
        List<Ticket> tickets = ticketService.findTicketsByDate(start, end);
        String outputPath = "D:\\Documents\\temp\\tickets"+temp+".parquet"; // Parquet 파일의 저장 경로 지정

        try {
            ticketToParquetConverter.writeTicketsToParquet(tickets, outputPath);
            Date date = new Date();

            String s3Key = "cs/dev/eca_cs_ticket_tm/base_dt=" + date.getTime();
            s3Uploader.uploadFileToS3(outputPath, s3Key);

            return "Parquet file created and uploaded successfully to: " + s3Key;
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to create and upload Parquet file.";
        }
    }

    @GetMapping("/s3")
    public void hello() {
        s3Uploader.listS3BucketContents();
    }

    @GetMapping("/ticketParquet")
    public String selectByPath(@RequestParam("s3key") String s3key) {
        return s3Uploader.readParquetFromS3(s3key);
    }
}
