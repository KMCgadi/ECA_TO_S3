package com.s3.eca2.api.ticketOrder;
import com.s3.eca2.domain.ticketOrder.TicketOrder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.springframework.stereotype.Component;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
@Component
public class TicketOrderToParquetConverter {

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(BINARY, OPTIONAL).named("ticketOrderEid"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("ticketEid"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("productId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("productName"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("partnerName"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("brandName"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("cateGory"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityStatus"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("deliveryMethod"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("paymentMethod"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("rating"))
            .named("TicketOrder");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeTicketOrderToParquet(List<TicketOrder> ticketOrders, String fileOutputPath) throws IOException {
        try (ParquetWriter<TicketOrder> writer = new TicketOrderParquetWriter(new Path(fileOutputPath),SCHEMA)) {
            for (TicketOrder ticketOrder : ticketOrders) {
                writer.write(ticketOrder);
            }
        }
    }
    private static class TicketOrderParquetWriter extends ParquetWriter<TicketOrder>{
        public TicketOrderParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new TicketOrderWriteSupport(schema));
        }
    }
    private static class TicketOrderWriteSupport extends WriteSupport<TicketOrder>{
        private MessageType schema;

        private RecordConsumer recordConsumer;

        public TicketOrderWriteSupport(MessageType schema){this.schema = schema;}

        @Override
        public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
            return new WriteContext(schema, new java.util.HashMap<>());
        }
        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }
        @Override
        public void write(TicketOrder ticketOrder){
            recordConsumer.startMessage();
            writeOptionalStringField("ticketOrderEid", ticketOrder.getTicketOrderEid());
            writeOptionalStringField("ticketEid", ticketOrder.getTicketEid());
            writeOptionalStringField("productId", ticketOrder.getProductId());
            writeStringField("productName", ticketOrder.getProductName());
            writeStringField("partnerName", ticketOrder.getPartnerName());
            writeStringField("brandName", ticketOrder.getBrandName());
            writeStringField("cateGory", ticketOrder.getCateGory());
            writeStringField("entityStatus", ticketOrder.getEntityStatus());
            writeOptionalDateStringField("modDate", ticketOrder.getModDate());
            writeOptionalDateStringField("regDate", ticketOrder.getRegDate());
            writeOptionalStringField("modUserEntityId", ticketOrder.getModUserEntityId());
            writeOptionalStringField("regUserEntityId", ticketOrder.getRegUserEntityId());
            writeStringField("deliveryMethod", ticketOrder.getDeliveryMethod());
            writeStringField("paymentMethod", ticketOrder.getPaymentMethod());
            writeStringField("rating", ticketOrder.getRating());

            recordConsumer.endMessage();
        }
        private void writeStringField(String fieldName, String value) {
            if (value != null) {
                int fieldIndex = schema.getFieldIndex(fieldName);
                recordConsumer.startField(fieldName, fieldIndex);
                recordConsumer.addBinary(Binary.fromString(value));
                recordConsumer.endField(fieldName, fieldIndex);
            }
        }

        private void writeOptionalStringField(String fieldName, Object value) {
            if (value != null) {
                writeStringField(fieldName, value.toString());
            }
        }

        private void writeOptionalDateStringField(String fieldName, Date date) {
            if (date != null) {
                writeStringField(fieldName, dateFormat.format(date));
            }
        }
    }
}
