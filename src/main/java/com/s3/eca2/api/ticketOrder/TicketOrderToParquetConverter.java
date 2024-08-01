package com.s3.eca2.api.ticketOrder;

import com.s3.eca2.domain.ticketOrder.TicketOrder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

@Component
public class TicketOrderToParquetConverter {

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(INT64, REQUIRED).named("TICKET_ORDER_EID"))
            .addField(Types.primitive(INT64, REQUIRED).named("TICKET_EID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ORDER_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("PRODUCT_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("PRODUCT_NAME"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("COUPON_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("PARTNER_NAME"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("BRAND_NAME"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("CATEGORY"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ENTITY_STATUS"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MOD_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("REG_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("MOD_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("REG_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("DELIVERY_METHOD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("PAYMENT_METHOD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("RATING"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ORDER_OPTION_ID"))
            .named("TicketOrder");

    private static final Logger logger = LoggerFactory.getLogger(TicketOrderToParquetConverter.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeTicketOrderToParquet(List<TicketOrder> ticketOrders, String fileOutputPath) throws IOException {
        try (ParquetWriter<TicketOrder> writer = new TicketOrderParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (TicketOrder ticketOrder : ticketOrders) {
                writer.write(ticketOrder);
            }
        } catch (Exception e) {
            logger.error("Error writing Parquet file: ", e);
            throw e;
        }
    }

    private static class TicketOrderParquetWriter extends ParquetWriter<TicketOrder> {
        public TicketOrderParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new TicketOrderWriteSupport(schema));
        }
    }

    private static class TicketOrderWriteSupport extends WriteSupport<TicketOrder> {
        private MessageType schema;

        private RecordConsumer recordConsumer;

        public TicketOrderWriteSupport(MessageType schema) {
            this.schema = schema;
        }

        @Override
        public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
            return new WriteContext(schema, new java.util.HashMap<>());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(TicketOrder ticketOrder) {
            recordConsumer.startMessage();
            writeLongField("TICKET_ORDER_EID", 0, ticketOrder.getTicketOrderEid());
            writeNullableLongField("TICKET_EID", 1, ticketOrder.getTicketEid());
            writeStringField("ORDER_ID", 2, ticketOrder.getOrderId());
            writeStringField("PRODUCT_ID", 3, ticketOrder.getProductId());
            writeStringField("PRODUCT_NAME", 4, ticketOrder.getProductName());
            writeStringField("COUPON_ID", 5, ticketOrder.getCouponId());
            writeStringField("PARTNER_NAME", 6, ticketOrder.getPartnerName());
            writeStringField("BRAND_NAME", 7, ticketOrder.getBrandName());
            writeStringField("CATEGORY", 8, ticketOrder.getCateGory());
            writeStringField("ENTITY_STATUS", 9, ticketOrder.getEntityStatus());
            writeStringField("MOD_DATE", 10, dateToString(ticketOrder.getModDate()));
            writeStringField("REG_DATE", 11, dateToString(ticketOrder.getRegDate()));
            writeNullableLongField("MOD_USER_ENTITY_ID", 12, ticketOrder.getModUserEntityId());
            writeNullableLongField("REG_USER_ENTITY_ID", 13, ticketOrder.getRegUserEntityId());
            writeStringField("DELIVERY_METHOD", 14, ticketOrder.getDeliveryMethod());
            writeStringField("PAYMENT_METHOD", 15, ticketOrder.getPaymentMethod());
            writeStringField("RATING", 16, ticketOrder.getRating());
            writeStringField("ORDER_OPTION_ID", 17, ticketOrder.getOrderOptionId());

            recordConsumer.endMessage();
        }

        private void writeLongField(String fieldName, int fieldIndex, long value) {
            recordConsumer.startField(fieldName, fieldIndex);
            recordConsumer.addLong(value);
            recordConsumer.endField(fieldName, fieldIndex);
        }

        private void writeStringField(String fieldName, int fieldIndex, String value) {
            if (value != null) {
                recordConsumer.startField(fieldName, fieldIndex);
                recordConsumer.addBinary(Binary.fromString(value));
                recordConsumer.endField(fieldName, fieldIndex);
            }
        }

        private String dateToString(Date date) {
            return date != null ? dateFormat.format(date) : null;
        }

        private void writeNullableLongField(String fieldName, int fieldIndex, Long value) {
            if (value != null) {
                recordConsumer.startField(fieldName, fieldIndex);
                recordConsumer.addLong(value);
                recordConsumer.endField(fieldName, fieldIndex);
            }
        }
    }
}
