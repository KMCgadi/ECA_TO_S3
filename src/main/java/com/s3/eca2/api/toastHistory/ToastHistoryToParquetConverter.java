package com.s3.eca2.api.toastHistory;

import com.s3.eca2.domain.toastHistory.ToastHistory;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

@Component
public class ToastHistoryToParquetConverter {
    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(INT64, REQUIRED).named("ENTITY_ID"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("SEND_TYPE"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("REQUEST_ID"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("SENDER"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("TEMPLATE_CD"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("ENTITY_STATUS"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("REG_DATE"))
            .addField(Types.optional(INT64).named("REG_USER_ENTITY_ID"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("MOD_DATE"))
            .addField(Types.optional(INT64).named("MOD_USER_ENTITY_ID"))
            .addField(Types.optional(BINARY).as(OriginalType.UTF8).named("WORK_TYPE"))
            .named("ToastHistory");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger logger = LoggerFactory.getLogger(ToastHistoryToParquetConverter.class);

    public void writeToastHistoryToParquet(List<ToastHistory> toastHistories, String fileOutputPath) throws IOException {
        try (ParquetWriter<ToastHistory> writer = new ToastHistoryToParquetConverter.ToastHistoryParquetWriter(new Path(fileOutputPath),SCHEMA)) {
            for (ToastHistory toastHistory : toastHistories) {
                writer.write(toastHistory);
            }
        } catch (Exception e) {
            logger.error(String.valueOf(e));
        }
    }
    private static class ToastHistoryParquetWriter extends ParquetWriter<ToastHistory>{
        public ToastHistoryParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new ToastHistoryToParquetConverter.ToastHistoryWriteSupport(schema));
        }
    }
    private static class ToastHistoryWriteSupport extends WriteSupport<ToastHistory> {
        private MessageType schema;
        private RecordConsumer recordConsumer;

        public ToastHistoryWriteSupport(MessageType schema){this.schema = schema;}

        @Override
        public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
            return new WriteContext(schema, new java.util.HashMap<>());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(ToastHistory toastHistory) {
            recordConsumer.startMessage();
            writeLongField("ENTITY_ID", 0, toastHistory.getEntityId());
            writeStringField("SEND_TYPE", 1, toastHistory.getSendType());
            writeStringField("REQUEST_ID", 2, toastHistory.getRequestId());
            writeStringField("SENDER", 3, toastHistory.getSender());
            writeStringField("TEMPLATE_CD", 4, toastHistory.getTemplateCode());
            writeStringField("ENTITY_STATUS", 5, toastHistory.getEntityStatus());
            writeStringField("REG_DATE", 6, dateToString(toastHistory.getRegDate()));
            writeNullableLongField("REG_USER_ENTITY_ID", 7, toastHistory.getRegUserEntityId());
            writeStringField("MOD_DATE", 8, dateToString(toastHistory.getModDate()));
            writeNullableLongField("MOD_USER_ENTITY_ID", 9, toastHistory.getModUserEntityId());
            writeStringField("WORK_TYPE", 10, toastHistory.getWorkType());
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
