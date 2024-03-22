package com.s3.eca2.api.toastHistory;

import com.s3.eca2.api.ticketOrder.TicketOrderToParquetConverter;
import com.s3.eca2.domain.ticketOrder.TicketOrder;
import com.s3.eca2.domain.toastHistory.ToastHistory;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
@Component
public class ToastHistoryToParquetConverter {

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("sendType"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("requestId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("sender"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("templateCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityStatus"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("workType"))
            .named("ToastHistory");
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeToastHistoryToParquet(List<ToastHistory> toastHistories, String fileOutputPath) throws IOException {
        try (ParquetWriter<ToastHistory> writer = new ToastHistoryToParquetConverter.ToastHistoryParquetWriter(new Path(fileOutputPath),SCHEMA)) {
            for (ToastHistory toastHistory : toastHistories) {
                writer.write(toastHistory);
            }
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
        public void write(ToastHistory toastHistory){
            recordConsumer.startMessage();
            writeOptionalStringField("entityId", toastHistory.getEntityId());
            writeStringField("sendType", toastHistory.getSendType());
            writeStringField("requestId", toastHistory.getRequestId());
            writeStringField("sender", toastHistory.getSender());
            writeStringField("templateCode", toastHistory.getTemplateCode());
            writeStringField("entityStatus", toastHistory.getEntityStatus());
            writeOptionalDateStringField("regDate", toastHistory.getRegDate());
            writeOptionalStringField("regUserEntityId", toastHistory.getRegUserEntityId());
            writeOptionalDateStringField("modDate", toastHistory.getModDate());
            writeOptionalStringField("modUserEntityId", toastHistory.getModUserEntityId());
            writeStringField("workType", toastHistory.getWorkType());
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
