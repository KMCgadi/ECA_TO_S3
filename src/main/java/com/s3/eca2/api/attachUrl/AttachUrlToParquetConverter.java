package com.s3.eca2.api.attachUrl;


import com.s3.eca2.domain.attachUrl.AttachUrl;
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
public class AttachUrlToParquetConverter {

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(BINARY, OPTIONAL).named("attachUrlEid"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("linkType"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("ticketId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("shortUrl"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("sendDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("submitDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("linkStatus"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityStatus"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modUserEntityId"))
            .named("AttachUrl");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeAttachUrlToParquet(List<AttachUrl> attachUrls, String fileOutputPath) throws IOException {
        try (ParquetWriter<AttachUrl> writer = new AttachUrlParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (AttachUrl attachUrl : attachUrls) {
                writer.write(attachUrl);
            }
        }
    }

    private static class AttachUrlParquetWriter extends ParquetWriter<AttachUrl> {
        public AttachUrlParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new AttachUrlWriteSupport(schema));
        }
    }

    private static class AttachUrlWriteSupport extends WriteSupport<AttachUrl> {
        private MessageType schema;
        private RecordConsumer recordConsumer;

        public AttachUrlWriteSupport(MessageType schema) {
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
        public void write(AttachUrl attachUrl) {
            recordConsumer.startMessage();
            writeOptionalStringField("attachUrlEid", attachUrl.getAttachUrlEid());
            writeStringField("linkType", attachUrl.getLinkType());
            writeOptionalStringField("ticketId", attachUrl.getTicketId());
            writeStringField("shortUrl", attachUrl.getShortUrl());
            writeOptionalDateStringField("sendDate", attachUrl.getSendDate());
            writeOptionalDateStringField("submitDate", attachUrl.getSubmitDate());
            writeStringField("linkStatus", attachUrl.getLinkStatus());
            writeStringField("entityStatus", attachUrl.getEntityStatus());
            writeOptionalDateStringField("regDate", attachUrl.getRegDate());
            writeStringField("regUserEntityId", attachUrl.getRegUserEntityId());
            writeOptionalDateStringField("modDate", attachUrl.getModDate());
            writeStringField("modUserEntityId", attachUrl.getModUserEntityId());

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
