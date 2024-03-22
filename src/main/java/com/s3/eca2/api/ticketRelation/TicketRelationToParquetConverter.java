package com.s3.eca2.api.ticketRelation;

import com.s3.eca2.domain.ticketRelation.TicketRelation;
import org.apache.hadoop.fs.Path;
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
public class TicketRelationToParquetConverter {

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(BINARY, OPTIONAL).named("ticketRelationEid"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityStatus"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("targetTicketEid"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("ticketEid"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("typeCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityStatus"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regUserEntityId"))
            .named("TicketRelation");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeTicketRelationToParquet(List<TicketRelation> ticketRelations, String fileOutputPath) throws IOException {
        try (ParquetWriter<TicketRelation> writer = new TicketRelationToParquetConverter.TicketRelationParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (TicketRelation ticketRelation : ticketRelations) {
                writer.write(ticketRelation);
            }
        }
    }

    private static class TicketRelationParquetWriter extends ParquetWriter<TicketRelation> {
        public TicketRelationParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new TicketRelationToParquetConverter.TicketRelationWriteSupport(schema));
        }
    }

    private static class TicketRelationWriteSupport extends WriteSupport<TicketRelation> {
        private MessageType schema;

        private RecordConsumer recordConsumer;

        public TicketRelationWriteSupport(MessageType schema) {
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
        public void write(TicketRelation ticketRelation) {
            recordConsumer.startMessage();
            writeOptionalStringField("ticketRelationEid", ticketRelation.getTicketRelationEid());
            writeStringField("entityStatus", ticketRelation.getEntityStatus());
            writeOptionalDateStringField("modDate", ticketRelation.getModDate());
            writeOptionalDateStringField("regDate", ticketRelation.getRegDate());
            writeOptionalStringField("targetTicketEid", ticketRelation.getTargetTicketEid());
            writeOptionalStringField("ticketEid", ticketRelation.getTicketEid());
            writeStringField("typeCode", ticketRelation.getTypeCode());
            writeOptionalStringField("modUserEntityId", ticketRelation.getModUserEntityId());
            writeOptionalStringField("regUserEntityId", ticketRelation.getRegUserEntityId());
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



