package com.s3.eca2.api.ticketRelation;

import com.s3.eca2.domain.ticketRelation.TicketRelation;
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
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

@Component
public class TicketRelationToParquetConverter {

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(INT64, REQUIRED).named("TICKET_RELATION_EID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ENTITY_STATUS"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MOD_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("REG_DATE"))
            .addField(Types.primitive(INT64, REQUIRED).named("TARGET_TICKET_EID"))
            .addField(Types.primitive(INT64, REQUIRED).named("TICKET_EID"))
            .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, REQUIRED).as(OriginalType.UTF8).named("TYPE_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("MOD_USER_ENTITY_ID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("REG_USER_ENTITY_ID"))
            .named("TicketRelation");

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger logger = LoggerFactory.getLogger(TicketRelationToParquetConverter.class);

    public void writeTicketRelationToParquet(List<TicketRelation> ticketRelations, String fileOutputPath) throws IOException {
        try (ParquetWriter<TicketRelation> writer = new TicketRelationToParquetConverter.TicketRelationParquetWriter(new Path(fileOutputPath), SCHEMA)) {
            for (TicketRelation ticketRelation : ticketRelations) {
                writer.write(ticketRelation);
            }
        } catch (Exception e) {
            logger.error("Error writing Parquet file: ", e);
            throw e;
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
            writeLongField("TICKET_RELATION_EID", 0, ticketRelation.getTicketRelationEid());
            writeStringField("ENTITY_STATUS", 1, ticketRelation.getEntityStatus());
            writeStringField("MOD_DATE", 2, dateToString(ticketRelation.getModDate()));
            writeStringField("REG_DATE", 3, dateToString(ticketRelation.getRegDate()));
            writeNullableLongField("TARGET_TICKET_EID", 4, ticketRelation.getTargetTicketEid());
            writeNullableLongField("TICKET_EID", 5, ticketRelation.getTicketEid());
            writeStringField("TYPE_CD", 6, ticketRelation.getTypeCode());
            writeNullableLongField("MOD_USER_ENTITY_ID", 7, ticketRelation.getModUserEntityId());
            writeNullableLongField("REG_USER_ENTITY_ID", 8, ticketRelation.getRegUserEntityId());
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



