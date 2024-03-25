package com.s3.eca2.api.ticketChannel;

import com.s3.eca2.domain.ticketChannel.Channel;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

@Component
public class ChannelToParquetConverter {

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(INT64, REQUIRED).named("TICKET_CHANNEL_EID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ENTITY_STATUS"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("MOD_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("REG_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("CONTACT_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("END_DATE"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("START_DATE"))
            .addField(Types.primitive(INT64, REQUIRED).named("TICKET_EID"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("TYPE_CD"))
            .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("PROCESS_DATE"))
            .addField(Types.optional(INT64).named("MOD_USER_ENTITY_ID"))
            .addField(Types.optional(INT64).named("REG_USER_ENTITY_ID"))
            .named("TicketChannel");

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeTicketChannelToParquet(List<Channel> channels, String fileOutputPath) throws IOException {
        try (ParquetWriter<Channel> writer = new TicketChannelParquetWriter(new Path(fileOutputPath),SCHEMA)){
            for (Channel channel : channels){
                writer.write(channel);
            }
        }
    }
    private static class TicketChannelParquetWriter extends ParquetWriter<Channel>{
        public TicketChannelParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new TicketChannelWriteSupport(schema));
        }
    }

    private static class TicketChannelWriteSupport extends WriteSupport<Channel> {
        private MessageType schema;
        private RecordConsumer recordConsumer;

        public TicketChannelWriteSupport(MessageType schema) { this.schema = schema;}

        @Override
        public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
            return new WriteContext(schema, new java.util.HashMap<>());
        }
        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(Channel channel){
            recordConsumer.startMessage();
            writeNullableLongField("ticketChannelEid",0, channel.getTicketChannelEid());
            writeStringField("entityStatus",1, channel.getEntityStatus());
            writeStringField("modDate",2, dateToString(channel.getModDate()));
            writeStringField("regDate",3, dateToString(channel.getRegDate()));
            writeStringField("contactCode",4, channel.getContactCode());
            writeStringField("endDate",5, channel.getEndDate());
            writeStringField("startDate",6, channel.getStartDate());
            writeNullableLongField("ticketEid",7, channel.getTicketEid());
            writeStringField("typeCode",8, channel.getTypeCode());
            writeStringField("processDate",9, channel.getProcessDate());
            writeNullableLongField("modUserEntityId",10, channel.getModUserEntityId());
            writeNullableLongField("regUserEntityId",11, channel.getRegUserEntityId());
            recordConsumer.endMessage();

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

        private void writeDateField(String fieldName, int fieldIndex, Date date) {
            if (date != null) {
                long timeInMillis = date.getTime(); // 날짜를 밀리초로 변환
                recordConsumer.startField(fieldName, fieldIndex);
                recordConsumer.addLong(timeInMillis); // TIMESTAMP_MILLIS 타입으로 기록
                recordConsumer.endField(fieldName, fieldIndex);
            }
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
