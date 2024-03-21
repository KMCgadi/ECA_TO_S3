package com.s3.eca2.api.ticketChannel;

import com.s3.eca2.domain.ticketChannel.Channel;
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
public class ChannelToParquetConverter {

    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(BINARY, OPTIONAL).named("ticketChannelEid"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityStatus"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("contactCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("endDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("startDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("ticketEid"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("typeCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("processDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regUserEntityId"))
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
            writeOptionalStringField("ticketChannelEid", channel.getTicketChannelEid());
            writeOptionalStringField("entityStatus", channel.getEntityStatus());
            writeOptionalDateStringField("modDate", channel.getModDate());
            writeOptionalDateStringField("regDate", channel.getRegDate());
            writeStringField("contactCode", channel.getContactCode());
            writeStringField("endDate", channel.getEndDate());
            writeStringField("startDate", channel.getStartDate());
            writeOptionalStringField("ticketEid", channel.getTicketEid());
            writeOptionalStringField("typeCode", channel.getTypeCode());
            writeStringField("processDate", channel.getProcessDate());
            writeOptionalStringField("modUserEntityId", channel.getModUserEntityId());
            writeOptionalStringField("regUserEntityId", channel.getRegUserEntityId());
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
