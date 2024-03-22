package com.s3.eca2.api.user;

import com.s3.eca2.api.toastHistory.ToastHistoryToParquetConverter;
import com.s3.eca2.domain.toastHistory.ToastHistory;
import com.s3.eca2.domain.user.User;
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
public class UserToParquetConverter {
    private static final MessageType SCHEMA = Types.buildMessage()
            .addField(Types.primitive(BINARY, OPTIONAL).named("userEid"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("entityStatus"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("authCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("birthDay"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("centerCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("deptCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("email"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("fax"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("hireDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("homeAddr"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("homeTel"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("id"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("lastEduCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("leaveDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("mobile"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("name"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("note"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("password"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("positionCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("rankCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("resignReason"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("teamCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("unusedReasonCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("usable"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("weddingDate"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("ctiExtension"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("ctiLoginId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("ctiYn"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("ipAddr"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("workStateCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("contractStateCode"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("modUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("regUserEntityId"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("token"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("recManageYn"))
            .addField(Types.primitive(BINARY, OPTIONAL).named("recCounselYn"))
            .named("ToastHistory");
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void writeUserToParquet(List<User> users, String fileOutputPath) throws IOException {
        try (ParquetWriter<User> writer = new UserToParquetConverter.UserParquetWriter(new Path(fileOutputPath),SCHEMA)) {
            for (User user : users) {
                writer.write(user);
            }
        }
    }
    private static class UserParquetWriter extends ParquetWriter<User>{
        public UserParquetWriter(Path file, MessageType schema) throws IOException {
            super(file, new UserToParquetConverter.UserWriteSupport(schema));
        }
    }
    private static class UserWriteSupport extends WriteSupport<User> {
        private MessageType schema;

        private RecordConsumer recordConsumer;

        public UserWriteSupport(MessageType schema){this.schema = schema;}

        @Override
        public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
            return new WriteContext(schema, new java.util.HashMap<>());
        }
        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }
        @Override
        public void write(User user){
            recordConsumer.startMessage();
            writeOptionalStringField("userEid", user.getUserEid());
            writeStringField("entityStatus", user.getEntityStatus());
            writeOptionalDateStringField("modDate", user.getModDate());
            writeOptionalDateStringField("regDate", user.getRegDate());
            writeOptionalStringField("authCode", user.getAuthCode());
            writeOptionalDateStringField("birthDay", user.getBirthDay());
            writeStringField("centerCode", user.getCenterCode());
            writeStringField("deptCode", user.getDeptCode());
            writeStringField("email", user.getEmail());
            writeStringField("fax", user.getFax());
            writeOptionalDateStringField("hireDate", user.getHireDate());
            writeStringField("homeAddr", user.getHomeAddr());
            writeStringField("homeTel", user.getHomeTel());
            writeStringField("id", user.getId());
            writeStringField("lastEduCode", user.getLastEduCode());
            writeOptionalDateStringField("leaveDate", user.getLeaveDate());
            writeStringField("mobile", user.getMobile());
            writeStringField("name", user.getName());
            writeStringField("note", user.getNote());
            writeStringField("password", user.getPassword());
            writeStringField("positionCode", user.getPositionCode());
            writeStringField("rankCode", user.getRankCode());
            writeStringField("resignReason", user.getResignReason());
            writeStringField("teamCode", user.getTeamCode());
            writeStringField("unusedReasonCode", user.getUnusedReasonCode());
            writeStringField("usable", user.getUsable());
            writeOptionalDateStringField("weddingDate", user.getWeddingDate());
            writeStringField("ctiExtension", user.getCtiExtension());
            writeStringField("ctiLoginId", user.getCtiLoginId());
            writeStringField("ctiYn", user.getCtiYn());
            writeStringField("ipAddr", user.getIpAddr());
            writeStringField("workStateCode", user.getWorkStateCode());
            writeStringField("contractStateCode", user.getContractStateCode());
            writeOptionalStringField("modUserEntityId", user.getModUserEntityId());
            writeOptionalStringField("regUserEntityId", user.getRegUserEntityId());
            writeStringField("token", user.getToken());
            writeStringField("recManageYn", user.getRecManageYn());
            writeStringField("recCounselYn", user.getRecCounselYn());
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
