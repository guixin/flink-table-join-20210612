package pers.lgx.flink.schema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.lgx.flink.bean.BasicInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RowSchema implements DeserializationSchema<Row>, SerializationSchema<Row> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RowSchema.class);
    private BasicInfo basicInfo;

    public RowSchema(BasicInfo basicInfo) {
        this.basicInfo = basicInfo;
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        String value = new String(message, StandardCharsets.UTF_8);
        LOGGER.info("input string {}", value);
        BasicInfo entity = basicInfo.instance(value);
        LOGGER.info("row: {}", entity.of());
        return entity.of();
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(Row element) {
        return basicInfo.row2String(element).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return basicInfo.typeInfo();
    }
}
