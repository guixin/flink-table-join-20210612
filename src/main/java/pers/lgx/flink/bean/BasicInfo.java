package pers.lgx.flink.bean;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import java.io.Serializable;

public interface BasicInfo extends Serializable {
    Row of();

    TypeInformation<Row> typeInfo();

    String row2String(Row row);

    default String toJson(BasicInfo basicInfo) {
        return JSON.toJSONString(basicInfo);
    }

    default BasicInfo instance(String data) {
        return JSON.parseObject(data, this.getClass());
    }
}
