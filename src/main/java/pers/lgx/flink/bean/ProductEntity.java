package pers.lgx.flink.bean;

import com.alibaba.fastjson.annotation.JSONField;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class ProductEntity implements BasicInfo {
    private static final long serialVersionUID = -4636420939348732185L;

    @JSONField(name = "product_id")
    private int id;
    @JSONField(name = "product_name")
    private String name;
    @JSONField(name = "created_at")
    private long createdAt;

    public int getId() {
        return id;
    }

    public ProductEntity setId(int id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public ProductEntity setName(String name) {
        this.name = name;
        return this;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public ProductEntity setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    @Override
    public Row of() {
        return Row.of(this.id, this.name, this.createdAt);
    }

    @Override
    public TypeInformation<Row> typeInfo() {
        return new RowTypeInfo(new TypeInformation[] {
                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
        },
                new String[] {"product_id", "product_name", "created_at"});
    }

    @Override
    public String row2String(Row row) {
        ProductEntity entity = new ProductEntity();
        entity.setId((Integer) row.getField(0));
        entity.setName((String) row.getField(1));
        entity.setCreatedAt((Long) row.getField(2));
        return toJson(entity);
    }
}
