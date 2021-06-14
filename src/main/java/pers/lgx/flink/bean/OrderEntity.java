package pers.lgx.flink.bean;

import com.alibaba.fastjson.annotation.JSONField;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class OrderEntity implements BasicInfo {
    private static final long serialVersionUID = 6605732870374375909L;

    @JSONField(name = "order_id")
    private int id;

    @JSONField(name = "product_id")
    private int product;

    @JSONField(name = "created_at")
    private long createdAt;

    public int getId() {
        return id;
    }

    public OrderEntity setId(int id) {
        this.id = id;
        return this;
    }

    public int getProduct() {
        return product;
    }

    public OrderEntity setProduct(int product) {
        this.product = product;
        return this;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public OrderEntity setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    @Override
    public Row of() {
        return Row.of(this.id, this.product, this.createdAt);
    }

    @Override
    public TypeInformation<Row> typeInfo() {
        return new RowTypeInfo(new TypeInformation[]{
                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
        },
                new String[]{"order_id", "product_id", "created_at"});
    }

    @Override
    public String row2String(Row row) {
        OrderEntity entity = new OrderEntity();
        entity.setId((Integer) row.getField(0));
        entity.setProduct((Integer) row.getField(1));
        entity.setCreatedAt((Long) row.getField(2));
        return toJson(entity);
    }
}
