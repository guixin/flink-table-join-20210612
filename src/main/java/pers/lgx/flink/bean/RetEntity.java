package pers.lgx.flink.bean;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

public class RetEntity implements BasicInfo {
    private static final long serialVersionUID = 1015491449091782451L;

    private Integer orderId;
    private Integer productId;
    private String productName;

    public Integer getOrderId() {
        return orderId;
    }

    public RetEntity setOrderId(Integer orderId) {
        this.orderId = orderId;
        return this;
    }

    public Integer getProductId() {
        return productId;
    }

    public RetEntity setProductId(Integer productId) {
        this.productId = productId;
        return this;
    }

    public String getProductName() {
        return productName;
    }

    public RetEntity setProductName(String productName) {
        this.productName = productName;
        return this;
    }

    @Override
    public Row of() {
        return null;
    }

    @Override
    public TypeInformation<Row> typeInfo() {
        return null;
    }

    @Override
    public String row2String(Row row) {
        RetEntity entity = new RetEntity();
        entity.setOrderId((Integer) row.getField(0));
        entity.setProductId((Integer) row.getField(1));
        entity.setProductName((String) row.getField(2));
        return toJson(entity);
    }
}
