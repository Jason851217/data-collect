package com.huaxin.datacollect.bean;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class CollectPoint implements Serializable {
    private Integer collectingDataId;
    private Integer orgId;
    private String dataName;
    private String dataNum;
    private String dataFormula;
    private Integer orderId;
}
