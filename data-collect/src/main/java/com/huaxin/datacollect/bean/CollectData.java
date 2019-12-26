package com.huaxin.datacollect.bean;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
@Builder
public class CollectData implements Serializable {
    private Integer dataId;
    private Date dateTime;
    private Double dataValue;

}
