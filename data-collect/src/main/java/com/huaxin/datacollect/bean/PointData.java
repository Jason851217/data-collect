package com.huaxin.datacollect.bean;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
@Builder
public class PointData implements Serializable {
    private Date dataTime;
    private String tagName;
    private Double value;
}
