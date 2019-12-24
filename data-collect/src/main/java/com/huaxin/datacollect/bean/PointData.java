package com.huaxin.datacollect.bean;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
@Builder
public class PointData implements Serializable,Comparable<PointData> {
    private Date dataTime;
    private String tagName;
    private Double value;


    @Override
    public int compareTo(PointData o) {
        return this.getDataTime().compareTo(o.getDataTime());
    }
}
