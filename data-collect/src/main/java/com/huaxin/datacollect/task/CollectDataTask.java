package com.huaxin.datacollect.task;

import com.huaxin.datacollect.bean.CollectData;
import com.huaxin.datacollect.bean.PointData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;


@Component
public class CollectDataTask {

    @Autowired
    @Qualifier("targetJdbcTemplate")
    private JdbcTemplate targetJdbcTemplate;

    @Scheduled(cron = "${test-cron}")
    public void print() {
        LocalDateTime now = LocalDateTime.now();
        String curTime = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime pre5DateTime = now.plusMinutes(-5);
        String pre5Time = pre5DateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.println("pre5Time:" + pre5Time + ",curTime:" + curTime);
        // select * from up_date where update < to_date('2007-09-07 00:00:00','yyyy-mm-dd hh24:mi:ss') and update > to_date('2007-07-07 00:00:00','yyyy-mm-dd hh24:mi:ss')
        // String sql = "insert into RDMS_COLLECTING_DATA_VALUE(DATA_ID,DATA_TIME,DATA_VALUE) values(?,?,?)";
        String sql = "select DATA_ID,DATA_TIME,DATA_VALUE from RDMS_COLLECTING_BASE_VALUE where  DATA_TIME > to_date(?,'yyyy-mm-dd hh24:mi:ss') and  DATA_TIME <=to_date(?,'yyyy-mm-dd hh24:mi:ss') ";
        List<CollectData> result = targetJdbcTemplate.query(sql, new Object[]{pre5Time, curTime}, (rs, i) -> {
            CollectData data = CollectData.builder().dataId(rs.getInt("DATA_ID")).dateTime(rs.getTimestamp("DATA_TIME")).dataValue(rs.getDouble("DATA_VALUE")).build();
            return data;
        });
        if (result != null && result.size() > 0) {
            Map<Integer, CollectData> dataMap = result.stream().collect(Collectors.groupingBy(CollectData::getDataId,
                    Collectors.collectingAndThen(
                            Collectors.reducing(BinaryOperator.maxBy(Comparator.comparing(CollectData::getDateTime))),
                            Optional::get)));
            List<Object[]> batchArgs = new ArrayList<Object[]>();
            dataMap.forEach((key, data) -> {
                Integer collectingDataId = key;
                Double value = data.getDataValue();
                batchArgs.add(new Object[]{collectingDataId, localDateTime2Date(now), value});
            });
            if (batchArgs.size() > 0) {
                String batchInsertSql = "insert into RDMS_COLLECTING_DATA_VALUE(DATA_ID,DATA_TIME,DATA_VALUE) values(?,?,?)";
                targetJdbcTemplate.batchUpdate(batchInsertSql, batchArgs);
            }
        }
    }

    public Date localDateTime2Date( LocalDateTime localDateTime){
        ZoneId zoneId = ZoneId.systemDefault();
        ZonedDateTime zdt = localDateTime.atZone(zoneId);//Combines this date-time with a time-zone to create a  ZonedDateTime.
        Date date = Date.from(zdt.toInstant());
        System.out.println(date.toString());//Tue Mar 27 14:17:17 CST 2018
        return date;
    }
}
