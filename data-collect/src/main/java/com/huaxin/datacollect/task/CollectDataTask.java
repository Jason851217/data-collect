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
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 *
 */
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
        String sql = "select DATA_ID,max(DATA_TIME) as lastTime, DATA_VALUE from RDMS_COLLECTING_DATA_VALUE where  DATA_TIME > to_date(?,'yyyy-mm-dd hh24:mi:ss') and  DATA_TIME <=to_date(?,'yyyy-mm-dd hh24:mi:ss') group by DATA_ID";
        List<CollectData> result = targetJdbcTemplate.query(sql, new Object[]{pre5DateTime, curTime}, (rs, i) -> {
            CollectData data = CollectData.builder().dataId(rs.getInt("DATA_ID")).dateTime(rs.getTimestamp("lastTime")).dataValue(rs.getDouble("DATA_VALUE")).build();
            return data;
        });
        if (result != null && result.size() > 0) {
            List<Object[]> batchArgs = new ArrayList<Object[]>();
            result.forEach((data -> {
                Integer collectingDataId = data.getDataId();
                Double value = data.getDataValue();
                batchArgs.add(new Object[]{collectingDataId, now, value});
            }));
            if (batchArgs.size() > 0) {
                String batchInsertSql = "insert into RDMS_COLLECTING_DATA_VALUE(DATA_ID,DATA_TIME,DATA_VALUE) values(?,?,?)";
                targetJdbcTemplate.batchUpdate(batchInsertSql, batchArgs);
            }
        }
    }
}
