package com.huaxin.datacollect;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.huaxin.datacollect.bean.CollectPoint;
import com.huaxin.datacollect.bean.PointData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.StringUtils;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
// @EnableScheduling
@Slf4j
public class DataCollectApplication {

    private static final String COLLECT_POINTS = "collectPoints";

    @Autowired
    @Qualifier("sourceJdbcTemplate")
    private JdbcTemplate sourceJdbcTemplate;

    @Autowired
    @Qualifier("targetJdbcTemplate")
    private JdbcTemplate targetJdbcTemplate;

    private Date lastProcessDate = new Date();//需要存储到DB

    public static void main(String[] args) {
        SpringApplication.run(DataCollectApplication.class, args);
    }


    @Bean
    public CommandLineRunner runner() {
        return (args) -> {
            // List<CollectPoint> collectPoints = getCollectPoints();
            // Cache<String,String> cache = CacheBuilder.newBuilder()
            //         .maximumSize(2)
            //         .expireAfterWrite(3,TimeUnit.SECONDS)
            //         .build();
            // cache.put("collectPoints","value1");

            //将采集点放入本地缓存
            LoadingCache<String, List<CollectPoint>> cahceBuilder = CacheBuilder.newBuilder()
                    .expireAfterAccess(1, TimeUnit.DAYS)
                    .build(new CacheLoader<String, List<CollectPoint>>() {
                        @Override
                        public List<CollectPoint> load(String key) throws Exception {
                            return getCollectPoints();
                        }
                    });

            while (true) {
                //读取实时数据
                List<PointData> pointDatas = readDatas();
                if (pointDatas != null && pointDatas.size() > 0) {
                    List<Object[]> batchArgs = new ArrayList<Object[]>();
                    List<CollectPoint> collectPoints = cahceBuilder.get(COLLECT_POINTS);
                    if (collectPoints.size() > 0) {
                        for (CollectPoint point : collectPoints) {
                            String dataFormula = point.getDataFormula();
                            for (PointData data : pointDatas) {
                                String tagName = data.getTagName();
                                if (tagName.equals(dataFormula)) {
                                    Integer collectingDataId = point.getCollectingDataId();
                                    Date dataTime = data.getDataTime();
                                    Double value = data.getValue();
                                    batchArgs.add(new Object[]{collectingDataId, dataTime, value});
                                    // storeData(collectingDataId, dataTime, value);
                                }
                            }
                        }
                        if (batchArgs.size() > 0) {
                            batchStoreData(batchArgs);
                            Optional<PointData> maxPointData = pointDatas.stream().max((x, y) -> x.compareTo(y));
                            if (maxPointData.isPresent()) {
                                lastProcessDate = maxPointData.get().getDataTime();
                            }
                        }
                    }
                }
                TimeUnit.SECONDS.sleep(1);
            }
        };
    }

    private void batchStoreData(List<Object[]> batchArgs) {
        String sql = "insert into rdms_collecting_data_value(CollectingDataid,DataTime,Value) values(?,?,?)";
        targetJdbcTemplate.batchUpdate(sql, batchArgs);
    }

    private void storeData(Integer collectingDataId, Date dataTime, Double value) {
        String sql = "insert into rdms_collecting_data_value(CollectingDataid,DataTime,Value) values(?,?,?)";
        targetJdbcTemplate.update(sql, collectingDataId, dataTime, value);

    }

    /**
     * 读取实时数据
     */
    public List<PointData> readDatas() {
        String sql = "select DateTime,TagName,Value from v_AnalogLive where DateTime > ?";
        List<PointData> result = sourceJdbcTemplate.query(sql, new Object[]{lastProcessDate}, (rs, rowNum) -> {
            PointData pointData = PointData.builder()
                    .dataTime(rs.getDate("DateTime"))
                    .tagName(rs.getString("TagName"))
                    .value(rs.getDouble("Value"))
                    .build();
            return pointData;
        });
        return result;
    }

    /**
     * 读取采集点
     *
     * @return
     */
    private List<CollectPoint> getCollectPoints() {
        String sql = "select COLLECTING_DATA_ID,ORGANIZATION_ID,DATA_NAME,DATA_NUM,DATA_FORMULA,ORDER_ID from rdms_collecting_data order by ORDER_ID";
        List<CollectPoint> points = targetJdbcTemplate.query(sql, (rs, rowNum) -> {
            CollectPoint collectPoint = CollectPoint.builder()
                    .collectingDataId(rs.getInt("COLLECTING_DATA_ID"))
                    .orgId(rs.getInt("ORGANIZATION_ID"))
                    .dataName(rs.getString("DATA_NAME"))
                    .dataNum(rs.getString("DATA_NUM"))
                    .dataFormula(rs.getString("DATA_FORMULA"))
                    .orderId(rs.getInt("ORDER_ID")).build();
            return collectPoint;
        });
        return points;
    }


    // @Scheduled(fixedDelay = 2000)
    // public void fixedDelay() {
    //     List<CollectPoint> collectItems = getCollectPoints();
    // }
}
