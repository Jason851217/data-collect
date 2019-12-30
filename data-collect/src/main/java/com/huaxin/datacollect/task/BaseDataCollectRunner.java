package com.huaxin.datacollect.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.huaxin.datacollect.bean.CollectPoint;
import com.huaxin.datacollect.bean.PointData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
@Slf4j
public class BaseDataCollectRunner implements CommandLineRunner {

    private static final String COLLECT_POINTS = "collectPoints";

    private ObjectMapper objectMapper = new ObjectMapper();

    //将采集点放入本地缓存
    private LoadingCache<String, List<CollectPoint>> cahceBuilder = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build(new CacheLoader<String, List<CollectPoint>>() {
                @Override
                public List<CollectPoint> load(String key) throws Exception {
                    return getCollectPoints();
                }
            });

    private String str;


    @Autowired
    @Qualifier("sourceJdbcTemplate")
    private JdbcTemplate sourceJdbcTemplate;

    @Autowired
    @Qualifier("targetJdbcTemplate")
    private JdbcTemplate targetJdbcTemplate;

    private String lastProcessDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

    @Override
    public void run(String... args) throws Exception {
        new Thread(() -> {
            while (true) {
                try {
                    process();
                    TimeUnit.SECONDS.sleep(1);
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        }).start();

    }

    public void process() throws Exception {
        //读取实时数据
        List<PointData> pointDatas = readDatas();
        if (pointDatas == null || pointDatas.isEmpty()) {
            log.info(">>>>>>>>本次没有待处理数据");
            return;
        }
        //获取采集点
        List<CollectPoint> collectPoints = cahceBuilder.get(COLLECT_POINTS);
        if (collectPoints == null || collectPoints.isEmpty()) {
            log.info(">>>>>>>>没有采集点");
            return;
        }
        Map<String, CollectPoint> collectPointMap = collectPoints.stream().collect(Collectors.toMap(data -> StringUtils.strip(data.getDataFormula()), data -> data));
        log.info(">>>>>>>>开始处理");
        List<Object[]> batchArgs = buildArgs(pointDatas, collectPointMap);
        if (batchArgs.size() > 0) {
            batchStoreData(batchArgs);
            // Optional<PointData> maxPointData = pointDatas.stream().max((x, y) -> x.getDataTime().compareTo(y.getDataTime()));
            // if (maxPointData.isPresent()) {
            //     lastProcessDate = maxPointData.get().getDataTime().toString();
            //     log.info(">>>>>>>>本次数据最大时间点：{}", lastProcessDate);
            // }
        }
        log.info("本次处理数据条数：{}", batchArgs.size());
        log.info(">>>>>>>>结束处理");
    }

    private boolean isExist(Integer dataId, Date dateTime) {
        String sql = "select count(*) from RDMS_COLLECTING_BASE_VALUE where DATA_ID =? and to_char(DATA_TIME, 'yyyymmdd hh24:mi:ss.ff3')=?";
        Integer rows = targetJdbcTemplate.queryForObject(sql, new Object[]{dataId, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(dateTime)}, Integer.class);
        return rows != null && rows.intValue() > 0;
    }

    /**
     * 匹配采集点数据，构造sql参数集合
     *
     * @param pointDatas      数据集合
     * @param collectPointMap 采集点集合
     * @return
     */
    private List<Object[]> buildArgs(List<PointData> pointDatas, Map<String, CollectPoint> collectPointMap) {
        List<Object[]> batchArgs = new ArrayList<Object[]>();
        for (PointData pointData : pointDatas) {
            String tagName = StringUtils.strip(pointData.getTagName());
            if (collectPointMap.containsKey(tagName)) {
                CollectPoint collectPoint = collectPointMap.get(tagName);
                Integer dataId = collectPoint.getCollectingDataId();
                Date dataTime = pointData.getDataTime();
                if (!isExist(dataId, dataTime)) {
                    Double value = pointData.getValue();
                    batchArgs.add(new Object[]{dataId, dataTime, value});
                }
            }
        }
        return batchArgs;
    }

    /**
     * 数据存储
     *
     * @param batchArgs
     */
    private void batchStoreData(List<Object[]> batchArgs) {
        String sql = "insert into RDMS_COLLECTING_BASE_VALUE(DATA_ID,DATA_TIME,DATA_VALUE) values(?,?,?)";
        targetJdbcTemplate.batchUpdate(sql, batchArgs);
    }

    /**
     * 读取实时数据
     */
    private List<PointData> readDatas() throws JsonProcessingException {
        // String sql = "select DateTime,TagName,Value from v_AnalogLive where CONVERT(varchar(10), DateTime, 120) =? ";
        String sql = "select DateTime,TagName,Value from v_AnalogLive";
        // List<PointData> result = sourceJdbcTemplate.query(sql, new Object[]{LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))}, (rs, rowNum) -> {
        List<PointData> result = sourceJdbcTemplate.query(sql, (rs, rowNum) -> {
            PointData pointData = PointData.builder()
                    .dataTime(rs.getTimestamp("DateTime", Calendar.getInstance(Locale.SIMPLIFIED_CHINESE)))
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
}
