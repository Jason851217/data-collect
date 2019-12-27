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
import org.springframework.boot.jackson.JsonObjectSerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.DigestUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
        List<CollectPoint> collectPoints = cahceBuilder.get(COLLECT_POINTS);
        if (collectPoints == null || collectPoints.isEmpty()) {
            log.info(">>>>>>>>没有采集点");
            return;
        }
        log.info(">>>>>>>>开始处理");
        log.info(">>>>>>>>本次待处理数据条数：{}", pointDatas.size());
        log.info(">>>>>>>>当前采集点个数：{}", collectPoints.size());


        List<Object[]> batchArgs = buildArgs(pointDatas, collectPoints);
        byte[] bytes = objectMapper.writeValueAsBytes(batchArgs);
        String s = DigestUtils.md5DigestAsHex(bytes);
        if(s.equals(str)){
            return ;
        }else{
            str=s;
            log.info(">>>>>>>>当前匹配采集点个数：{}", batchArgs.size());
            if (batchArgs.size() > 0) {
                batchStoreData(batchArgs);
                Optional<PointData> maxPointData = pointDatas.stream().max((x, y) -> x.getDataTime().compareTo(y.getDataTime()));
                if (maxPointData.isPresent()) {
                    lastProcessDate = maxPointData.get().getDataTime().toString();
                    log.info(">>>>>>>>本次数据最大时间点：{}", lastProcessDate);
                }
            }
            log.info(">>>>>>>>结束处理");
        }

    }

    /**
     * 匹配采集点数据，构造sql参数集合
     *
     * @param pointDatas    数据集合
     * @param collectPoints 采集点集合
     * @return
     */
    private List<Object[]> buildArgs(List<PointData> pointDatas, List<CollectPoint> collectPoints) {
        List<Object[]> batchArgs = new ArrayList<Object[]>();
        for (CollectPoint point : collectPoints) {
            String dataFormula = point.getDataFormula();
            for (PointData data : pointDatas) {
                String tagName = data.getTagName();
                if (StringUtils.strip(tagName).equals(StringUtils.strip(dataFormula))) {
                    Integer collectingDataId = point.getCollectingDataId();
                    Date dataTime = data.getDataTime();
                    Double value = data.getValue();
                    batchArgs.add(new Object[]{collectingDataId, dataTime, value});
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
//        String sql = "select DateTime,TagName,Value from v_AnalogLive where  DATEDIFF(ms,DateTime,enddate) CONVERT(varchar(23), DateTime, 25) > ?";
//        String sql = "select DateTime,TagName,Value from v_AnalogLive where  DATEDIFF(ms,?,CONVERT(varchar(23), DateTime, 25))>0";
//        List<PointData> result = sourceJdbcTemplate.query(sql, new Object[]{lastProcessDate}, (rs, rowNum) -> {
//            PointData pointData = PointData.builder()
//                    .dataTime(rs.getTimestamp("DateTime"))
//                    .tagName(rs.getString("TagName"))
//                    .value(rs.getDouble("Value"))
//                    .build();
//            return pointData;
//        });
        String sql = "select DateTime,TagName,Value from v_AnalogLive where CONVERT(varchar(10), DateTime, 120) =? ";
        List<PointData> result = sourceJdbcTemplate.query(sql, new Object[]{LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))},(rs, rowNum) -> {
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
