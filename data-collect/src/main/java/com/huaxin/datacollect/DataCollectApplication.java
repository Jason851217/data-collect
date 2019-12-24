package com.huaxin.datacollect;

import com.huaxin.datacollect.bean.CollectPoint;
import com.huaxin.datacollect.config.DataSourceProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.sql.*;
import java.util.*;
import java.util.Date;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class DataCollectApplication {

    private static DataSourceProperties dataSourceProperties = DataSourceProperties.getInstance();

    public static void main(String[] args) {
        SpringApplication.run(DataCollectApplication.class, args);
    }

    @Scheduled(fixedDelay = 2000)
    public void fixedDelay() {
        List<CollectPoint> collectItems = getCollectPoints();
    }

    private List<CollectPoint> getCollectPoints() {
        Connection targetConn = getTargetConnection();
        Statement stmt = null;
        ResultSet rs = null;
        String sql = "select COLLECTING_DATA_ID,ORGANIZATION_ID,DATA_NAME,DATA_NUM,DATA_FORMULA,ORDER_ID from rdms_collecting_data order by ORDER_ID";
        List<CollectPoint> points = new ArrayList<CollectPoint>();
        try {
            stmt = targetConn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                CollectPoint collectPoint = CollectPoint.builder()
                        .collectingDataId(rs.getInt("COLLECTING_DATA_ID"))
                        .orgId(rs.getInt("ORGANIZATION_ID"))
                        .dataName(rs.getString("DATA_NAME"))
                        .dataNum(rs.getString("DATA_NUM"))
                        .dataFormula(rs.getString("DATA_FORMULA"))
                        .orderId(rs.getInt("ORDER_ID")).build();
                points.add(collectPoint);
            }
            log.info("points:{}", points);
            log.info("points size:{}", points.size());
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            release(targetConn, rs, stmt);
        }
        return points;
    }


    public void release(Connection conn, ResultSet rs, Statement stmt) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public Connection getTargetConnection() {
        Connection targetConn = null;
        try {
            Class.forName(dataSourceProperties.getTargetDriver());
            targetConn = DriverManager.getConnection(dataSourceProperties.getTargetUrl(), dataSourceProperties.getTargetUsername(),
                    dataSourceProperties.getTargetPassword());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return targetConn;

    }

    public Connection getSrouceConnection() {
        Connection sourceConn = null;
        try {
            Class.forName(dataSourceProperties.getSourceDriver());
            sourceConn = DriverManager.getConnection(dataSourceProperties.getSourceUrl(), dataSourceProperties.getSourceUsername(),
                    dataSourceProperties.getSourcePassword());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sourceConn;
    }

}
