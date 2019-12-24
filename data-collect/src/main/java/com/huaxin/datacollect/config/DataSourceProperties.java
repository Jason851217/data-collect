package com.huaxin.datacollect.config;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

@Getter
@ToString
@Builder
public class DataSourceProperties {

    private static final String fileName = "application.properties";

    private static Properties props;

    private static volatile DataSourceProperties instance;

    static {
        props = new Properties();
        try {
            props.load(new InputStreamReader(DataSourceProperties.class.getClassLoader().getResourceAsStream(fileName), "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String sourceDriver;
    private String sourceUrl;
    private String sourceUsername;
    private String sourcePassword;

    private String targetDriver;
    private String targetUrl;
    private String targetUsername;
    private String targetPassword;

    private DataSourceProperties(String sourceDriver, String sourceUrl, String sourceUsername, String sourcePassword, String targetDriver, String targetUrl, String targetUsername, String targetPassword) {
        this.sourceDriver = sourceDriver;
        this.sourceUrl = sourceUrl;
        this.sourceUsername = sourceUsername;
        this.sourcePassword = sourcePassword;
        this.targetDriver = targetDriver;
        this.targetUrl = targetUrl;
        this.targetUsername = targetUsername;
        this.targetPassword = targetPassword;
    }

    public static DataSourceProperties getInstance() {
        if (instance == null) {
            synchronized (DataSourceProperties.class) {
                if (instance == null) {
                    String sourceDriver = props.getProperty("source.db.driver");
                    String sourceUrl = props.getProperty("source.db.url");
                    String sourceUsername = props.getProperty("source.db.username");
                    String sourcePassword = props.getProperty("source.db.password");

                    String targetDriver = props.getProperty("target.db.driver");
                    String targetUrl = props.getProperty("target.db.url");
                    String targetUsername = props.getProperty("target.db.username");
                    String targetPassword = props.getProperty("target.db.password");

                    return new DataSourceProperties(sourceDriver, sourceUrl, sourceUsername, sourcePassword, targetDriver, targetUrl, targetUsername, targetPassword);
                }
            }
        }
        return instance;
    }

}