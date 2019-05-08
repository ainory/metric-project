package com.mobigen.collector.service;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;

@Service
public class DBUtil {
    Logger logger = LoggerFactory.getLogger(MetricCollector.class);

    @Autowired
    @Qualifier("dataSource")
    private HikariDataSource dataSource;

    public Connection getConnection() throws Exception{
        Connection conn;
        try {
            conn = dataSource.getConnection();
//            conn.setAutoCommit(false);
        } catch (Exception e) {
            logger.error("Connection 얻기 실패");
            throw e;
        }

        return conn;
    }

    public void commit(Connection conn){
        try {
            if(conn!=null) conn.commit();
        } catch (Exception e) {
            logger.error("Commit 실패", e);
        }
    }

    public void rollback(Connection conn){
        try {
            if(conn!=null) conn.rollback();
        } catch (Exception e) {
            logger.error("Rollback 실패" ,e);
        }
    }

    public void close(Connection conn){
        try {
            if(conn!=null) {
                conn.close();
            }
        } catch (Exception e) {
            logger.error("Connection Close 실패" ,e);
        }
    }
}
