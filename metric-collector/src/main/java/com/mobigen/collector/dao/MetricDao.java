package com.mobigen.collector.dao;

import com.mobigen.collector.dto.MetricConfig;
import com.mobigen.collector.dto.TableConfig;
import com.mobigen.collector.service.DBUtil;
import com.mobigen.collector.service.ErrorLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Component
public class MetricDao {
    Logger logger = LoggerFactory.getLogger(MetricDao.class);

    @Autowired
    DBUtil dbUtil;
    @Value("${jdbc.fetchSize}")
    int BATCH_SIZE;

    @Autowired
    ErrorLogManager errorLogManager;

    /**
     * 다수 쿼리 처리 메소드
     *
     * @param sqls
     */
    public void executeQuery(List<String> sqls) {
        Statement stmt = null;
        Connection conn = null;
        try {
            if(sqls != null && sqls.size() > 0){
//                logger.info("DB 작업 시작");
                conn = dbUtil.getConnection();
                stmt = conn.createStatement();

                // 쿼리 실행
                for(String sql : sqls){
                    stmt.addBatch(String.valueOf(sql));
                }
                stmt.executeBatch();

                dbUtil.commit(conn);
            }
        } catch (Exception e){
            logger.error("DB 쿼리 수행 실패", e);
            dbUtil.rollback(conn);

            errorLogManager.writeErrorSql(e, sqls);
        } finally {
            close(stmt, conn);
        }
    }

    /**
     * 단일 쿼리 처리 메소드
     *
     * @param sql
     */
    public void executeQuery(String sql) {
        Statement stmt = null;
        Connection conn = null;
        try {
            if(sql != null && sql.length() > 0){
//                logger.info("DB 작업 시작");
                conn = dbUtil.getConnection();
                stmt = conn.createStatement();

                // 쿼리 실행
                stmt.executeQuery(sql);

                dbUtil.commit(conn);
            }
        } catch (Exception e){
            logger.error("DB 쿼리 수행 실패", e);
            dbUtil.rollback(conn);
            errorLogManager.writeErrorSql(e, sql);
        } finally {
            close(stmt, conn);
        }
    }

    public List<MetricConfig> selectMetricConfig() {
        Statement stmt = null;
        Connection conn = null;
        ResultSet rs ;
        List<MetricConfig> metricConfigs = new ArrayList<>();

        try {
            String sql = "SELECT * FROM METRIC_CONFIG";

            conn = dbUtil.getConnectionForSelect();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);

            while(rs.next()){
                MetricConfig metricConfig = new MetricConfig();

                metricConfig.setTable_name(rs.getString("table_name"));
                // 동적 필드가 아닌 경우 metric_name 그대로 가져오기
                String dynamic_flag = rs.getString("dynamic_flag");
                if("N".equals(dynamic_flag)){
                    metricConfig.setMetric_name(rs.getString("metric_name"));
                // 동적 필드인 경우 정규식 가져오기
                } else if("Y".equals(dynamic_flag)){
                    metricConfig.setMetric_name(rs.getString("reg_pattern"));
                }
                metricConfig.setColumn_name(rs.getString("column_name"));
                metricConfig.setCollect_flag(rs.getString("collect_flag"));
                metricConfig.setDynamic_flag(dynamic_flag);

                // 결과 List에 추가
                metricConfigs.add(metricConfig);
            }

            logger.info("METRIC_CONFIG 테이블 정보 로드 완료");
        } catch (Exception e){
            logger.error("METRIC_CONFIG 테이블 정보 로드 실패", e);
        } finally {
            close(stmt, conn);
        }

        return metricConfigs;
    }


    public List<TableConfig> selectTableConfig() {
        Statement stmt = null;
        Connection conn = null;
        ResultSet rs ;
        List<TableConfig> tableConfigs = new ArrayList<>();

        try {
            String sql = "SELECT * FROM TABLE_CONFIG";

            conn = dbUtil.getConnectionForSelect();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);

            while(rs.next()){
                TableConfig tableConfig = new TableConfig();

                tableConfig.setTable_name(rs.getString("table_name"));
                tableConfig.setInsert_flag(rs.getString("insert_flag"));
                tableConfig.setDynamic_classify_column_name(rs.getString("dynamic_classify_column_name"));

                tableConfigs.add(tableConfig);
            }

            logger.info("TABLE_CONFIG 테이블 정보 로드 완료");
        } catch (Exception e){
            logger.error("TABLE_CONFIG 테이블 정보 로드 실패", e);
        } finally {
            close(stmt, conn);
        }

        return tableConfigs;
    }

    public void close(Statement stmt, Connection conn){
        try {
            if(stmt!=null) stmt.close();
            if(conn!=null) dbUtil.close(conn);
        } catch (Exception e) {
            logger.error("DB 자원 close 실패", e);
        }
    }

    public String selectTimestamp(String selectSql) {
        Statement stmt = null;
        Connection conn = null;
        ResultSet rs ;
        String timestamp = "";

        try {
            conn = dbUtil.getConnectionForSelect();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(selectSql);

            while(rs.next()){
                timestamp = rs.getString("timestamp");
            }

        } catch (Exception e){
            logger.error("Timestamp 조회 실패", e);
            errorLogManager.writeErrorSql(e, selectSql);
        } finally {
            close(stmt, conn);
        }

        return timestamp;
    }
}
