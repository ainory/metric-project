package com.mobigen.collector.service;

import com.mobigen.collector.dao.MetricDao;
import com.mobigen.collector.dto.MetricConfig;
import com.mobigen.collector.dto.MetricInfo;
import com.mobigen.collector.dto.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DBProcessor {
    Logger logger = LoggerFactory.getLogger(DBProcessor.class);

    @Value("${kafka.bootstrap.servers}")
    String BOOTSTRAP_SERVER;
    @Value("${jdbc.bulkInsertSize}")
    int BATCH_SIZE;

    @Autowired
    MetricDao metricDao;

    private List<MetricConfig> metricConfigs;
    private Map<String, TableConfig> tableConfigs;

    /**
     * Config 테이블 정보 가져와서 저장
     */
    @PostConstruct
    public void init(){
        metricConfigs = getMetricConfig();
        tableConfigs = getTableConfig();
    }

    /**
     * Metric_Config 테이블 가져오는 메소드
     * (필드에 대한 정보들 저장되어있는 테이블)
     *
     * @return
     */
    private List<MetricConfig>  getMetricConfig() {
        List<MetricConfig> metricConfigs = null;
        try{
            metricConfigs = metricDao.selectMetricConfig();
        } catch (Exception e){
            logger.error("Table Config 불러오기 실패", e);
        }
        return metricConfigs;
    }

    /**
     * Table_Config 테이블 가져오는 메소드
     * (테이블 정보 저장되어있음)
     *
     * @return Map<테이블명, TableConfig 정보>
     */
    private Map<String, TableConfig>  getTableConfig() {
        Map<String, TableConfig> tableConfig = new HashMap<>();
        try{
            for(TableConfig config : metricDao.selectTableConfig()){
                tableConfig.put(config.getTable_name(), config);
            }
        } catch (Exception e){
            logger.error("Table Config 불러오기 실패", e);
        }

        return tableConfig;
    }

    /**
     * 업데이트, 삽입용 분류하여 DB 처리
     *
     * @param metrics
     */
    @Async("db-executor")
    public void processMetrics(Set<MetricInfo> metrics) {
        if(metrics != null && metrics.size() > 0){
            try{
                Long startTime = System.currentTimeMillis();

                Map<String, String[]> insertMetrics = new HashMap<>();
                Map<String, String[]> updateMetrics = new HashMap<>();

                Long getMetricStart = System.currentTimeMillis();
                Map<String, String[]> metricRows = getMetricRowForSql(metrics);
                Long getMetricEnd = System.currentTimeMillis();
                logger.info("쿼리용 Map 변환 시간: " + (getMetricEnd - getMetricStart) + "ms");

                // INSERT, UPDATE 구분
                for(String keys : metricRows.keySet()){
                    String tableName = keys.split(",")[0];
                    if("Y".equals(tableConfigs.get(tableName).getInsert_flag())){
                        insertMetrics.put(keys, metricRows.get(keys));
                    } else {
                        updateMetrics.put(keys, metricRows.get(keys));
                    }
                }

                // DB 처리
                Long insertStart = System.currentTimeMillis();
                insertMetrics(insertMetrics);
                Long insertEnd = System.currentTimeMillis();
                updateMetrics(updateMetrics);
                Long updateEnd = System.currentTimeMillis();
                logger.info("insert 시간: " + (insertEnd - insertStart) + "ms");
                logger.info("update 시간: " + (updateEnd - insertEnd) + "ms");


                Long endTime = System.currentTimeMillis();
                logger.info("총 DB처리 시간: " + (endTime - startTime) + "ms");

            } catch ( Exception e) {
                logger.error("DB 작업 실패", e);
            } finally {
                logger.info("DB 작업 완료\n");
            }
        }

    }

    /**
     * 쿼리용으로 데이터 가공하는 메소드
     *
     * @param metrics
     * @return Map< 키(tableName, sys_seq, proc_seq, timestamp + 동적필드구분자 ), Metric Value Array>
     */
    private Map<String,String[]> getMetricRowForSql(Set<MetricInfo> metrics) {
        Map<String, List<String>> colsConfig = getColumnsByTable();

        Map<String, String[]> metricRows = null;
        try {

            metricRows = new HashMap<>();
            for (MetricInfo metric : metrics) {

                for (MetricConfig config : metricConfigs) {
//                    String tableName = metric.getTable_name();
                    // RRD는 테이블명 다르기때문에 config에서 가져옴
                    String tableName = config.getTable_name();
                    String metricName = metric.getMetric_name();

                    boolean isMatch;
                    Pattern metircNamePattern = null;

                    // metric_name 패턴 일치 && table_name 일치
                    if ("Y".equals(config.getDynamic_flag())) {
                        metircNamePattern = Pattern.compile(config.getMetric_name());
                        isMatch = metircNamePattern.matcher(metricName).matches()
                                && ("RRD".equals(tableName) || config.getTable_name().equals(tableName));
                        // metric_name 일치 && table_name 일치
                    } else {
                        isMatch = config.getMetric_name().equals(metricName)
                                && ("RRD".equals(tableName) || config.getTable_name().equals(tableName));
                    }

                    if (isMatch) {
                        // 수집 여부 = 'Y'
                        if ("Y".equals(config.getCollect_flag())) {

                            // ------- 1. KEY
                            String keys = tableName + "," + metric.getSystem_seq() + "," + metric.getProcess_seq() + "," + convertFormat(metric.getTimestamp());
                            // 동적 수집 필드인 경우 구분명도 포함
                            String dynamicColName = tableConfigs.get(config.getTable_name()).getDynamic_classify_column_name();
                            String dynamicColVal = "";
                            if ("Y".equals(config.getDynamic_flag())) {
                                Matcher matcher = metircNamePattern.matcher(metric.getMetric_name());
                                while (matcher.find()) {
                                    dynamicColVal = matcher.group(1);
                                }
                                keys += "," + dynamicColVal;
                            }

                            // ------- 2. VALUE 세팅
                            // config에서 가져온 필드명 순서와 같은 인덱스에 삽입
                            List<String> columnInfo = colsConfig.get(tableName);
                            String[] vals = metricRows.get(keys);
                            if (vals == null || vals.length == 0) {
                                vals = new String[columnInfo.size()];
                                Arrays.fill(vals,null);
                            }
                            // key에 해당하는 값
                            int metricIdx = columnInfo.indexOf("system_seq");
                            vals[metricIdx] = metric.getSystem_seq();
                            metricIdx = columnInfo.indexOf("process_seq");
                            vals[metricIdx] = metric.getProcess_seq();
                            metricIdx = columnInfo.indexOf("timestamp");
                            vals[metricIdx] = convertFormat(metric.getTimestamp());
                            // metric 값
                            metricIdx = columnInfo.indexOf(config.getColumn_name()); // metricName X (∵ 동적필드인 경우 이름 다름, config에서 가져오기)
                            vals[metricIdx] = metric.getMetric_value();
                            // 동적필드 구분자 값
                            if(!"".equals(dynamicColVal)){
                                metricIdx = columnInfo.indexOf(dynamicColName);
                                vals[metricIdx] = dynamicColVal;
                            }


                            // ------ 3. key, value 삽입
                            metricRows.put(keys, vals);

                            break;
                        }
                    }
                }

            }
        } catch (Exception e) {
            logger.info("INSERT 작업 실패", e);

            logger.info("INSERT 실패한 Metric 정보: \n" + metrics);
        }
        return metricRows;
    }

    /**
     * INSERT 작업 수행 메소드
     *
     * @param metrics INSERT 할 메트릭 정보들
     */
    private void insertMetrics(Map<String, String[]> metrics) {
        if(metrics != null && metrics.size() > 0){

            List<String> sqls = new ArrayList<>();
            int totBulkCnt = 0;
            int totRowCnt = 0;

            Map<String, List<String>> colsConfig = getColumnsByTable();
            Map<String, List<String>> bulkinsertMap = new HashMap<>();

            try{

                for(Map.Entry<String,String[]> metricRow: metrics.entrySet()) {
                    String tableName = metricRow.getKey().split(",")[0];
                    List<String> colNameList = colsConfig.get(tableName);
                    String[] colValList = metricRow.getValue();

                    // 단일 sql문 생성- null 이 포함되어 있는경우
                    if(Arrays.asList(colValList).contains(null)){
                        String colNames ="";
                        String colVals = "";
                        String duplicateKeys = "";
                        for(int i = 0; i < colNameList.size() ; i++) {
                            colNames +=  " `" + colNameList.get(i) + "`,";
                            colVals +=  (colValList[i] == null )? colValList[i] +"," : "'" + colValList[i] +"',";
                            if(colValList[i] != null){
                                duplicateKeys += " `" + colNameList.get(i) + "`=VALUES(`" +  colNameList.get(i) +"`),";
                            }
                        }

                        colNames = colNames.substring(0, colNames.length()-1);
                        colVals = colVals.substring(0, colVals.length()-1);
                        duplicateKeys = duplicateKeys.substring(0, duplicateKeys.length()-1);
                        String sql = "INSERT INTO " + tableName + "(" + colNames + ")" + " VALUES (" + colVals +")"
                                + " ON DUPLICATE KEY UPDATE " + duplicateKeys;

                        sqls.add(sql);

                    // bulk insert 문을 위한 value 문자열 생성
                    } else {
                        String colVals = "";
                        for(String val : colValList){
                            colVals +=  "'" + val +"',";
                        }
                        colVals = colVals.substring(0, colVals.length()-1);
                        List<String> valueList = bulkinsertMap.get(tableName);
                        if(valueList == null || valueList.size() == 0){
                            valueList = new ArrayList<>();
                        }
                        valueList.add(colVals);
                        bulkinsertMap.put(tableName, valueList);
                    }
                    totRowCnt++;
                }

                // bulk insert 문 생성
                if(bulkinsertMap.size() > 0){
                    for(Map.Entry<String, List<String>> bulkinsert : bulkinsertMap.entrySet()){
                        String tableName = bulkinsert.getKey();
                        List<String> colNameList = colsConfig.get(tableName);
                        List<String> colValList = bulkinsert.getValue();

                        // Column 명 리스트, duplicated Key 구문 생성
                        String colNames ="";
                        String duplicateKeys ="";
                        for(int i = 0; i < colNameList.size() ; i++) {
                            colNames +=  " `" + colNameList.get(i) + "`,";
                            duplicateKeys += " `" + colNameList.get(i) + "`=VALUES(`" +  colNameList.get(i) +"`),";
                        }
                        colNames = colNames.substring(0, colNames.length()-1);
                        duplicateKeys = duplicateKeys.substring(0, duplicateKeys.length()-1);

                        int bulkCnt = 1;
                        String colVals = "";
                        for(String values: colValList){
                            colVals +="(" + values + "),";

                            // 배치 적용
                            if(bulkCnt++ % BATCH_SIZE == 0){
                                colVals = colVals.substring(0, colVals.length()-1);
                                String sql = "INSERT INTO " + tableName + "(" + colNames + ")" + " VALUES " + colVals
                                + " ON DUPLICATE KEY UPDATE " + duplicateKeys;
                                sqls.add(sql);
                                totBulkCnt++;
                                // 초기화
                                colVals = "";
                            }
                        }
                        // 나머지 요소 배치 적용
                        if(colVals.length() > 0){
                            colVals = colVals.substring(0, colVals.length()-1);
                            String sql = "INSERT INTO " + tableName + "(" + colNames + ")" + " VALUES " + colVals
                                    + " ON DUPLICATE KEY UPDATE " + duplicateKeys;
                            totBulkCnt++;
                            sqls.add(sql);
                        }
                    }
                }

                logger.info("(INSERT) 총 처리 예정 ROW : " + totRowCnt);
                logger.info("(INSERT) 총 bulk 구문 개수: " + totBulkCnt);

                // Insert 문 수행
                metricDao.executeQuery(sqls);

            } catch (Exception e){
                logger.info("INSERT 작업 실패", e);

                logger.info("INSERT 실패한 Metric 정보: \n" + getErrorMetricInfo(metrics));
            }
        }

    }


    /**
     * UPDATE 작업 수행 메소드
     *
     * @param metrics UPDATE 할 메트릭 정보들 ( Map<테이블명, Map<키, 벨류>> )
     */
    private void updateMetrics(Map<String, String[]> metrics) {
        int updateCnt = 0;
        int insertCnt = 0;
        int selectCnt = 0;
        int passCnt = 0;

        Map<String, List<String>> colsConfig = getColumnsByTable();

        try{
            // 가장 최신 timestamp를 가진 요소만 선택
            Map<String, String> resKey = new HashMap<>();
            for(String keys : metrics.keySet()) {
                String[] keyArr = keys.split(",");
                String anotherKey = keyArr[0]+","+keyArr[1]+","+keyArr[2];
                if(keyArr.length>4) anotherKey += ","+keyArr[4];
                String timestamp = keyArr[3];

                // 같은 키(tableName, system_seq, process_seq, dynamic_colName)가 이미 있으면
                String oldTime = resKey.get(anotherKey);
                if(oldTime!= null && oldTime.length() > 0) {
                    // 날짜 비교해서 최신것이 아니면 패스
                    LocalDateTime oldDt = LocalDateTime.parse(oldTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                    LocalDateTime newDt = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                    if(oldDt.isAfter(newDt)) {
                        passCnt++;
                        continue;
                    }
                }
                resKey.put(anotherKey, timestamp);
            }

            // 결정한 키에 해당하는 값들 UPSERT
            for(Map.Entry<String,String> keys : resKey.entrySet()) {
                String timestamp = keys.getValue();

                String[] keyArr = keys.getKey().split(",");
                String tableName = keyArr[0];
                String key = tableName+","+keyArr[1]+","+keyArr[2];
                key += ","+ timestamp;
                if(keyArr.length>3) key += "," + keyArr[3];

                // SELECT SQL 생성
                String selectSql = "";
                selectSql += "SELECT SUBSTRING(date_format(timestamp, '%Y-%m-%d %H:%i:%S.%f'),1,23) timestamp";
                selectSql += " FROM " + tableName + " WHERE system_seq = " + keyArr[1] + " AND process_seq = " + keyArr[2];
                if(keyArr.length>3)
                    selectSql += ", " + tableConfigs.get(tableName).getDynamic_classify_column_name() + "=" + keyArr[3];
                selectSql += " ORDER BY timestamp DESC LIMIT 1";
                // SELECT 수행
                String selectTime = metricDao.selectTimestamp(selectSql);
                selectCnt++;

                // UPSERT
                List<String> colNameList = colsConfig.get(tableName);
                String[] colValList = metrics.get(key);
                if(selectTime != null && selectTime.length() > 0) {
                    LocalDateTime oldDt = LocalDateTime.parse(selectTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                    LocalDateTime rowDt = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

                    // 기존 DB에 등록된 시간보다 크다면 Update 진행
                    if (!oldDt.isAfter(rowDt)) {
                        // 같은 시간이 있다면 null 이 아닌것만 update
                        // 이후 시간이라면 전체 value 모두 update
                        boolean allUpdate = true;
                        if(oldDt.isEqual(rowDt)) allUpdate = false;
                        String updateSql = "";
                        updateSql += "UPDATE " + tableName + " SET ";
                        for(int i = 0; i < colNameList.size() ; i++) {
                            if(allUpdate || colValList[i] != null) {
                                updateSql += " `" + colNameList.get(i) + "`= ";
                                updateSql += (colValList[i] == null )? colValList[i] : "'" + colValList[i] +"'";
                                updateSql += ",";
                            }
                        }
                        updateSql = updateSql.substring(0, updateSql.length() - 1);
                        updateSql += " WHERE system_seq = " + keyArr[1] + " AND process_seq = " + keyArr[2];

                        metricDao.executeQuery(updateSql);
                        updateCnt++;
                    }

                // 같은 key를 가진 로우가 없다면 새로 Insert 진행
                } else {
                    String colNames ="";
                    String colVals = "";
                    for(int i = 0; i < colNameList.size() ; i++) {
                        colNames +=  " `" + colNameList.get(i) + "`,";
                        colVals +=  (colValList[i] == null)? colValList[i] +"," : "'" + colValList[i] + "',";
                    }

                    colNames = colNames.substring(0, colNames.length() -1);
                    colVals = colVals.substring(0, colVals.length() -1);

                    String insertSql = "INSERT INTO " + tableName + "(" + colNames +")"
                            + "VALUES (" + colVals +")";

                    metricDao.executeQuery(insertSql);
                    insertCnt++;
                }

            }
            logger.info("(UPDATE) 최신 데이터 아니여서 넘어간 개수: " + passCnt);
            logger.info("(UPDATE) select 시도 개수: " + selectCnt);
            logger.info("(UPDATE) update 개수: " + updateCnt);
            logger.info("(UPDATE) insert 개수: " + insertCnt);

        }  catch ( Exception e){
            logger.error("UPDATE 처리 실패", e);

            // 실패한 메트릭 정보 로그
            logger.info("UPDATE 실패한 Metric 정보: \n" + getErrorMetricInfo(metrics));

        }
    }

    /**
     * 에러 로그 출력용
     * 메트릭 정보 출력 메소드
     *
     * @param metrics
     * @return
     */
    private String getErrorMetricInfo(Map<String, String[]> metrics){
        String info ="";
        Map<String, List<String>> colsConfig = getColumnsByTable();

        for(String keys : metrics.keySet()) {
            String[] keyArr = keys.split(",");
            List<String> colNameList = colsConfig.get(keyArr[0]);
            String[] colValList= metrics.get(keys);
            info += "테이블 명: " + keyArr[0] + "/  ";
            for(int i = 0; i < colNameList.size() ; i++) {
                info +=  colNameList.get(i) +":" + colValList[i] +',';
            }
            info = info.substring(0, info.length()-1);
            info += "\n";
        }
        return info;
    }

    /**
     * @param mills Milliseconds 형식의 문자열
     * @return yyyy-MM-dd HH:mm:ss.SSS 형식의 문자열
     */
    private String convertFormat(String mills){
        String res = "";
        try{
            LocalDateTime dt = new Timestamp(Long.parseLong(mills)).toLocalDateTime();
            res = dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
        } catch (Exception e){
            logger.error("날짜 변환 실패", e);
        }
        return res;
    }

    /**
     * DB 에서 불러온 config 가공
     * 각 테이블에 속한 필드 명 리스트 얻기
     *
     * @return Map<DB 테이블명 , List<속한 필드 명>>
     */
    private Map<String, List<String>> getColumnsByTable(){
        Map<String, List<String>> columns = new HashMap<>();

        try{
            for(MetricConfig config: metricConfigs){
                String tableName = config.getTable_name();
                String colName = config.getColumn_name();

                List<String> colsByTable = columns.get(tableName);
                if( colsByTable == null || colsByTable.size() == 0){
                    colsByTable = new ArrayList<>();
                }

                colsByTable.add(colName);
                columns.put(tableName, colsByTable);
            }

        } catch (Exception e){
            logger.error("DB Config 가공 실패", e);
        }

        return columns;
    }


}
