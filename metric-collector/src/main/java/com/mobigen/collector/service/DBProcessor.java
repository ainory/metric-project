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

                Long getMetricStart = System.currentTimeMillis();
                Map<String, Map<Map<String,String>,Map<String,String>>> parsedMetric = getMetricMapForSql(metrics);
                Long getMetricEnd = System.currentTimeMillis();
                logger.info("쿼리용 Map 변환 시간: " + (getMetricEnd - getMetricStart) + "ms");

                // INSERT, UPDATE 용 데이터 분류
                if(parsedMetric.size() > 0){
                    Map<String, Map<Map<String,String>,Map<String,String>>> insertMap = new HashMap<>();
                    Map<String, Map<Map<String,String>,Map<String,String>>> updateMap = new HashMap<>();

                    int rowCnt = 0;
                    // Table Config 정보와 비교하면서 분류
                    for(Map.Entry<String, Map<Map<String,String>,Map<String,String>>> parsedMetricEntry : parsedMetric.entrySet()){
                        String tableName = parsedMetricEntry.getKey();
                        if("Y".equals(tableConfigs.get(tableName).getInsert_flag())){
                            insertMap.put(parsedMetricEntry.getKey(), parsedMetricEntry.getValue());
                        } else {
                            updateMap.put(parsedMetricEntry.getKey(), parsedMetricEntry.getValue());
                        }
                        rowCnt += parsedMetricEntry.getValue().keySet().size();
                    }

                    logger.info("DB처리 예정 개수: " + rowCnt);

                    // DB 처리
                    Long insertStart = System.currentTimeMillis();
                    insertMetrics(insertMap);
                    Long insertEnd = System.currentTimeMillis();
                    updateMetrics(updateMap);
                    Long updateEnd = System.currentTimeMillis();
                    logger.info("insert 시간: " + (insertEnd - insertStart) + "ms");
                    logger.info("update 시간: " + (updateEnd - insertEnd) + "ms");

                    Long endTime = System.currentTimeMillis();
                    logger.info("DB처리 시간: " + (endTime - startTime) + "ms");
                }

            } catch ( Exception e) {
                logger.error("DB 작업 실패", e);
            } finally {
                logger.info("DB 작업 완료\n");
            }
        }

    }

    /**
     * INSERT 작업 수행 메소드
     *
     * @param metrics INSERT 할 메트릭 정보들 ( Map<테이블명, Map<키, 벨류>> )
     */
    private void insertMetrics(Map<String, Map<Map<String, String>, Map<String, String>>> metrics) {
        if(metrics != null && metrics.size() > 0){

            List<String> sqls = new ArrayList<>();
            int totBulkCnt = 0;
            int totRowCnt = 0;

            Map<String, List<String>> colsConfig = getColumnsByTable();

            try{
                // 테이블 단위로 쿼리문 생성( ∵ 배치)
                for(String tableName : metrics.keySet()){
                    List<List< Map<String, String> >> bulkInsertList = new ArrayList<>();
                    int bulkCnt = 0;

                    // Metric Config에서 테이블명으로 필드 리스트 가져오기
                    List<String> colNameList = colsConfig.get(tableName);

                    // 한 로우에 해당하는 값 생성 - List<Map<String, String>> = 한 로우에 들어갈 필드(이름, 값)들로 이뤄진 리스트
                    // ∵ duplicated key 관리
                    boolean isSingleSql = false;
                    List<Map<String, String>> oneRow;
                    for(Map.Entry<Map<String,String>,Map<String,String>> keyValCols : metrics.get(tableName).entrySet()){
                        oneRow = new ArrayList<>();
                        // 테이블에 들어가야하는 필드 모두 체크 (bulk insert - values 순서 보장)
                        for(String colName : colNameList){
                            String val;
                            // 키로 사용했던 Map 체크
                            if( keyValCols.getKey().get(colName) != null){
                                val = keyValCols.getKey().get(colName);
                                // 날짜 형식 변환
                                if("timestamp".equals(colName)){
                                    val = convertFormat(val);
                                }

                            // 값으로 사용했던 Map 체크
                            } else if(keyValCols.getValue().get(colName) != null ){
                                val = keyValCols.getValue().get(colName);

                            // 두 곳에서 모두 못찾았다면 null 값으로 채우기
                            } else {
                                // 이전에 모아뒀던 sql문 처리
                                if(bulkInsertList.size() > 0){
                                    sqls.add(createInsertDuplicateSQL(tableName, colNameList, bulkInsertList)); totBulkCnt++;
                                    bulkInsertList.clear();
                                }

                                // null 있는 로우는 bulk insert X
                                isSingleSql = true;
                                val = "null";
                            }

                            Map<String, String> colNameVal = new HashMap<>();
                            colNameVal.put(colName, val);

                            oneRow.add(colNameVal);
                        }

                        bulkInsertList.add(oneRow);
                        bulkCnt ++;
                        totRowCnt++;

                        // 배치 설정
                        if(isSingleSql || (BATCH_SIZE > 0 && bulkCnt % BATCH_SIZE == 0)){
                            sqls.add(createInsertDuplicateSQL(tableName, colNameList, bulkInsertList)); totBulkCnt++;
                            bulkInsertList.clear();
                        }

                    }

                    // 배치 나머지
                    if (bulkInsertList.size() > 0) {
                        sqls.add(createInsertDuplicateSQL(tableName, colNameList, bulkInsertList)); totBulkCnt++;
                    }

                }

                logger.info("(INSERT) 총 처리 예정 ROW : " + totRowCnt);
                logger.info("(INSERT) 총 bulk 구문 개수: " + totBulkCnt);

                // Insert 문 수행
                metricDao.executeQuery(sqls);

            } catch (Exception e){
                logger.info("INSERT 작업 실패", e);

                logger.info("INSERT 실패한 Metric 정보: \n" + metrics);
            }
        }

    }

    /**
     * UPDATE 작업 수행 메소드
     *
     * @param metrics UPDATE 할 메트릭 정보들 ( Map<테이블명, Map<키, 벨류>> )
     */
    private void updateMetrics(Map<String, Map<Map<String, String>, Map<String, String>>> metrics) {
        int updateCnt = 0;
        int insertCnt = 0;
        int selectCnt = 0;

        try{
            for(String tableName : metrics.keySet()){
                // 한 로우 단위로 DB작업 수행
                for( Map.Entry<Map<String,String>,Map<String,String>> oneRow : metrics.get(tableName).entrySet()){

                    // 현재 판단중인 데이터의 timestamp값 (날짜 형식)
                    String rowTimestamp = convertFormat(oneRow.getKey().get("timestamp"));

                    // 키 값을 기준(timestamp 제외)으로 조회하여 시간 정보 가져옴
                    Map<String, String> wheres = new HashMap<>();
                    wheres.putAll(oneRow.getKey());
                    wheres.remove("timestamp");
                    String selectSql = createSelectTimestampSQL(tableName, wheres);

                    String selectTime = metricDao.selectTimestamp(selectSql);
                    selectCnt++;

                    if(selectTime != null && selectTime.length() > 0){
                        LocalDateTime oldDt = LocalDateTime.parse(selectTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                        LocalDateTime rowDt = LocalDateTime.parse(rowTimestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

                        // 기존 DB에 등록된 시간보다 크다면 Update 진행
                        if(oldDt.isBefore(rowDt)) {

                            // UPDATE 할 값에 현재 데이터의 timestamp 값도 추가
                            Map<String, String> oneRowVals = new HashMap<>();
                            oneRowVals.putAll(oneRow.getValue());
                            oneRowVals.put("timestamp", rowTimestamp);
                            wheres.put("timestamp", selectTime);
                            String sql = createUpdateSQL(tableName, oneRowVals, wheres);

                            metricDao.executeQuery(sql);
                            updateCnt++;
//                            logger.info("update 쿼리 : " + sql);
                        }

                    // 같은 key를 가진 로우가 없다면 새로 Insert 진행
                    } else {

                        Map<String,String> oneRowVals = new HashMap<>();
                        oneRowVals.putAll(oneRow.getKey());
                        oneRowVals.put("timestamp", rowTimestamp); // 날짜 형식 변환
                        oneRowVals.putAll(oneRow.getValue());
                        String sql = createInsertSQL(tableName, oneRowVals);

                        metricDao.executeQuery(sql);
                        insertCnt++;
//                        logger.info("insert 쿼리 : " + sql);
                    }

                }
            }

            logger.info("(UPDATE) select 시도 개수: " + selectCnt);
            logger.info("(UPDATE) update 개수: " + updateCnt);
            logger.info("(UPDATE) insert 개수: " + insertCnt);

        } catch ( Exception e){
            logger.error("UPDATE 처리 실패", e);

            logger.info("UPDATE 실패한 Metric 정보: \n" + metrics);
        }
    }

    /**
     * DB의 timestamp값 조회하는 쿼리문 생성
     * 가장 최근 시간으로 조회
     *
     * @param tableName
     * @param wheres 조회하려는 조건값
     * @return
     */
    private String createSelectTimestampSQL(String tableName, Map<String,String> wheres){
        String selectSql ="";
        try{
            String where ="";

            // WHERE절에 사용될 구문 생성
            for(Map.Entry<String,String> whereEntry: wheres.entrySet()){
                String colName = whereEntry.getKey();
                String colVal = whereEntry.getValue();
                where +=  " `" + colName + "`='" + colVal +"' AND";
            }

            where = where.substring(0, where.length() - 3);
            selectSql = "SELECT SUBSTRING(date_format(timestamp, '%Y-%m-%d %H:%i:%S.%f'),1,23) timestamp" +
                    " FROM " + tableName + " WHERE " + where + " ORDER BY timestamp DESC LIMIT 1";

        } catch (Exception e){
            logger.error("SELECT문 생성 실패", e);
        }

        return selectSql;
    }

    /**
     * UPDATE 쿼리문 생성 메소드
     *
     * @param tableName
     * @param oneRowVals UPDATE 할 값들: Map<필드명, 필드값>
     * @param wheres UPDATE WHERE 절에 들어갈 값들: Map<필드명, 필드값>
     * @return
     */
    private String createUpdateSQL(String tableName, Map<String, String> oneRowVals, Map<String, String> wheres){
        String sql = "";

        try {
            String values ="";
            for(Map.Entry<String,String> valEntry: oneRowVals.entrySet()){
                String colName = valEntry.getKey();
                String colVal = valEntry.getValue();
                values +=  " `" + colName + "`='" + colVal +"',";
            }

            String where ="";
            for(Map.Entry<String,String> whereEntry: wheres.entrySet()){
                String colName = whereEntry.getKey();
                String colVal = whereEntry.getValue();
                where +=  " `" + colName + "`='" + colVal +"' AND";
            }

            values = values.substring(0, values.length() - 1);
            where = where.substring(0, where.length() - 3);

            sql = "UPDATE " + tableName + " SET " + values + " WHERE " + where;

        } catch (Exception e){
            logger.error("UPDATE문 생성 실패", e);
        }

        return sql;
    }

    /**
     * INSERT 쿼리문 생성 메소드
     *
     * @param tableName
     * @param oneRowVals INSERT 할 값들 : Map<필드명, 필드값>
     * @return
     */
    private String createInsertSQL(String tableName, Map<String, String> oneRowVals){
        String sql = "";

        try{
            String colNames = "";
            String colVals = "";

            for(Map.Entry<String,String> keyEntry: oneRowVals.entrySet()){
                String colName = keyEntry.getKey();
                String colVal = keyEntry.getValue();
                colNames +=  " `" + colName + "`,";
                colVals +=  "'" + colVal +"',";
            }

            colNames = colNames.substring(0, colNames.length() -1);
            colVals = colVals.substring(0, colVals.length() -1);

            sql = "INSERT INTO " + tableName + "(" + colNames +")"
                    + "VALUES (" + colVals +")";

        } catch (Exception e){
            logger.error("INSERT문 생성 실패", e);
        }


        return sql;
    }

    /**
     * 완전한 SQL 문 생성 메소드
     *
     * @param tableName 테이블명
     * @param colNameList 컬럼명 리스트
     * @param colValList Bulk insert 구문 List< 한 로우에 들어가는 필드 List< Map<필드명, 필드값> >>
     * @return 완전한 INSERT 구문
     */
    private String createInsertDuplicateSQL(String tableName, List<String> colNameList, List<List<Map<String, String>>> colValList){
        String sql = "";
        String values ="";
        Set<String> duplicateKeySet = new HashSet();
        String duplicateKeys ="";
        try{
            // INSERT 구문 생성 , VALUES () 전까지.
            String columnNames = "";
            for(String colName : colNameList){
                columnNames +=  "`" + colName + "`,";
            }
            columnNames = columnNames.substring(0, columnNames.length()-1);

            for(List<Map<String, String>> oneRow : colValList){
                values += "(";

                for(Map<String, String> nameVals : oneRow){
                    for(Map.Entry<String, String> nameVal : nameVals.entrySet()){
                        // VALUES 설정  ->  "'a', 'b', 'c', null, 'd'..."
                        String value = nameVal.getValue();
                        if("null".equals(value)){
                            values += value + ",";
                        } else {
                            values += "'"+ value + "',";
                            // null 이 아닌경우만 duplicateKey 설정
                            duplicateKeySet.add("`" + nameVal.getKey() +"`");
                        }
                    }
                }
                values = values.substring(0, values.length()-1);
                values += "),";
            }

            for(String dup: duplicateKeySet) duplicateKeys += dup + "=VALUES(" + dup + "),";
            duplicateKeys = duplicateKeys.substring(0, duplicateKeys.length()-1);
            values = values.substring(0, values.length()-1);

            sql += "INSERT INTO " + tableName + "(" + columnNames + ")" + " VALUES " + values
                    + " ON DUPLICATE KEY UPDATE " + duplicateKeys;

        } catch (Exception e){
            logger.error("Insert-Duplicate문 생성 실패", e);
        }

        return sql;
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


    /**
     * 테이블, key를 기준으로 SET 데이터들 집합시켜 Map형식으로 재배치
     *
     * @param metrics
     * @return Map<테이블명, Map< Map<필드명, 값>, Map<필드명, 값> >>
     *                                |                       |
     *          키(sys_seq, proc_seq, timestamp 등)    값(일반 메트릭 값)
     */
    private Map<String, Map<Map<String,String>,Map<String,String>>> getMetricMapForSql(Set<MetricInfo> metrics) {
        Map<String, Map<Map<String,String>,Map<String,String>>> resMetricMap = new HashMap<>();

        for (MetricInfo metric : metrics) {
            for (MetricConfig config : metricConfigs) {
                try {
                    boolean isMatch;
                    Pattern metircNamePattern = null;

                    // metric_name 패턴 일치 && table_name 일치
                    if("Y".equals(config.getDynamic_flag())){
                        metircNamePattern = Pattern.compile(config.getMetric_name());
                        isMatch = metircNamePattern.matcher(metric.getMetric_name()).matches()
                            && ("RRD".equals(metric.getTable_name()) || config.getTable_name().equals(metric.getTable_name()));
                    } else {
                        isMatch = config.getMetric_name().equals(metric.getMetric_name())
                                && ("RRD".equals(metric.getTable_name()) || config.getTable_name().equals(metric.getTable_name()));
                    }
//
                    if (isMatch) {
                        // 수집 여부 = 'Y'
                        if ("Y".equals(config.getCollect_flag())) {
                            // ----------- 1. 키 설정 (system_seq, process_seq, timestamp + dynamic_classify_column_name)
                            Map<String, String> keys = new HashMap<>();
                            keys.put("system_seq", metric.getSystem_seq());
                            keys.put("process_seq", metric.getProcess_seq());
                            keys.put("timestamp", metric.getTimestamp());
                            // 동적 필드인 경우 구분명 추출
                            if ("Y".equals(config.getDynamic_flag())) {
                                String dynamicColName = tableConfigs.get(config.getTable_name()).getDynamic_classify_column_name();
                                String dynamicColVal = "";
                                Matcher matcher = metircNamePattern.matcher(metric.getMetric_name());
                                while (matcher.find()) {
                                    dynamicColVal = matcher.group(1);
                                }
                                keys.put(dynamicColName, dynamicColVal);
                            }

                            // ----------- 2. value 설정
                            Map<String, String> values = new HashMap<>();
                            String colName = config.getColumn_name();
                            String metricValue = metric.getMetric_value();
                            values.put(colName, metricValue);


                            // ----------- 3. 결과로 반환할 map에 추가
                            String tableName = config.getTable_name();

                            // 같은 Key에 속한 값이 있는지 확인
                            Map<Map<String, String>, Map<String, String>> keyVals = resMetricMap.get(tableName);
                            // 해당 table name에 속한 데이터가 없다면 초기화
                            if (keyVals == null || keyVals.size() == 0) {
                                keyVals = new HashMap<>();

                            } else {
                                Map<String, String> oldValues = keyVals.get(keys);
                                // 해당 key 값들에 속한 데이터가 이미 존재한다면
                                if (oldValues != null && oldValues.size() > 0) {
                                    // 합치기
                                    values.putAll(oldValues);
                                }
                            }

                            // 리턴할 값에 추가
                            keyVals.put(keys, values);
                            resMetricMap.put(tableName, keyVals);

                        }
                        break;
                    }


                } catch (Exception e){
                    logger.error("Metrics 정보 -> 쿼리생성용 Map으로 변환 실패", e);

                    // 실패한 메트릭 정보
                    logger.error("가공 실패한 Metric 정보: \n" + metric);
                }
            }

            /*logger.info("파싱 결과 >>>" );
            for(String tableName : resMetricMap.keySet()){
                logger.info("------------ " + tableName + "-------------");
                for(Map.Entry<Map<String, String>, Map<String, String>> mm: resMetricMap.get(tableName).entrySet()){
                    logger.info(String.valueOf(mm.getKey()));
                    logger.info(String.valueOf(mm.getValue()));
                }
            }*/
        }

        return resMetricMap;
    }

}
