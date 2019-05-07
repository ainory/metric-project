package com.mobigen.collector.service;

import com.mobigen.collector.dto.MetricBuffer;
import com.mobigen.collector.dto.MetricInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;

@Service
@EnableScheduling
public class MetricProcessor {
    Logger logger = LoggerFactory.getLogger(MetricProcessor.class);

    @Value("${log.exp.max}")
    int EXP_MAX;

    @Autowired
    MetricBuffer metricBuffer;

    @Autowired
    DBProcessor dbProcessor;

    public void run() {
        logger.info("데이터 처리 시작");

        try{
            Long startTime = System.currentTimeMillis();

            // 처리하려는 map 가져오기
            Set<MetricInfo> metrics =  getFilteredMetricSet();

            // 데이터 DB 처리
            if(metrics.size() > 0)
                dbProcessor.processMetrics(metrics);
            else
                logger.info("DB > 처리할 데이터가 존재하지 않습니다.");

            Long endTime = System.currentTimeMillis();
            logger.info("메트릭 처리 시간: " + (endTime - startTime) + "ms");
        } catch (Exception e){
            logger.error("데이터 처리 실패", e);
        }

    }

    /**
     * Metric 버퍼에서 기간 지난 데이터 제외한 정보들 가져오는 메소드
     *
     * @return
     */
    private Set<MetricInfo> getFilteredMetricSet(){
        Set<MetricInfo> metricSet = new HashSet<>();
        try{
            metricBuffer.lock();

//            showAll_Test();

            // 최대 유지 기간 지난 데이터 삭제
            removeOldMetricInfo();

            // 메트릭 데이터 가져오기
            Map<LocalDateTime, Set<MetricInfo>> popMetricMap = metricBuffer.popMetricMap();

            if(popMetricMap.size() > 0){
                for( Set<MetricInfo> val :  popMetricMap.values()){
                    metricSet.addAll(val);
                }
            }

        } catch (Exception e) {
            logger.error("MetricMap 가져오기 실패", e);
        } finally {
            try{
                if(metricBuffer.isHeldByCurrentThread()){
                    metricBuffer.unlock();
                }
            } catch (Exception e){
                logger.error("Lock 해제 실패", e);
            }
        }

        return metricSet;
    }

    /**
     * 일정 기한 지난 메트릭 데이터 삭제하는 메소드
     * (Milliseconds 는 고려하지 않는다)
     */
    private void removeOldMetricInfo(){
        int removeCnt = 0;
        try{
            Set<LocalDateTime> keySet = metricBuffer.getKeySet();

            if( keySet.size() > 0) {
                LocalDateTime maxLimitTime = LocalDateTime.now().minusMinutes(EXP_MAX).withNano(0);

                Iterator iter = keySet.iterator();
                while (iter.hasNext()) {
                    LocalDateTime targetTime = ((LocalDateTime)iter.next()).withNano(0);
                    // 기한 지난 데이터면 삭제
                    if (targetTime.isBefore(maxLimitTime)) {
                        iter.remove(); removeCnt++;
                    }
                }

                /*if(removeCnt > 0) {
                    logger.info("******************** remove : "+ removeCnt +"key 삭제 ***************");
                    getSortedKeys_Test();
                    showAll_Test();
                }*/

            }

        } catch (Exception e){
            logger.error("기한 지난 데이터 삭제 실패", e);
        }

    }


    // 현재 keyset 정렬해서 보여줌
    public String[] getSortedKeys_Test() {
        Set<LocalDateTime> s = metricBuffer.getKeySet();

        String[] arr = new String[0];
        if (s.size() > 0) {
            int n = s.size();
            arr = new String[n];

            for (LocalDateTime t : s) {
                arr[--n] = t.toString();
            }

            Arrays.sort(arr);
        }

        return arr;
    }

    // 시간별 데이터 모두 보여줌
    public void showAll_Test(){

        String[] keys = getSortedKeys_Test();
        logger.info("keySet: " + Arrays.toString(keys));

        if (keys.length > 0) {
            for(String str : keys){
                LocalDateTime key = LocalDateTime.parse(str);
                Set<MetricInfo> metrics = metricBuffer.getMetric(key);
//            for(LocalDateTime dt : map.keySet()){
                System.out.println("------- " + key.toString() +" -> 현재개수: "+ metrics.size() +"개 ----------");

    //            for(MetricInfo parsedMetric : map.get(dt)){
                int sample = metrics.size();
                if( sample > 3){
                    sample = 3;
                }
                Iterator iter = metrics.iterator();
                for(int i = 0; i< sample ; i++){
    //                logger.info(String.valueOf(parsedMetric));
                    logger.info(String.valueOf(iter.next()));
                }
            }

        }
        System.out.println("********************************* \n");
    }
}
