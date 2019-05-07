package com.mobigen.collector.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobigen.collector.dto.MetricBuffer;
import com.mobigen.collector.dto.MetricInfo;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.*;
import java.util.*;


@Service
public class MetricCollector {

    Logger logger = LoggerFactory.getLogger(MetricCollector.class);

    @Value("${kafka.bootstrap.servers}")
    String BOOTSTRAP_SERVER;
    @Value("${kafka.topic}")
    String TOPIC;
    @Value("${kafka.poll.duration.seconds}")
    long POLL_DURATION;
    @Value("${kafka.group.id}")
    String GROUP_ID;
    @Value("${kafka.max.poll.records}")
    String MAX_POLL_RECORDS;
    @Value("${kafka.auto.offset.reset}")
    String AUTO_OFFSET_RESET;

    @Value("${log.exp.max}")
    int EXP_MAX;

    private Consumer<String, String> consumer;

    @Autowired
    ObjectMapper mapper;

    @Autowired
    MetricBuffer metricBuffer;

    @Async("collect-executor")
    public void run() {
        logger.info("수집기 시작");

        ConsumerRecords<String, String> consumerRecords;

        while(true){
            try{
                // Consumer 객체 얻기
                if(consumer == null){
                    consumer = createConsumer();
                }

                consumerRecords = consumer.poll(Duration.ofSeconds(POLL_DURATION));

                // 데이터 수집
                if(consumerRecords.count() > 0){
                    for(ConsumerRecord<String,String> record: consumerRecords){
                        // 버퍼에 저장
                        insertBuffer(record.value());
                    }
                }

            } catch (Exception e){
                logger.error("데이터 수집 실패", e);
            }
        }

    }

    /**
     * Kafka Consumer 객체 생성 메소드
     *
     * @return
     */
    private Consumer<String,String> createConsumer() {
        Consumer<String, String> consumer = null;
        Properties prop;

        while (consumer == null) {
            try {
                prop = new Properties();
                prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
                prop.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
                prop.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
                prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
                prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

                consumer = new KafkaConsumer<>(prop);
                consumer.subscribe(Collections.singletonList(TOPIC));

                logger.info("Kafka Consumer 생성:" + consumer);
            } catch (Exception e) {
                logger.error("Kafka Consumer 객체 생성 실패, 3초 후 재 시도", e);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e1) {
                    logger.error("재시도를 위한 대기 실패", e);
                }
            }
        }

        return consumer;
    }

    /**
     * 로그 버퍼에 저장하는 메소드
     *
     * @param jsonLog 저장하려는 로그 문자열
     */
    public void insertBuffer(String jsonLog){
        // Json Array -> Object Array 로 변환
        MetricInfo[] objs = parsingJsonArray(jsonLog); //mapper.readValue(jsonLog, MetricInfo[].class);

        // 버퍼(공유객체)에 저장
        if(objs != null){
            for (MetricInfo metricInfo : objs) {
                try {
                    LocalDateTime dateTime = new Timestamp(Long.parseLong(metricInfo.getTimestamp())).toLocalDateTime();

//                    if("HM_NODE_USAGE".equals(metricInfo.getTable_name())){
                    // 시간 필터
                    if (dateTime.isAfter(LocalDateTime.now().minusMinutes(EXP_MAX))) {

                        metricBuffer.lock();
                        // 시간 별로 저장
                        metricBuffer.addMetric(dateTime, metricInfo);
                    }
                } catch (Exception e){
                    logger.error("데이터 공유객체에 저장 실패", e);

                    // 저장하려고 했던 객체 정보 에러로그로 남김
                    logger.error("처리 중 에러 발생한 데이터 정보: \n" + String.valueOf(metricInfo));
                } finally {
                    try{
                        if(metricBuffer.isHeldByCurrentThread()){
                            metricBuffer.unlock();
//                            logger.info("수집기 측 : Lock 잠금 해제");
                        }
                    } catch (Exception e){
                        logger.error("Lock 해제 실패", e);
                    }
                }
            }
        }

    }

    /**
     * Json 파싱하는 메소드
     *
     * @param json 파싱하려는 json 문자열
     * @return
     */
    public MetricInfo[] parsingJsonArray(String json){
        try {
            return mapper.readValue(json, MetricInfo[].class);

        } catch (Exception e) {
            logger.error("Josn 파싱 실패", e);

            // 해당 json 문자열 에러 로그로 남김
            logger.error("처리 중 에러 발생한 데이터 정보: \n" + json);
            return null;
        }
    }

}
