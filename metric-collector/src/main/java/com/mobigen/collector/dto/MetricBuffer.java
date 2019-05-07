package com.mobigen.collector.dto;

import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

@Repository
public class MetricBuffer {
    private Map<LocalDateTime, Set<MetricInfo>> metricMap;
    private ReentrantLock accessLock = new ReentrantLock();

    public MetricBuffer() {
        this.metricMap = new HashMap<>();
    }

    @Override
    public String toString() {
        return "MetricBuffer{" +
                "metricMap=" + metricMap +
                '}';
    }

    public Map<LocalDateTime, Set<MetricInfo>> popMetricMap(){
        Map<LocalDateTime, Set<MetricInfo>> copy = new HashMap<>(this.metricMap);
        this.metricMap = new HashMap<>();

        return copy;
    }

    public Set getKeySet(){
        return this.metricMap.keySet();
    }

    public Set<MetricInfo> getMetric(LocalDateTime key){
        return this.metricMap.get(key);
    }

    public void addMetric(LocalDateTime dateTime, MetricInfo obj){
        Set<MetricInfo> metricInfoSet = this.metricMap.get(dateTime);
//        System.out.println("같은 dateTime을 가진 객체: " + metricList);

        if(metricInfoSet != null && metricInfoSet.size() > 0){
//            System.out.println("현재 같은 dt 가진 객체 개수: " + metricList.size());
//            System.out.println("추가하려는 객체: " + obj);
            metricInfoSet.add(obj);
        } else {
            metricInfoSet = new HashSet<>();
            metricInfoSet.add(obj);
        }
        metricMap.put(dateTime, metricInfoSet);

    }

    public void removeMetric(LocalDateTime key){
        this.metricMap.remove(key);
    }

    public void lock(){
        this.accessLock.lock();
    }

    public void unlock(){
        this.accessLock.unlock();
    }

    public boolean isHeldByCurrentThread(){
        return this.accessLock.isHeldByCurrentThread();
    }

}
