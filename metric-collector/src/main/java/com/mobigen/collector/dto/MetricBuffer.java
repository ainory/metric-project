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

        if(metricInfoSet != null && metricInfoSet.size() > 0){
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
