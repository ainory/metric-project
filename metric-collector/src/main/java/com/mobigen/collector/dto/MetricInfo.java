package com.mobigen.collector.dto;

import java.util.Objects;

public class MetricInfo {
    private String system_seq;
    private String process_seq;
    private String metric_name;
    private String table_name;
    private String metric_value;
    private String timestamp;

    @Override
    public String toString() {
        return "MetricInfo{" +
                "system_seq=" + system_seq +
                ", process_seq=" + process_seq +
                ", metric_name='" + metric_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", metric_value='" + metric_value + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricInfo that = (MetricInfo) o;
        return Objects.equals(system_seq, that.system_seq) &&
                Objects.equals(process_seq, that.process_seq) &&
                Objects.equals(metric_name, that.metric_name) &&
                Objects.equals(table_name, that.table_name) &&
                Objects.equals(metric_value, that.metric_value) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(system_seq, process_seq, metric_name, table_name, metric_value, timestamp);
    }


    public String getSystem_seq() {
        return system_seq;
    }

    public void setSystem_seq(String system_seq) {
        this.system_seq = system_seq;
    }

    public String getProcess_seq() {
        return process_seq;
    }

    public void setProcess_seq(String process_seq) {
        this.process_seq = process_seq;
    }

    public String getMetric_name() {
        return metric_name;
    }

    public void setMetric_name(String metric_name) {
        this.metric_name = metric_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getMetric_value() {
        return metric_value;
    }

    public void setMetric_value(String metric_value) {
        this.metric_value = metric_value;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        if("RRD".equals(this.table_name)){
            this.timestamp = timestamp + "000";
        }else {
            this.timestamp = timestamp;
        }
    }


}