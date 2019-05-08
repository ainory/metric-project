package com.mobigen.collector.dto;

public class MetricConfig {
    private String metric_name;
    private String table_name;
    private String column_name;
    private String collect_flag;
    private String dynamic_flag;

    @Override
    public String toString() {
        return "MetricConfig{" +
                "metric_name='" + metric_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", column_name='" + column_name + '\'' +
                ", collect_flag='" + collect_flag + '\'' +
                ", dynamic_flag='" + dynamic_flag + '\'' +
                '}';
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

    public String getColumn_name() {
        return column_name;
    }

    public void setColumn_name(String column_name) {
        this.column_name = column_name;
    }

    public String getCollect_flag() {
        return collect_flag;
    }

    public void setCollect_flag(String collect_flag) {
        this.collect_flag = collect_flag;
    }

    public String getDynamic_flag() {
        return dynamic_flag;
    }

    public void setDynamic_flag(String dynamic_flag) {
        this.dynamic_flag = dynamic_flag;
    }
}
