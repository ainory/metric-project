package com.mobigen.collector.dto;

import org.springframework.stereotype.Repository;

@Repository
public class TableConfig {
    private String table_name;
    private String insert_flag;
    private String dynamic_classify_column_name;

    @Override
    public String toString() {
        return "TableConfig{" +
                "table_name='" + table_name + '\'' +
                ", insert_flag='" + insert_flag + '\'' +
                ", dynamic_classify_column_name='" + dynamic_classify_column_name + '\'' +
                '}';
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getInsert_flag() {
        return insert_flag;
    }

    public void setInsert_flag(String insert_flag) {
        this.insert_flag = insert_flag;
    }

    public String getDynamic_classify_column_name() {
        return dynamic_classify_column_name;
    }

    public void setDynamic_classify_column_name(String dynamic_classify_column_name) {
        this.dynamic_classify_column_name = dynamic_classify_column_name;
    }
}
