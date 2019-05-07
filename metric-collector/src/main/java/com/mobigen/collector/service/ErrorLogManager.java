package com.mobigen.collector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ErrorLogManager {
    Logger logger = LoggerFactory.getLogger(ErrorLogManager.class);

    /**
     * 에러발생 쿼리 파일로 저장
     *
     * @param e 에러 메세지
     * @param sqls 작업 실패한 쿼리문
     */
    public void writeErrorSql(Exception e, List<String> sqls){
        try{
            if(sqls != null && sqls.size() >0){
                logger.error("-----------------------------------");
                logger.error(e.getMessage(), e);
                for(String log: sqls){
                    logger.error(log);
                }
            }
        } catch (Exception ec){
            logger.error("에러 로그 기록 실패", ec);
        }
    }

    /**
     * 에러발생 쿼리 파일로 저장
     *
     * @param e 에러 메세지
     * @param sql 작업 실패한 쿼리문
     */
    public void writeErrorSql(Exception e, String sql){
        try{
            if(sql != null && sql.length() >0){
                logger.error("----------------------------");
                logger.error(e.getMessage(), e);
                logger.error(sql);
            }
        } catch (Exception ec){
            logger.error("에러 로그 기록 실패", ec);
        }
    }
}
