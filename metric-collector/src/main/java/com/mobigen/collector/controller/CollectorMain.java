package com.mobigen.collector.controller;

import com.mobigen.collector.service.MetricCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

@EnableAsync
@Component
public class CollectorMain {
    static Logger logger = LoggerFactory.getLogger(CollectorMain.class);

    public static void main(String[] args) {
        logger.info("Start Log Collector");
        ApplicationContext ctx = new ClassPathXmlApplicationContext("/spring-config.xml");
        MetricCollector metricCollector = ctx.getBean(MetricCollector.class);

        metricCollector.run();
    }
}
