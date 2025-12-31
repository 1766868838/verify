package cn.infocore.dbs.compare.quartz;

import cn.infocore.dbs.compare.service.impl.DbCompareServiceImpl;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

public class QuartzConfig {
    @Bean
    public JobDetail myJobDetail() {
        // 定义任务
        return JobBuilder.newJob(DbCompareServiceImpl.class)
                .withIdentity("DbCompareServiceImpl", "group1")
                .storeDurably()
                .build();
    }

    @Bean
    public Trigger myJobTrigger(JobDetail myJobDetail) {
        // 定义触发器
        return TriggerBuilder.newTrigger()
                .forJob(myJobDetail)
                .withIdentity("myJobTrigger", "group1")
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInSeconds(5) // 每5秒执行一次
                        .repeatForever())
                .build();
    }
}