package cn.infocore.dbs.compare.quartz;

import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
public class QuartzService {

    @Autowired
    private Scheduler scheduler;

    /**
     * 添加任务
     * @param jobName 任务名称
     * @param jobGroup 任务组
     * @param jobClass 任务类
     * @param cron 表达式
     * @throws SchedulerException
     */
    public void addJob(String jobName, String jobGroup, Class<? extends Job> jobClass, String cron) throws SchedulerException {
        JobDetail jobDetail = JobBuilder.newJob(jobClass)
                .withIdentity(jobName, jobGroup)
                .build();

        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(jobName, jobGroup)
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .build();

        scheduler.scheduleJob(jobDetail, trigger);
    }

    /**
     * 添加任务
     * @param jobName 任务名称
     * @param jobGroup 任务组
     * @param jobClass 任务类
     * @throws SchedulerException
     */
    public void addJob(String jobName, String jobGroup, Class<? extends Job> jobClass, JobDataMap jobDataMap) throws SchedulerException {

        jobDataMap.computeIfAbsent("startTime", k -> new Date());

        JobDetail jobDetail = JobBuilder.newJob(jobClass)
                .withIdentity(jobName, jobGroup)
                .usingJobData(jobDataMap)
                .build();

        SimpleTrigger trigger = (SimpleTrigger)TriggerBuilder.newTrigger()
                .withIdentity(jobName, jobGroup)
                .startAt((Date) jobDataMap.get("startTime"))
                .build();


//        Trigger trigger = TriggerBuilder.newTrigger()
//                .withIdentity(jobName, jobGroup)
//                .build();
        scheduler.scheduleJob(jobDetail, trigger);
    }


    /**     * 删除任务     *     * @param jobName 任务名称     * @param jobGroup 任务组     * @throws SchedulerException
     */
    public void deleteJob(String jobName, String jobGroup) throws SchedulerException {
        scheduler.deleteJob(JobKey.jobKey(jobName, jobGroup));
    }

    /**     * 暂停任务     *     * @param jobName 任务名称     * @param jobGroup 任务组     * @throws SchedulerException
     */
    public void pauseJob(String jobName, String jobGroup) throws SchedulerException {
        scheduler.pauseJob(JobKey.jobKey(jobName, jobGroup));
        System.out.println(jobName+"暂停任务成功");
    }

    /**     * 恢复任务     *     * @param jobName 任务名称     * @param jobGroup 任务组     * @throws SchedulerException
     */
    public void resumeJob(String jobName, String jobGroup) throws SchedulerException {
        scheduler.resumeJob(JobKey.jobKey(jobName, jobGroup));
        System.out.println(jobName+"任务恢复成功");
    }
}
