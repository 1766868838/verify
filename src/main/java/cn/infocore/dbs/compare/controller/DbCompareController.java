package cn.infocore.dbs.compare.controller;

import cn.infocore.dbs.compare.model.DbCompare;
import cn.infocore.dbs.compare.model.dto.DbCompareDto;
import cn.infocore.dbs.compare.model.vo.DbCompareVo;
import cn.infocore.dbs.compare.quartz.QuartzService;
import cn.infocore.dbs.compare.service.impl.DbCompareServiceImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/DbCompare")
@Tag(name = "数据校验管理", description = "数据校验管理")
public class DbCompareController {

    @Autowired
    private DbCompareServiceImpl service;

    @Autowired
    private Scheduler scheduler;

    @Autowired
    private QuartzService quartzService;
    @PostMapping
    @Operation(summary = "新增数据校验", description = "新增数据校验")
    public void create(@RequestBody DbCompareDto dbCompareDto){
        service.create(dbCompareDto);
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "删除数据校验")
    public void delete(@PathVariable("id") @Parameter(name = "id", example = "1") Long id){
        service.delete(id);
    }

    @PutMapping
    @Operation(summary = "更新数据校验")
    public void update(@RequestBody DbCompare dbCompare){
        service.update(dbCompare);
    }

    @GetMapping
    @Operation(summary = "查询数据校验")
    public List<DbCompare> list(){
        return service.list();
    }

    @GetMapping("/{id}")
    @Operation(summary = "查看数据校验详情")
    public DbCompareVo get(@PathVariable("id") Long id){
        return service.get(id);
    }

    @PostMapping("/start")
    @Operation(summary = "启动数据校验任务")
    public void start(@RequestBody DbCompare dbCompare) throws SchedulerException {
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put("DbCompare", dbCompare);



        quartzService.addJob(dbCompare.getName(),"group1",
                DbCompareServiceImpl.class, jobDataMap);
    }
    @PostMapping("/pause")
    @Operation(summary = "暂停数据校验任务")
    public void pause(@RequestBody DbCompare dbCompare) throws SchedulerException {
        quartzService.pauseJob(dbCompare.getName(),"group1");
    }

    @PostMapping("/resume")
    @Operation(summary = "恢复数据校验任务")
    public void resume(@RequestBody DbCompare dbCompare) throws SchedulerException {
        quartzService.resumeJob(dbCompare.getName(),"group1");
        //quartzService.addJob("test","gourp2", TestJob.class, "* * * * * ? *");
    }
}
