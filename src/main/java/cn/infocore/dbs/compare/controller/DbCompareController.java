package cn.infocore.dbs.compare.controller;

import cn.infocore.dbs.compare.model.DbCompare;
import cn.infocore.dbs.compare.model.dto.DbCompareDto;
import cn.infocore.dbs.compare.service.impl.DbCompareServiceImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/DbCompare")
@Tag(name = "数据校验管理", description = "数据校验管理")
public class DbCompareController {

    @Autowired
    private DbCompareServiceImpl service;

    @PostMapping
    @Operation(summary = "新增数据校验", description = "新增数据校验")
    public void create(DbCompareDto dbCompareDto){
        service.create(dbCompareDto);
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "删除数据校验")
    public void delete(@PathVariable("id") @Parameter(name = "id", example = "1") Long id){
        service.delete(id);
    }

    @PutMapping
    @Operation(summary = "更新数据校验")
    public void update(DbCompareDto dbCompareDto){
        service.update(dbCompareDto);
    }

    @GetMapping
    @Operation(summary = "查询数据校验")
    public List<DbCompare> list(){
        return service.list();
    }

}
