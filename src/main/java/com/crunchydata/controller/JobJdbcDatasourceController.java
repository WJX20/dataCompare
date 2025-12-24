package com.crunchydata.controller;


import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.crunchydata.models.JobJdbcDatasource;
import com.crunchydata.result.ReturnT;
import com.crunchydata.services.JobJdbcDatasourceService;
import com.crunchydata.utils.LocalCacheUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

@RestController
@RequestMapping("/api/jobJdbcDatasource")
public class JobJdbcDatasourceController {
    /**
     * 服务对象
     */
    @Autowired
    private JobJdbcDatasourceService jobJdbcDatasourceService;

    /**
     * 分页查询所有数据
     *
     * @return 所有数据
     */
//    @GetMapping
//    public ReturnT<IPage<JobJdbcDatasource>> selectAll() {
//        BaseForm form = new BaseForm();
//        QueryWrapper<JobJdbcDatasource> query = (QueryWrapper<JobJdbcDatasource>) form.pageQueryWrapperCustom(form.getParameters(), new QueryWrapper<JobJdbcDatasource>());
//        return new ReturnT<>(jobJdbcDatasourceService.page(form.getPlusPagingQueryEntity(), query));
//    }

    @GetMapping("/pageList")
    public ReturnT<IPage<JobJdbcDatasource>> pageList() {
        BaseForm form = new BaseForm();
        QueryWrapper<JobJdbcDatasource> query = (QueryWrapper<JobJdbcDatasource>) form.pageQueryWrapperCustom(form.getParameters(), new QueryWrapper<JobJdbcDatasource>());
        return new ReturnT<>(jobJdbcDatasourceService.page(form.getPlusPagingQueryEntity(), query));
    }

    /**
     * 获取所有数据源
     * @return
     */
    //@ApiOperation("获取所有数据源")
    @GetMapping("/all")
    public ReturnT<List<JobJdbcDatasource>> selectAllDatasource() {
        return new ReturnT<>(this.jobJdbcDatasourceService.selectAllDatasource());
    }

    /**
     * 通过主键查询单条数据
     *
     * @param id 主键
     * @return 单条数据
     */
    ////@ApiOperation("通过主键查询单条数据")
    @GetMapping("{id}")
    public ReturnT<JobJdbcDatasource> selectOne(@PathVariable Serializable id) {
        return new ReturnT<>(this.jobJdbcDatasourceService.getById(id));
    }

    /**
     * 新增数据
     *
     * @param entity 实体对象
     * @return 新增结果
     */
    //@ApiOperation("新增数据")
    @PostMapping
    public ReturnT<Boolean> insert(@RequestBody JobJdbcDatasource entity) {
        return new ReturnT<>(this.jobJdbcDatasourceService.save(entity));
    }

    /**
     * 修改数据
     *
     * @param entity 实体对象
     * @return 修改结果
     */
    @PutMapping
    //@ApiOperation("修改数据")
    public ReturnT<Boolean> update(@RequestBody JobJdbcDatasource entity) {
        LocalCacheUtil.remove(entity.getDatasourceName());
        JobJdbcDatasource d = jobJdbcDatasourceService.getById(entity.getId());
        if (null != d.getJdbcUsername() && entity.getJdbcUsername().equals(d.getJdbcUsername())) {
            entity.setJdbcUsername(null);
        }
        if (null != entity.getJdbcPassword() && entity.getJdbcPassword().equals(d.getJdbcPassword())) {
            entity.setJdbcPassword(null);
        }
        return new ReturnT<>(this.jobJdbcDatasourceService.updateById(entity));
    }

    /**
     * 删除数据
     *
     * @param idList 主键结合
     * @return 删除结果
     */
    @DeleteMapping
    //@ApiOperation("删除数据")
    public ReturnT<Boolean> delete(@RequestParam("idList") List<Long> idList) {
        return new ReturnT<>(this.jobJdbcDatasourceService.removeByIds(idList));
    }

    /**
     * 单个删除数据
     *
     * @param id 主键
     * @return 删除结果
     */
    @PostMapping("/removeById")
//    @ApiOperation("单个删除数据")
    public ReturnT<Boolean> removeById(@RequestParam("id") int id) {
        return new ReturnT<>(this.jobJdbcDatasourceService.removeById(id));
    }

    /**
     * 测试数据源
     * @param jobJdbcDatasource
     * @return
     */
    @PostMapping("/test")
//    @ApiOperation("测试数据")
    public ReturnT<Boolean> dataSourceTest (@RequestBody JobJdbcDatasource jobJdbcDatasource) throws IOException {
        return jobJdbcDatasourceService.dataSourceTest(jobJdbcDatasource);
//        return new ReturnT<>(ReturnT.FAIL_CODE,"暂时未实现该功能");
    }
}
