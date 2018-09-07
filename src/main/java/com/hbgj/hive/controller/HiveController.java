package com.hbgj.hive.controller;

import com.hbgj.hive.entity.ResponseObject;
import com.hbgj.hive.service.HiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.List;

@RestController
@EnableAutoConfiguration
@RequestMapping("/")
public class HiveController {
    private static final SimpleDateFormat DATEFORMATE = new SimpleDateFormat("yyyy-MM-dd");
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveController.class);
    @Autowired
    private HiveService hiveService;

    @RequestMapping("showDatabases")
    public ResponseObject showDataBases() {
        ResponseObject resp=new ResponseObject();
        hiveService.showDataBases(resp);
        return resp;
    }

    @RequestMapping("showTables")
    public ResponseObject showTables(String dbName) {
        ResponseObject resp=new ResponseObject();
        hiveService.showTables(dbName,resp);
        return resp;
    }

    @RequestMapping("showPartions")
    public ResponseObject showPartions(String dbName,String tableName) {
        ResponseObject resp=new ResponseObject();
        hiveService.showPartions(dbName,tableName,resp);
        return resp;
    }

    @RequestMapping("getFields")
    public ResponseObject getFields(String dbName,String tableName) {
        ResponseObject resp=new ResponseObject();
        hiveService.getFields(dbName,tableName,resp);
        return resp;
    }


    @RequestMapping("exeQuery")
    public ResponseObject execSql(String dbName,String sql) {
        ResponseObject resp=new ResponseObject();
        hiveService.execSql(dbName,sql,resp);
        return resp;
    }

    @RequestMapping("exec")
    public ResponseObject exec(String dbName,String sql) {
        ResponseObject resp=new ResponseObject();
        hiveService.exec(dbName,sql,resp);
        return resp;
    }



}