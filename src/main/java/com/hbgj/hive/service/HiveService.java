package com.hbgj.hive.service;

import com.hbgj.hive.entity.ResponseObject;

import java.util.List;

public interface HiveService {
    void showDataBases(ResponseObject resp);

    void showTables(String dbName, ResponseObject resp);

    void showPartions(String dbName, String tableName, ResponseObject resp);

    void getFields(String dbName, String tableName, ResponseObject resp);

    void execSql(String dbName, String sql, ResponseObject resp);

    void exec(String dbName, String sql, ResponseObject resp);
}
