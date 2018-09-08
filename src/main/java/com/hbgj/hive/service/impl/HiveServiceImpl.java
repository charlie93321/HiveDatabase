package com.hbgj.hive.service.impl;

import com.hbgj.hive.db.HiveUtil;
import com.hbgj.hive.entity.ResponseObject;
import com.hbgj.hive.service.HiveService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class HiveServiceImpl implements HiveService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveServiceImpl.class);
    private static final String REMOTE = "LOCAL";

    @Autowired
    private HiveUtil hiveUtil;
    @Override
    public void showDataBases(ResponseObject resp) {
        List<Map<String, Object>> list= null;
        try {
            list = hiveUtil.getDatabaseMeta(" SHOW DATABASES ");
            List<String> dbs=new ArrayList<String>();
            for (int i = 0; i < list.size(); i++) {
                Map<String, Object> kv=list.get(i);
                if(null!=kv) {
                    Object dbName=null;
                    if(REMOTE.equals("REMOTE")){
                        dbName=kv.get(HiveUtil.REMOTE_DB_NAME);
                    }else{
                        dbName=kv.get(HiveUtil.LOCAL_DB_NAME);
                    }
                    if(null!=dbName )dbs.add(dbName.toString());
                }
            }
            resp.setData(dbs);
            resp.setSuccess(true);
            resp.setMessage("your request is success");
        } catch (Exception e) {
            LOGGER.info("当查询数据库执行- sql: SHOW DATABASES 时抛出异常",e);
            resp.setData(null);
            resp.setSuccess(false);
            resp.setMessage(e.getMessage());
        }


    }




    public void showTables(String dbName,ResponseObject resp) {
        List<Map<String, Object>> list= null;
        try {
            list = hiveUtil.getDatabaseMeta(dbName," SHOW TABLES ");
            List<String> tabs=new ArrayList<String>();
            for (int i = 0; i < list.size(); i++) {
                Map<String, Object> kv=list.get(i);
                if(null!=kv) {
                    Object tname=null;
                    if(REMOTE.equals("REMOTE")){
                        tname=kv.get(HiveUtil.REMOTE_TABLE_NAME);
                    }else{
                        tname=kv.get(HiveUtil.LOCAL_TABLE_NAME);
                    }
                    if(null!=tname)tabs.add(tname.toString());
                }
            }
            resp.setData(tabs);
            resp.setSuccess(true);
            resp.setMessage("your request is success");
        } catch (Exception e) {
            LOGGER.info("当查询数据库执行- sql: SHOW TABLES 时抛出异常",e);
            resp.setData(null);
            resp.setSuccess(false);
            resp.setMessage(e.getMessage());
        }
    }

    @Override
    public void showPartions(String dbName, String tableName, ResponseObject resp) {
        List<Map<String, Object>> list= null;
        try {
            list = hiveUtil.getPatitions(dbName,tableName);
            if(null==list){
                resp.setData(null);
            }else {
                List<String> partions=new ArrayList<String>();
                for (int i = 0; i < list.size(); i++) {
                    Map<String, Object> kv = list.get(i);
                    if (null != kv) {
                         Object partion=null;
                        if(REMOTE.equals("REMOTE")){
                            partion=kv.get(HiveUtil.REMOTE_TABLE_PARTITION);
                        }else{
                            partion=kv.get(HiveUtil.LOCAL_TABLE_PARTITION);
                        }
                         if(null!=partion)partions.add(partion.toString());
                    }
                }
                resp.setData(partions);
            }
            resp.setSuccess(true);
            resp.setMessage("your request is success");
        } catch (Exception e) {
            LOGGER.info("当查询数据库执行- sql: SHOW PATITIONS 时抛出异常",e);
            resp.setData(null);
            resp.setSuccess(false);
            resp.setMessage(e.getMessage());
        }
    }

    @Override
    public void getFields(String dbName, String tableName, ResponseObject resp) {
        List<Map<String, Object>> list= null;
        try {
            resp.setData(
                    hiveUtil.getDatabaseMeta(dbName," desc "+tableName)
            );
            resp.setSuccess(true);
            resp.setMessage("your request is success");
        } catch (Exception e) {
            LOGGER.info("当查询数据库执行- sql: desc table  时抛出异常",e);
            resp.setData(null);
            resp.setSuccess(false);
            resp.setMessage(e.getMessage());
        }
    }

    @Override
    public void execSql(String dbName, String sql, ResponseObject resp) {
        try {
            resp.setData(
                    hiveUtil.execQuery(dbName,sql)
            );
            resp.setSuccess(true);
            resp.setMessage("your request is success");
        } catch (Exception e) {
            LOGGER.info("当查询数据库执行- sql: "+sql+" 时抛出异常",e);
            resp.setData(null);
            resp.setSuccess(false);
            resp.setMessage(e.getMessage());
        }
    }

    @Override
    public void exec(String dbName, String sql, ResponseObject resp) {
        resp.setData(null);
        try {
            hiveUtil.exec(dbName,sql);
            resp.setSuccess(true);
            resp.setMessage("your request is success");
        } catch (Exception e) {
            LOGGER.info("当查询数据库执行- sql: "+sql+" 时抛出异常",e);

            resp.setSuccess(false);
            resp.setMessage(e.getMessage());
        }
    }
}
