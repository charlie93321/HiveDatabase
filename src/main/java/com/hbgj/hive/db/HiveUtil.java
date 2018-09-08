package com.hbgj.hive.db;

import org.springframework.jdbc.core.JdbcTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

public class HiveUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveUtil.class);
    private JdbcTemplate hiveJdbcTemplate;

    public static final String REMOTE_DB_NAME="result";
    public static final String REMOTE_TABLE_NAME="tableName";
    public static final String REMOTE_TABLE_PARTITION="result";
    public static final String LOCAL_DB_NAME="database_name";
    public static final String LOCAL_TABLE_NAME="tab_name";
    public static final String LOCAL_TABLE_PARTITION="partition";

    public JdbcTemplate getHiveJdbcTemplate() {
        return hiveJdbcTemplate;
    }

    public void setHiveJdbcTemplate(JdbcTemplate hiveJdbcTemplate) {
        this.hiveJdbcTemplate = hiveJdbcTemplate;
    }

    public List<Map<String, Object>> getDatabaseMeta(String sqlForMeta)throws Exception{
        try {
            return  hiveJdbcTemplate.queryForList(sqlForMeta);
        }catch (Exception e){
            LOGGER.info("查询hive数据库是抛出异常",e.getMessage());
            throw e;
        }
    }
    public List<Map<String, Object>> getDatabaseMeta(String dbName,String sqlForMeta) throws  Exception{
        try {
            hiveJdbcTemplate.execute("use " + dbName);
            return hiveJdbcTemplate.queryForList(sqlForMeta);
        }catch (Exception e){
            LOGGER.info("查询hive数据库是抛出异常",e.getMessage());
            throw e;
        }
    }

    /**
     *
     * @param dbName 数据库名
     * @param tableName 表名
     * @return   返回 [] 或其 他不为空的集合 表示这个是一个分区表
     *           返回null 表示这个表为非分区表
     *           其他情况则会抛出异常
     * @throws Exception
     */
    public List<Map<String, Object>> getPatitions(String dbName, String tableName)throws  Exception{
        try {
            hiveJdbcTemplate.execute("use " + dbName);
            return hiveJdbcTemplate.queryForList("show PARTITIONS "+tableName);
        }catch (Exception e){
             if(e.getMessage().contains("is not a partitioned table")){
                 return null;
             }else{
                 throw e;
             }
        }
    }

   public List<Map<String, Object>> execQuery(String dbName,String sql) throws  Exception{
       try {
           hiveJdbcTemplate.execute("use " + dbName);
           return hiveJdbcTemplate.queryForList(sql);
       }catch (Exception e){
               throw e;
       }
   }

    public void exec(String dbName, String sql) throws  Exception{
        try {
            hiveJdbcTemplate.execute("use " + dbName);
            hiveJdbcTemplate.execute(sql);
        }catch (Exception e){
            throw e;
        }
    }
}
