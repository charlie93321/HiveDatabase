package com.hbgj.hive.db;
import com.hbgj.code.CodeUtil;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class HiveJdbcConfig {

    private static final Logger logger = LoggerFactory.getLogger(HiveJdbcConfig.class);

    @Autowired
    private Environment env;

    @Bean(name = "hiveJdbcDataSource")
    @Qualifier("hiveJdbcDataSource")
    public DataSource dataSource() throws Exception {
        DataSource dataSource = new DataSource();
        dataSource.setUrl(CodeUtil.decode(env.getProperty("hive.url")));
        dataSource.setDriverClassName(env.getProperty("hive.driver-class-name"));
        dataSource.setUsername(CodeUtil.decode(env.getProperty("hive.user")));
        dataSource.setPassword(CodeUtil.decode(env.getProperty("hive.password")));
        logger.debug("Hive DataSource Inject Successfully...");
        return dataSource;
    }

    @Bean(name = "hiveJdbcTemplate")
    public JdbcTemplate hiveJdbcTemplate(@Qualifier("hiveJdbcDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name="hiveUtil")
    public HiveUtil hiveUtil(JdbcTemplate hiveJdbcTemplate){
        HiveUtil hiveUtil=new HiveUtil();
        hiveUtil.setHiveJdbcTemplate(hiveJdbcTemplate);
        return hiveUtil;
    }

}