package etlutil;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.util.*;

public class MysqlJDBCDao2 implements Serializable {
    private String driverName = "";
    public String dbUrl = "";
    public String dbUser = "";
    public String dbPassword = "";
    private Connection conn = null;
    private Statement stmt = null;

    private static Logger log = Logger.getLogger(MysqlJDBCDao2.class);

    public MysqlJDBCDao2() {
        PropertiesReader pr = new PropertiesReader();
        try {
            pr.loadPropertiesByClassPath("mysql.properties");
            //pr.loadProperties("config/mysql.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        driverName = "com.mysql.jdbc.Driver";
        dbUrl = pr.getProperty("db.mysql.url");
        log.info("db.mysql.url:" + dbUrl);
        dbUser = pr.getProperty("db.mysql.user");
        log.info("db.mysql.user:" + dbUser);
        dbPassword = pr.getProperty("db.mysql.password");
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", dbUser);
        connectionProperties.put("password", dbPassword);

        try {
            conn = getConn();
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public MysqlJDBCDao2(String url, String user, String passwd) {
        driverName = "com.mysql.jdbc.Driver";
        dbUrl = url;
        log.info("db.mysql.url:" + dbUrl);
        dbUser = user;
        log.info("db.mysql.user:" + dbUser);
        dbPassword = passwd;

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", dbUser);
        connectionProperties.put("password", dbPassword);

        try {
            conn = getConn();
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private Connection getConn() throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    public void closeConn() throws SQLException {
        if (stmt != null)
            stmt.close();
        if (conn != null)
            conn.close();
    }

    private int count(ResultSet rest) throws SQLException {
        int rowCount = 0;
        while (rest.next()) {
            rowCount++;
        }
        rest.beforeFirst();
        return rowCount;
    }

    public HashMap<String, String> getESInfo() throws SQLException {
        HashMap<String, String> map = new HashMap<String, String>();
        String sql = "SELECT DIC_KEY,DIC_VALUE FROM sys_dic_info WHERE PARENT_DIC_TYPE='es'";
        log.info("sql = " + sql);
        ResultSet res = stmt.executeQuery(sql);
        log.info("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
            String key = res.getString("DIC_KEY");
            String value = res.getString("DIC_VALUE");
            log.debug("key: " + key + ", value: " + value);
            map.put(key, value);
        }
        res.close();
        return map;
    }

    public List<String> getRegexps(String dataSourceName) throws SQLException {
        List<String> list = new ArrayList<String>();
        String tableA = "data_source";
        String tableB = "bus_etl_reg";
        String sql = "select b.REG_VALUE as reg from " + tableB + " b join " + tableA +
                " a on a.id=b.SOURCE_ID and b.ACTION_TYPE='MATCH' and b.REG_NAME='ES' and a.name='" +
                dataSourceName + "' order by b.sort_id asc";
        log.info("sql = " + sql);
        ResultSet res = stmt.executeQuery(sql);
        log.info("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
            String reg = res.getString("reg");
            log.debug("reg: " + reg);
            list.add(reg);
        }
        res.close();
        return list;
    }

    /*public String getTopicKeys(String topic) throws SQLException {
        String sql = "select topickeys from lite.topic_keys where topic = '" + topic + "'";
        log.info("sql = " + sql);
        ResultSet res = stmt.executeQuery(sql);
        log.info("query the sql is finished, get " + count(res) + " results!");
        String returnString = "";
        if (res.next()) {
            returnString = res.getString("topickeys");
        } else {
            returnString = "";
        }
        res.close();
        return returnString;
    }*/

    public String getTopicKeys(String topic) throws SQLException {
        String sql = "select a.name as topickey from data_source a join  biz_etl_task_config b " +
                "on a.channel_uid = b.UUID and a.onoff = 'on' and b.CHANNEL_ID='" +
                topic + "'";         //etlTaskName_1
        log.info("sql = " + sql);
        ResultSet res = stmt.executeQuery(sql);
        log.info("query the sql is finished, get " + count(res) + " results!");
        String string = "";
        while (res.next()) {
            String key = res.getString("topickey");
            log.debug("topickey: " + key);
            string += key + ",";
        }
        res.close();
        String returnString = "";
        if (string.contains(",")) {
            returnString = string.substring(0, string.lastIndexOf(","));
        } else {
            returnString = string;
        }
        return returnString;
    }

    public String getStatus(String source) throws SQLException {
        String sql = "select a.is_active as active FROM bus_es_info a join data_source b on a.SOURCE_ID = b.id and b.name = '" + source + "'";
        log.info("sql = " + sql);
        ResultSet res = stmt.executeQuery(sql);
        log.info("query the sql is finished, get " + count(res) + " results!");
        String returnString = "";
        if (res.next()) {
            returnString = res.getString("active");
        } else {
            returnString = "0";
        }
        res.close();
        log.info("returnString of status = " + returnString);
        return returnString;
    }

    public String getIndex(String source) throws SQLException {
        String sql = "select a.index_name as indexName FROM bus_es_info a join data_source b on a.SOURCE_ID = b.id and b.name = '" + source + "'";
        log.info("sql = " + sql);
        ResultSet res = stmt.executeQuery(sql);
        log.info("query the sql is finished, get " + count(res) + " results!");
        String returnString = "";
        if (res.next()) {
            returnString = res.getString("indexName").trim();
        }
        if (returnString.equalsIgnoreCase("")) {
            returnString = source;
        }
        res.close();
        log.info("index of " + source + " = " + returnString);
        return returnString;
    }

}
