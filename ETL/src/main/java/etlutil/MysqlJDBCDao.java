package etlutil;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.util.*;

@Deprecated
public class MysqlJDBCDao implements Serializable {
    //private ResultSet res;
    private String driverName = "";
    public String dbUrl = "";
    public String dbUser = "";
    public String dbPassword = "";
    //public String sql = "";
    //private String configtable = "";
    //public String columntable = "";
    private Connection conn = null;
    private Statement stmt = null;
    //public HashMap<String, String> alltypes = null;


    private static Logger log = Logger.getLogger(MysqlJDBCDao.class);

    public MysqlJDBCDao() {
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
            //alltypes = getAllTypes();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public MysqlJDBCDao(String url, String user, String passwd) {
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
            //alltypes = getAllTypes();
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

    public void save(String jobId, String appId, String appName, String timeStamp) {
        String query = " INSERT INTO applicationID(jobId, appId, appName, timestamp)"
                + " VALUES (?, ?, ?, ?)";
        PreparedStatement preparedStmt = null;
        try {
            preparedStmt = conn.prepareStatement(query);
            preparedStmt.setString(1, jobId);
            preparedStmt.setString(2, appId);
            preparedStmt.setString(3, appName);
            preparedStmt.setString(4, timeStamp);
            preparedStmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private int count(ResultSet rest) throws SQLException {
        int rowCount = 0; //寰楀埌褰撳墠琛屽彿锛屼篃灏辨槸璁板綍鏁�
        while (rest.next()) {
            rowCount++;
        }
        rest.beforeFirst();
        return rowCount;
    }

    public String getRegexpUUID(String name) throws SQLException {
        String string = "";
        String tableA = "bus_config_data_change";
        String tableB = "data_source";
        String sql = "select a.uuid as uuid from " + tableA +
                " a join " + tableB
                + " b on a.SOURCE_ID = b.id and b.name = '"
                + name + "' and a.name = 'etlregexp' ";
        log.info("========sql========" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        if (res.next()) {
            string = res.getString("uuid");
        }
        return string;
    }

    public String getMappingUUID(String name) throws SQLException {
        String string = "";
        String tableA = "bus_config_data_change";
        String tableB = "data_source";
        String sql = "select a.uuid as uuid from " + tableA +
                " a join " + tableB
                + " b on a.SOURCE_ID = b.id and b.name = '"
                + name + "' and a.name = 'etlregconfig' ";
        log.info("========sql========" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        if (res.next()) {
            string = res.getString("uuid");
        }
        return string;
    }

    public String getUUID(String name, String sourceName) {
        String sql = "SELECT * FROM bus_config_data_change WHERE `name`=? AND source_id=?";
        int sourceID = getSourceID(sourceName);
        PreparedStatement preStat;
        try {
            preStat = conn.prepareStatement(sql);
            preStat.setString(1, name);
            preStat.setInt(2, sourceID);
            ResultSet resultSet = preStat.executeQuery();
            if (resultSet.next()) {
                return resultSet.getString("uuid");
            } else {
                return null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public int getSourceID(String sourceName) {
        String sql = "SELECT * FROM data_source WHERE `name`=?";
        String sql_getID = "";
        PreparedStatement preStat;
        try {
            preStat = conn.prepareStatement(sql);
            preStat.setString(1, sourceName);
            ResultSet resultSet = preStat.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("id");
            } else {
                return -1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public ResultSet query(String sql) throws SQLException {
        //res = stmt.executeQuery(sql);
        //System.out.println("query the sql is finished, get " + count(res) + " results!");
        ResultSet res = stmt.executeQuery(sql);
        return res;
    }

    @Deprecated
    public HashMap<String, String> getAllTypes() throws SQLException {
        HashMap<String, String> map = new HashMap<>();
        String sql = "SELECT CONFIG_NAME,PROPERTY_TYPE FROM " + "bus_etl_config";
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
//		System.out.println("鎵ц select * query 杩愯缁撴灉:");
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
            String key = res.getString(1);
            String value = res.getString(2);
            map.put(key, value);
        }
        res.beforeFirst();
        return map;
    }

    public HashMap<String, String> getPrestoInfo() throws SQLException {
        HashMap<String, String> map = new HashMap<>();
        String sql = "select dic_key,dic_value from sys_dic_info where PARENT_DIC_TYPE='prestojdbcdao'";
        log.info("========sql========" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
//            System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3));
            String key = res.getString("dic_key");
            String value = res.getString("dic_value");
            map.put(key, value);
        }
        return map;
    }

    @Deprecated
    public HashMap<String, String> getConfig(String type) throws SQLException {
        HashMap<String, String> map = new HashMap<>();
        String sql = "select * from " + "bus_etl_config" + " where CONFIG_NAME = '" + type + "'";
        log.info("========sql========" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
//            System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3));
            String key = res.getString(2);
            String value = res.getString(3);
            map.put(key, value);
        }
        res.beforeFirst();
        return map;
    }

    /**
     * @param sourceName
     * @param programName
     * @throws SQLException
     */
    public HashMap<String, String> getConfig(String sourceName, String programName) throws SQLException {
        HashMap<String, String> map = new HashMap<String, String>();
        String sql = "SELECT config_key, config_value FROM data_source s INNER JOIN bus_source_config c ON s.ID = c.SOURCE_ID " +
                " WHERE LOWER(s.name) = LOWER(?) AND LOWER(c.CONF_TYPE)= LOWER(?)";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.setString(1, sourceName);
        preparedStatement.setString(2, programName);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            String configKey = resultSet.getString("config_key");
            String configValue = resultSet.getString("config_value");
            log.debug("configKey: " + configKey + " configValue: " + configValue);
            map.put(configKey, configValue);
        }
        resultSet.close();
        preparedStatement.close();
        return map;
    }

    /**
     * @param sourceName
     * @param programName
     * @return
     * @throws SQLException
     */
    public Integer getModeId(String sourceName, String programName) throws SQLException {
        Integer modeId = null;
        String sql = "SELECT  DISTINCT c.MODE_ID  FROM data_source s INNER JOIN bus_source_config c ON s.ID = c.SOURCE_ID " +
                " WHERE LOWER(s.name) = LOWER(?) AND LOWER(c.CONF_TYPE)= LOWER(?)";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.setString(1, sourceName);
        preparedStatement.setString(2, programName);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            modeId = resultSet.getInt("mode_id");
            log.debug("modeId: " + modeId);
        }
        resultSet.close();
        preparedStatement.close();
        return modeId;
    }

    @Deprecated
    public String getStringConfig(String type) throws SQLException {
        //HashMap<String, String> map = new HashMap<String, String>();
        String ret = "";
        String sql = "select * from " + "bus_etl_config" + " where CONFIG_NAME = '" + type + "'";
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
            //System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3));
            ret += res.getString(2) + "=" + res.getString(3) + "\n";
        }
        res.beforeFirst();
        return ret;
    }

    /**
     * @param programName
     * @return
     * @throws SQLException
     */
    public HashMap<String, String> getConfigByProgram(String programName) throws SQLException {
        HashMap<String, String> map = new HashMap<String, String>();
        String sql = "SELECT config_key, config_value FROM data_source s INNER JOIN bus_source_config c ON s.ID = c.SOURCE_ID " +
                " WHERE  LOWER(c.CONF_TYPE)= LOWER(?)";
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        preparedStatement.setString(1, programName);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            String configKey = resultSet.getString("config_key");
            String configValue = resultSet.getString("config_value");
            log.debug("configKey: " + configKey + " configValue: " + configValue);
            map.put(configKey, configValue);
        }
        resultSet.close();
        preparedStatement.close();
        return map;
    }

    public HashMap<String, String> getETLConfig(String type) throws SQLException {
        String table1 = "bus_etl_reg_conf";
        String table2 = "data_source";
        HashMap<String, String> map = new HashMap<>();
        ArrayList<String> sourceName = new ArrayList<>();
        ArrayList<String> tableName = new ArrayList<>();
        ArrayList<String> allData = new ArrayList<>();
        ArrayList<String> dbName = new ArrayList<>();
        ArrayList<String> dbType = new ArrayList<>();
        String sql = "select distinct b.name as name, a.table_name as table_name, a.ALL_DATA as all_data," +
                "a.db_name as db_name, a.db_type as db_type from " + table1
                + " a join " + table2 + " b on a.SOURCE_ID = b.id and b.name = '" + type + "'";
        log.info("========sql========" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
//            System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3));
            sourceName.add(res.getString("name"));
            tableName.add(res.getString("table_name"));
            allData.add(res.getString("all_data"));
            dbName.add(res.getString("db_name"));
            dbType.add(res.getString("db_type"));
        }
        res.beforeFirst();
        for (int i = 0; i < tableName.size(); i++) {
            String str = "{\n" +
                    "\t\"processors\":\n" +
                    "\t\t{\"" + sourceName.get(i) + "\":\n" +
                    "\t\t{\n" +
                    "\t\t\t\"db_type\": \"" + dbType.get(i) + "\",\n" +
                    "\t\t\t\"db_name\": \"" + dbName.get(i) + "\",\n" +
                    "\t\t\t\"table_name\": \"" + tableName.get(i) + "\",\n" +
                    "\t\t\t\"all_data\":" + (allData.get(i).equalsIgnoreCase("1") ? "true" : "false") + ",\n" +
                    "\t\t\t\"pattern_list\":\n" +
                    "\t\t\t[\n";
            //str += getETLPatternConfig(type, tableName.get(i));
            str += getETLPatternConfig(type);
            str += "\n\t\t\t]\n" +
                    "\t\t}\n" +
                    "\t}\n" +
                    "}";
            map.put(tableName.get(i), str);
        }
        return map;
    }

    private String getETLPatternConfig(String type) throws SQLException {
        String tableA = "bus_etl_reg";
        String tableB = "data_source";
        String string = "";
        ArrayList<String> actionType = new ArrayList<>();
        ArrayList<String> regValue = new ArrayList<>();

        String sql = "select distinct b.ACTION_TYPE as action_type ,b.REG_VALUE as reg_value, " +
                "b.sort_id from " + tableA + " b join " + tableB + " a on a.id=b.SOURCE_ID  and a.name='"
                + type + "' order by b.sort_id asc";
        log.info("========sql========" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
//            System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3));
            actionType.add(res.getString("action_type"));
            regValue.add(res.getString("reg_value"));
        }
        res.beforeFirst();

        String columnsString = getETLColumnConfig(type);

        for (int i = 0; i < regValue.size(); i++) {
            String str = "\t\t\t\t{\n" +
                    "\t\t\t\t\t\"pattern_id\": " + (i + 1) + ",\n" +
                    "\t\t\t\t\t\"pattern\": \"" + regValue.get(i) + "\",\n" +
                    "\t\t\t\t\t\"action\":\"" + actionType.get(i) + "\",\n" +
                    "\t\t\t\t\t\"columns\":\n" +
                    "\t\t\t\t\t[\n";
            //str += getETLColumnConfig(type, regID.get(i));
            str += columnsString;
            str += "\n\t\t\t\t\t]\n" +
                    "\t\t\t\t}";
            if (i < regValue.size() - 1) {
                str += ",\n";
            }
            string += str;
        }
        return string;
    }

    private String getETLColumnConfig(String type) throws SQLException {
        String string = "";
        String tableA = "bus_etl_reg_conf";
        String tableB = "data_source";
        String sql = "select a.COL_NAME as col_name, a.REG_EXP_NAME as group_name, " +
                "a.COL_TYPE as col_type, a.DEFAULT_VALUE as default_value, " +
                "a.FUNC_NAME as function_name, a.FUNC_PARAM as function_para from " +
                tableA + " a join " + tableB + " b on a.SOURCE_ID = b.id and b.name = '"
                + type + "'";
        log.info("========sql========" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
            String str = "";
            str += "\t\t\t\t\t\t{\n" +
                    "\t\t\t\t\t\t\t\"regExpName\":\"" + res.getString("group_name") + "\",\n" +
                    "\t\t\t\t\t\t\t\"columnName\":\"" + res.getString("col_name") + "\",\n" +
                    "\t\t\t\t\t\t\t\"type\":\"" + res.getString("col_type") + "\"";
            if (!res.getString("default_value").trim().equalsIgnoreCase("")) {
                str += ",\n\t\t\t\t\t\t\t\"value\": \"" + res.getString("default_value").trim() + "\"";
            }
            if (!res.getString("function_name").trim().equalsIgnoreCase("")) {
                str += ",\n\t\t\t\t\t\t\t\"function\": \"" + res.getString("function_name").trim() + "\"";
            }
            if (!res.getString("function_para").trim().equalsIgnoreCase("")) {
                str += ",\n\t\t\t\t\t\t\t\"arg\": \"" + res.getString("function_para").trim() + "\"";
            }
            str += "\n\t\t\t\t\t\t},\n";
            string += str;
        }
        res.beforeFirst();
        return string.substring(0, string.length() - 2);
    }

    private String getETLPatternConfig(String type, String table) throws SQLException {
        String tableA = "bus_etl_reg_conf";
        String tableB = "bus_etl_reg";
        String tableC = "data_source";
        String string = "";
        ArrayList<String> actionType = new ArrayList<>();
        ArrayList<String> regValue = new ArrayList<>();
        ArrayList<Integer> regID = new ArrayList<>();
        String sql = "select distinct b.ACTION_TYPE as action_type, b.REG_VALUE as reg_value, b.ID as reg_id"
                //+",b.SORT_id as sort_id"
                + " from " + tableA
                + " a join " + tableC + " c on a.SOURCE_ID = c.id and c.name = '" + type
                + "' join " + tableB + " b on a.REG_ID = b.id and a.TABLE_NAME = '" + table + "'"
                //+"order by b.sort_id asc"
                ;
        log.info("========sql========" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
//            System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3));
            actionType.add(res.getString("action_type"));
            regValue.add(res.getString("reg_value"));
            regID.add(res.getInt("reg_id"));
            //sortID.add(res.getInt("sort_id"));
        }
        res.beforeFirst();

        for (int i = 0; i < regID.size(); i++) {
            String str = "\t\t\t\t{\n" +
                    "\t\t\t\t\t\"pattern_id\": " + (i + 1) + ",\n" +
                    "\t\t\t\t\t\"pattern\": \"" + regValue.get(i) + "\",\n" +
                    "\t\t\t\t\t\"action\":\"" + actionType.get(i) + "\",\n" +
                    "\t\t\t\t\t\"columns\":\n" +
                    "\t\t\t\t\t[\n";
            str += getETLColumnConfig(type, regID.get(i));
            str += "\n\t\t\t\t\t]\n" +
                    "\t\t\t\t}";
            if (i < regID.size() - 1) {
                str += ",\n";
            }
            string += str;
        }
        return string;
    }

    private String getETLColumnConfig(String type, int pattern_id) throws SQLException {
        String string = "";
        String tableA = "bus_etl_reg_conf";
        String tableB = "data_source";
        String sql = "select a.COL_NAME as col_name, a.REG_EXP_NAME as group_name, " +
                "a.COL_TYPE as col_type, a.DEFAULT_VALUE as default_value, " +
                "a.FUNC_NAME as function_name from " + tableA + " a join " + tableB
                + " b on a.SOURCE_ID = b.id and b.name = '" + type
                + "' and a.REG_ID = " + pattern_id;
        log.info("========sql========" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
            String str = "";
            str += "\t\t\t\t\t\t{\n" +
                    "\t\t\t\t\t\t\t\"regExpName\":\"" + res.getString("group_name") + "\",\n" +
                    "\t\t\t\t\t\t\t\"columnName\":\"" + res.getString("col_name") + "\",\n" +
                    "\t\t\t\t\t\t\t\"type\":\"" + res.getString("col_type") + "\"";
            if (!res.getString("default_value").trim().equalsIgnoreCase("")) {
                str += ",\n\t\t\t\t\t\t\t\"value\": \"" + res.getString("default_value").trim() + "\"";
            }
            if (!res.getString("function_name").trim().equalsIgnoreCase("")) {
                str += ",\n\t\t\t\t\t\t\t\"function\": \"" + res.getString("function_name").trim() + "\"";
            }
            str += "\n\t\t\t\t\t\t},\n";
            string += str;
        }
        res.beforeFirst();
        return string.substring(0, string.length() - 2);
    }

    public String getDbNameInDictionary() throws SQLException {
        String sql = "SELECT dic_value FROM sys_dic_info WHERE PARENT_DIC_TYPE='DAO_API' AND DIC_NAME='DAO_API_SCHEMA'";
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            return res.getString("dic_value");
        } else {
            return "";
        }
    }

    public String getBrokersInDictionary() throws SQLException {
        String sql = "SELECT DIC_VALUE FROM sys_dic_info WHERE PARENT_DIC_TYPE='KAFKA' AND DIC_NAME='BROKER_LIST'";
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            return res.getString("dic_value");
        } else {
            return "";
        }
    }

    public String getTopicKeys(String topic) throws SQLException {
        String sql = "select topickeys from lite.topic_keys where topic = '"+topic+"'";
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            return res.getString("topickeys");
        } else {
            return "";
        }
    }

    public HashMap<String, String> getESInfo() throws SQLException {
        HashMap<String, String> map = new HashMap<String, String>();
        String sql = "SELECT DIC_KEY,DIC_VALUE FROM sys_dic_info WHERE PARENT_DIC_TYPE='es'";
        ResultSet res = stmt.executeQuery(sql);
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

    public HashMap<String, ArrayList<String>> getBatchConfig(String id) throws SQLException {
        String table1 = "BUS_BATCH_DETAIL";
        String table2 = "BUS_BATCH_MAIN";
        HashMap<String, ArrayList<String>> map = new HashMap<>();
        ArrayList<String> sqlList = new ArrayList<>();
        ArrayList<String> mode = new ArrayList<>();
        ArrayList<String> dbType = new ArrayList<>();
        ArrayList<String> tableName = new ArrayList<>();

        String sql = "select a.hql as sqllist, a.output_mode as mode, " +
                "a.output_type as dbtype, a.target_table_name as tablename " +
                "from " + table1 + " a where a.batch_id = "
                + id + " order by a.step asc";
        log.info("========sql========" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
            sqlList.add("".equalsIgnoreCase(res.getString("sqllist")) ? "" : res.getString("sqllist"));
            mode.add("".equalsIgnoreCase(res.getString("mode")) ? "" : res.getString("mode"));
            dbType.add("".equalsIgnoreCase(res.getString("dbtype")) ? "" : res.getString("dbtype"));
            tableName.add("".equalsIgnoreCase(res.getString("tablename")) ? "" : res.getString("tablename"));
        }
        res.beforeFirst();

        map.put("sqlList", sqlList);
        map.put("mode", mode);
        map.put("dbType", dbType);
        map.put("tableName", tableName);

        return map;
    }

    public HashMap<String, ArrayList<String>> getBatchConfigByUUID(String uuid) throws SQLException {
        String table1 = "BUS_BATCH_DETAIL";
        String table2 = "BUS_BATCH_MAIN";
        HashMap<String, ArrayList<String>> map = new HashMap<>();
        ArrayList<String> sqlList = new ArrayList<>();
        ArrayList<String> mode = new ArrayList<>();
        ArrayList<String> dbType = new ArrayList<>();
        ArrayList<String> tableName = new ArrayList<>();

        String sql = "select a.hql as sqllist, a.output_mode as mode, " +
                "a.output_type as dbtype, a.target_table_name as tablename " +
                "from " + table1 + " a join " + table2 + " b on a.batch_id = b.id " +
                "and b.UUID = '" + uuid + "' order by a.step asc";
        log.info("========sql========" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("query the sql is finished, get " + count(res) + " results!");
        while (res.next()) {
            sqlList.add("".equalsIgnoreCase(res.getString("sqllist")) ? "" : res.getString("sqllist"));
            mode.add("".equalsIgnoreCase(res.getString("mode")) ? "" : res.getString("mode"));
            dbType.add("".equalsIgnoreCase(res.getString("dbtype")) ? "" : res.getString("dbtype"));
            tableName.add("".equalsIgnoreCase(res.getString("tablename")) ? "" : res.getString("tablename"));
        }
        res.beforeFirst();

        map.put("sqlList", sqlList);
        map.put("mode", mode);
        map.put("dbType", dbType);
        map.put("tableName", tableName);

        return map;
    }

    /**
     * generic query method
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> queryAll(String sql) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList();

        PreparedStatement preStat = this.conn.prepareStatement(sql);

        //ResultSet resultSet = stmt.executeQuery(sql);
        ResultSet resultSet = preStat.executeQuery();

        while (resultSet != null && resultSet.next()) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            Map<String, Object> row = new HashMap();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                Object value = resultSet.getObject(i);
                String columnName = metaData.getColumnName(i);
                row.put(columnName, value);
            }
            rows.add(row);

        }
        return rows;
    }

    /**
     * 通用查询函数
     *
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> queryByParams(String sql, List<Object> params) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList();

        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        for (int i = 1; i <= params.size(); i++) {
            Object param = params.get(i - 1);
            if (param instanceof String) {
                preparedStatement.setString(i, (String) param);
            } else if (param instanceof Integer) {
                preparedStatement.setInt(i, (Integer) param);
            } else if (param instanceof Double) {
                preparedStatement.setDouble(i, (Double) param);
            } else if (param instanceof Timestamp) {
                preparedStatement.setTimestamp(i, (Timestamp) param);
            }
        }
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            Map<String, Object> row = new HashMap();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                Object value = resultSet.getObject(i);
                String columnName = metaData.getColumnName(i);
                row.put(columnName, value);
            }
            rows.add(row);
        }
        resultSet.close();
        preparedStatement.close();
        return rows;
    }
}
