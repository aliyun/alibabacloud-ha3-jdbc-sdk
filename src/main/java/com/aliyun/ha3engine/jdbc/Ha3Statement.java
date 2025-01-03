package com.aliyun.ha3engine.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.druid.util.JdbcConstants;
import com.aliyun.ha3engine.jdbc.common.config.Ha3KvPairBuilder;
import com.aliyun.ha3engine.jdbc.common.exception.Ha3DriverException;
import com.aliyun.ha3engine.jdbc.common.utils.Ha3ToolUtils;
import com.aliyun.ha3engine.jdbc.sdk.client.CloudClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ha3 JDBC statement
 *
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class Ha3Statement implements Statement, Ha3Wrapper {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected Ha3ResultSet ha3ResultSet;
    protected final Ha3Connection ha3Connection;
    protected CloudClient cloudClient;

    public Ha3Statement(Ha3Connection ha3Connection) {
        this.ha3Connection = ha3Connection;
    }

    public Ha3Statement(Ha3Connection ha3Connection, CloudClient cloudClient) throws SQLException {
        this.ha3Connection = ha3Connection;
        this.cloudClient = cloudClient;
    }

    /**
     * 执行 sql,此方法仅支持select语句
     *
     * @param sql
     * @return ResultSet
     * @throws SQLException
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        sql = sql.trim();

        sql = sql.replaceAll("\r", " ").replaceAll("\n", " ");
        String sqlNorm = sql.trim().toLowerCase();
        if (sqlNorm.startsWith("query=")) {
            sqlNorm = sqlNorm.replaceFirst("query=", "");
        }
        int startOfStmtPos = findStartOfStatement(sqlNorm);
        char firstStmtChar = Character.toUpperCase(sqlNorm.charAt(startOfStmtPos));
        if (firstStmtChar == 'S') {
            String fromIndex = null;
            String[] sqlParts = sql.split(" ");
            for (int i = 0; i < sqlParts.length; i++) {
                if ("from".equals(sqlParts[i].toLowerCase())) {
                    fromIndex = sqlParts[i + 1];
                    break;
                }
            }
            ha3Connection.setSchema(fromIndex);
            this.execute(sql);
            return ha3ResultSet;
        } else {
            throw new SQLException(
                "Provided query type '" + firstStmtChar + "' is not supported!");
        }
    }

    protected static int findStartOfStatement(String sqlNorm) {
        return findStartOfStatement(sqlNorm, 0);
    }

    private static int findStartOfStatement(String sql, int pos) {
        int statementStartPos = pos;
        while (Character.isWhitespace(sql.charAt(statementStartPos))) {
            statementStartPos++;
        }
        if (Ha3ToolUtils.startsWithIgnoreCaseAndWs(sql, "/*", statementStartPos)) {
            statementStartPos = statementStartPos + 2;
            statementStartPos = sql.indexOf("*/", statementStartPos);
            if (statementStartPos == -1) {
                statementStartPos = 0;
            } else {
                statementStartPos += 2;
            }
        } else if (Ha3ToolUtils.startsWithIgnoreCaseAndWs(sql, "--", statementStartPos)
            || Ha3ToolUtils.startsWithIgnoreCaseAndWs(sql, "#", statementStartPos)) {
            statementStartPos = sql.indexOf('\n', statementStartPos);

            if (statementStartPos == -1) {
                statementStartPos = sql.indexOf('\r', statementStartPos);
                if (statementStartPos == -1) {
                    statementStartPos = 0;
                }
            }
        }

        char ch = sql.charAt(statementStartPos);
        int ich = (int)ch;
        if ((ich >= 65 && ich <= 90) || (ich >= 97 && ich <= 122)) {
            return statementStartPos;
        } else {
            return findStartOfStatement(sql, statementStartPos);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ha3 Cloud client not support update!");
    }

    @Override
    public void close() {
        synchronized (this) {
            this.ha3Connection.statements.remove(this);
        }
    }

    @Override
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public int getMaxRows() {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public int getQueryTimeout() {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    /**
     * 判断查询的index是否存在，存在则请求Server获取数据
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    @Override
    public boolean execute(String sql) throws SQLException {
        initResultSet(sql);
        return true;
    }

    /**
     * 发起远端调用，请求Ha3 Server获取数据
     *
     * @param sql
     * @throws SQLException
     */
    protected void initResultSet(String sql) throws Ha3DriverException {
        if (this.ha3Connection.getHa3Config().isEnableDetailLog()) {
            logger.info("Ha3Statement sql:" + sql);
        }
        String result = null;
        StringBuilder res = new StringBuilder();
        if (this.ha3Connection.getHa3Config().isEnableDynamicParams()) {
            //开启sql动态参数化
            if (sql.contains("kvpair")) {
                //sql带了kvpair，默认用户自己拼了动态参数等
                res.append(sql);
                result = cloudClient.query(res.toString());
                this.ha3ResultSet = new Ha3ResultSet(result, this);
            } else {
                boolean canConvert = true;
                String originalSql = sql;

                // 正则表达式匹配 SELECT 关键字，忽略大小写
                Pattern selectPattern = Pattern.compile("\\bSELECT\\b", Pattern.CASE_INSENSITIVE);

                Matcher selectMatcher = selectPattern.matcher(sql);

                // 计算匹配到的 SELECT 关键字数量
                int selectCount = 0;
                while (selectMatcher.find()) {
                    selectCount++;
                }

                String beginSql = "";
                String endSql = "";
                int firstIndex = -1;
                int lastIndex = -1;
                if (selectCount > 1) {
                    firstIndex = sql.indexOf("(");
                    lastIndex = sql.lastIndexOf(")");
                    if (-1 != firstIndex && -1 != lastIndex) {
                        beginSql = sql.substring(0, firstIndex + 1);
                        endSql = sql.substring(lastIndex, sql.length());
                        sql = sql.substring(firstIndex + 1, lastIndex);
                    }
                }

                // 提取LIMIT子句中的常量值
                Pattern limitPattern = Pattern.compile("(?i)limit\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
                Matcher limitMatcher = limitPattern.matcher(sql);
                int limitValue = -1;
                if (limitMatcher.find()) {
                    limitValue = Integer.parseInt(limitMatcher.group(1));
                    sql = sql.replaceFirst("(?i)limit\\s+\\d+", "");
                }

                // 提取OFFSET子句中的常量值
                Pattern offsetPattern = Pattern.compile("(?i)offset\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
                Matcher offsetMatcher = offsetPattern.matcher(sql);
                int offsetValue = -1;
                if (offsetMatcher.find()) {
                    offsetValue = Integer.parseInt(offsetMatcher.group(1));
                    sql = sql.replaceFirst("(?i)offset\\s+\\d+", "");
                }

                // 创建不区分大小写的识别where的Pattern对象
                Pattern pattern = Pattern.compile(Pattern.quote("where"), Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(sql);
                String firstPart = "";
                String secondPart = "";
                String thirdPart = "";
                if (matcher.find()) {
                    // 查询的第一部分
                    firstPart = sql.substring(0, matcher.start());
                    // 查询的第二部分
                    secondPart = sql.substring(matcher.start(), sql.length());
                    Pattern groupByPattern = Pattern.compile(Pattern.quote("group by"), Pattern.CASE_INSENSITIVE);
                    Matcher groupByMatcher = groupByPattern.matcher(secondPart);
                    if (groupByMatcher.find()) {
                        // 查询的第三部分
                        thirdPart = secondPart.substring(groupByMatcher.start(), secondPart.length());
                        // 查询的第二部分
                        secondPart = secondPart.substring(0, groupByMatcher.start());
                    }
                } else {
                    firstPart = sql;
                }

                // 创建不区分大小写的识别case when的Pattern对象
                Pattern caseWhenPattern = Pattern.compile(Pattern.quote("case when"), Pattern.CASE_INSENSITIVE);
                Matcher caseWhenMatcher = caseWhenPattern.matcher(secondPart);
                if (caseWhenMatcher.find()) {
                    canConvert = false;
                }

                // 创建不区分大小写的识别in的Pattern对象
                Pattern inPattern = Pattern.compile(Pattern.quote(" in"), Pattern.CASE_INSENSITIVE);
                Matcher inMatcher = inPattern.matcher(secondPart);
                if (inMatcher.find()) {
                    canConvert = false;
                }

                final DbType dbType = JdbcConstants.MYSQL;
                // 参数化SQL是输出的参数保存在这个List中
                List<Object> outParameters = new ArrayList<Object>();
                String psql = "";
                if (secondPart != null && StringUtils.isNotEmpty(secondPart)) {
                    secondPart = "select * from mock " + secondPart;
                    if (canConvert) {
                        try {
                            psql = ParameterizedOutputVisitorUtils.parameterize(secondPart, dbType, outParameters);
                        } catch (Exception e) {
                            logger.info("sql can not convert to dynamic params!");
                            canConvert = false;
                        }
                    } else {
                        logger.info("sql can not convert to dynamic params!");
                    }

                    psql = psql.replace("SELECT *\n"
                        + "FROM mock\n", "");
                }

                psql = firstPart + psql + " " + thirdPart;

                // 恢复LIMIT子句
                if (limitValue > 0) {
                    int limitIndex = psql.length();
                    psql = psql.substring(0, limitIndex) + " LIMIT " + limitValue;
                }

                // 恢复OFFSET子句
                if (offsetValue >= 0) {
                    int offsetIndex = psql.length();
                    psql = psql.substring(0, offsetIndex) + " OFFSET " + offsetValue;
                }

                if ((selectCount > 1) && (-1 != firstIndex && -1 != lastIndex)) {
                    psql = beginSql + psql + endSql;
                }

                if (canConvert) {
                    try {
                        PreparedStatement preparedStatement = this.ha3Connection.prepareStatement(psql);
                        for (int i = 0; i < outParameters.size(); i++) {
                            preparedStatement.setObject(i + 1, outParameters.get(i));
                        }
                        Ha3KvPairBuilder kvPairBuilder = new Ha3KvPairBuilder();
                        kvPairBuilder.enableCache();
                        kvPairBuilder.setPrepareLevel("jni.post.optimize");
                        kvPairBuilder.setDatabaseName("general");
                        kvPairBuilder.enableUrlEncodeData();
                        if (outParameters.size() > 0) {
                            kvPairBuilder.setDynamicParams();
                        }
                        preparedStatement.setString(0, kvPairBuilder.getKvPairString());
                        this.ha3ResultSet = (Ha3ResultSet)preparedStatement.executeQuery();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        PreparedStatement preparedStatement = this.ha3Connection.prepareStatement(originalSql);
                        Ha3KvPairBuilder kvPairBuilder = new Ha3KvPairBuilder();
                        kvPairBuilder.enableCache();
                        kvPairBuilder.setPrepareLevel("jni.post.optimize");
                        kvPairBuilder.setDatabaseName("general");
                        kvPairBuilder.enableUrlEncodeData();
                        preparedStatement.setString(0, kvPairBuilder.getKvPairString());
                        this.ha3ResultSet = (Ha3ResultSet)preparedStatement.executeQuery();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            //非动态参数的情况下，默认添加kvpairs
            if (sql.contains("kvpair")) {
                res.append(sql);
            } else {
                Ha3KvPairBuilder kvPairBuilder = new Ha3KvPairBuilder();
                kvPairBuilder.enableCache();
                kvPairBuilder.setPrepareLevel("jni.post.optimize");
                res.append(sql).append("&&kvpair=").append(kvPairBuilder.getKvPairString());
            }
            result = cloudClient.query(res.toString());
            this.ha3ResultSet = new Ha3ResultSet(result, this);
        }

    }

    /**
     * 返回Ha3 ResultSet
     *
     * @return
     */
    @Override
    public ResultSet getResultSet() {
        return this.ha3ResultSet;
    }

    @Override
    public int getUpdateCount() {
        return -1;
    }

    @Override
    public boolean getMoreResults() {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public int getFetchSize() {
        return 10000;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void addBatch(String sql) {

    }

    @Override
    public void clearBatch() {

    }

    @Override
    public int[] executeBatch() {
        return null;
    }

    /**
     * 返回 Ha3 Connection
     *
     * @return
     */
    @Override
    public Connection getConnection() {
        return this.ha3Connection;
    }

    @Override
    public boolean getMoreResults(int current) {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return this.executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return this.executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return this.executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return this.execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return this.execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return this.execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) {
    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public void closeOnCompletion() {
    }

    @Override
    public boolean isCloseOnCompletion() {
        return false;
    }
}
