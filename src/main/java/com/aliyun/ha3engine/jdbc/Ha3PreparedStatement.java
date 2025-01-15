package com.aliyun.ha3engine.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import com.aliyun.ha3engine.jdbc.common.config.Ha3KvPairBuilder;
import com.aliyun.ha3engine.jdbc.common.exception.ErrorCode;
import com.aliyun.ha3engine.jdbc.common.exception.Ha3DriverException;
import com.aliyun.ha3engine.jdbc.common.utils.JsonUtils;
import com.aliyun.ha3engine.jdbc.sdk.client.CloudClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ha3 JDBC preparedStatement
 *
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class Ha3PreparedStatement extends Ha3Statement implements PreparedStatement {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private String preparedSql;
    private final char firstStmtChar;
    private final Object[] sqlAndParams;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ROOT);
    private static String ENCODING = "UTF-8";

    public Ha3PreparedStatement(Ha3Connection ha3Connection, CloudClient cloudClient, String sql)
        throws SQLException {
        super(ha3Connection, cloudClient);
        preparedSql = sql.trim();
        preparedSql = preparedSql.replaceAll("\r", " ").replaceAll("\n", " ");
        String sqlNorm = preparedSql.trim().toLowerCase();
        if (sqlNorm.startsWith("query=")) {
            sqlNorm = sqlNorm.replaceFirst("query=", "");
        }
        int startOfStmtPos = findStartOfStatement(sqlNorm);
        firstStmtChar = Character.toUpperCase(sqlNorm.charAt(startOfStmtPos));
        String fromIndex = null;
        String[] sqlParts = preparedSql.trim().split(" ");
        for (int i = 0; i < sqlParts.length; i++) {
            if (firstStmtChar == 'S') {
                if ("from".equals(sqlParts[i].toLowerCase())) {
                    fromIndex = sqlParts[i + 1];
                    ha3Connection.setSchema(fromIndex);
                    break;
                }
            } else if (firstStmtChar == 'I' || firstStmtChar == 'D') {
                break;
            } else {
                throw new SQLException(
                    "Provided query type '" + firstStmtChar + "' is not supported!");
            }

        }

        String[] parts = (sql + ";").split("\\?");
        this.sqlAndParams = new Object[parts.length];
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (firstStmtChar == 'S') {
            this.execute(preparedSql);
            return ha3ResultSet;
        } else if (firstStmtChar == 'I' || firstStmtChar == 'D') {
            throw new SQLException("Please use executeUpdate!");
        }  else {
            throw new SQLException(
                "Provided query type '" + preparedSql.substring(0, preparedSql.indexOf(' ')) + "' is not supported!");
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (firstStmtChar == 'I') {
            return executeHa3Update(preparedSql);
        } else if (firstStmtChar == 'D') {
            return executeHa3Delete(preparedSql);
        } else {
            throw new SQLException(
                    "Provided query type '" + preparedSql.substring(0, preparedSql.indexOf(' ')) + "' is not supported!");
        }
    }

    /**
     * 发起远端调用，请求Ha3 Server写入数据
     *
     * @param sql
     * @throws SQLException
     */
    protected int executeHa3Update(String sql) throws Ha3DriverException {
        List<Map<String, ?>> body = getBodyFromInsertSql(sql);
        String tableName = getTableNameFromSql(sql);
        String pk = getPk(sql);
        cloudClient.insertOrUpdate(tableName, pk, body);
        return body.size();
    }

    /**
     * 发起远端调用，请求Ha3 Server删除数据
     *
     * @param sql
     * @throws SQLException
     */
    protected int executeHa3Delete(String sql) throws Ha3DriverException {
        List<Map<String, ?>> body = getBodyFromDeleteSql(sql);
        String tableName = getTableNameFromSql(sql);
        String pk = getPkFromSql(sql);
        cloudClient.insertOrUpdate(tableName, pk, body);
        return body.size();
    }


    /**
     * 获取pk信息，优先从param里获取，如果param里没有，默认取insert sql语句的第一个字段
     * @return
     */
    private String getPk(String sql) throws Ha3DriverException {
        // 优先从param里取pk字段
        if (sqlAndParams.length > 0) {
            Map<String, String> param = (Map<String, String>) sqlAndParams[0];
            if (null != param && param.containsKey("pk")) {
                return param.get("pk");
            }
        }
        return getPkFromSql(sql);
    }

    /**
     * 从insert sql语句中解析推送请求的body
     * @param sql
     * @return
     */
    private java.util.List<java.util.Map<String, ?>> getBodyFromInsertSql(String sql) throws Ha3DriverException {
        List<Map<String, ?>> body = new ArrayList<>();

        int paramIndex = 1;

        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);

        // 获取列名
        Collection<TableStat.Column> columns = getColumnsFromSql(sql);
        List<String> columnNames = columns.stream().map(e -> e.getName()).collect(Collectors.toList());

        // 解析value记录
        List<SQLInsertStatement.ValuesClause> valuesList = ((MySqlInsertStatement) statement).getValuesList();

        for (SQLInsertStatement.ValuesClause valuesClause : valuesList) {
            java.util.Map<String, Object> record = new HashMap<>();
            record.put("cmd", "add");

            List<SQLExpr> exprList = valuesClause.getValues();
            if (exprList.size() != columnNames.size()) {
                throw new Ha3DriverException(ErrorCode.INVALID_PARAM, "values size not match column size!");
            }
            Map<String, Object> fields= new HashMap<>();
            Object value;
            for (int i=0; i<exprList.size(); i++) {
                SQLExpr expr = exprList.get(i);
                if (expr instanceof SQLNullExpr) {
                    value = null;
                }
                else if (expr instanceof SQLValuableExpr) {
                    value = ((SQLValuableExpr) expr).getValue();
                } else if (expr instanceof SQLVariantRefExpr) {
                    // 从param里取值
                    value = sqlAndParams[paramIndex++];
                } else {
                    throw new Ha3DriverException(ErrorCode.INVALID_PARAM, "unsupported expr type: " + expr.getClass().getName());
                }
                fields.put(columnNames.get(i), value);
            }
            record.put("fields", fields);
            body.add(record);
        }
        return body;
    }


    /**
     * 从delete sql语句中解析推送请求的body
     * @param sql
     * @return
     */
    private List<Map<String, ?>> getBodyFromDeleteSql(String sql) throws Ha3DriverException {
        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);
        int paramIndex = 1;

        // 获取列名
        Collection<TableStat.Column> columns = getColumnsFromSql(sql);
        if (columns.size() != 1) {
            throw new Ha3DriverException(ErrorCode.INVALID_PARAM, "column error, only support delete data by primary key.");
        }
        List<String> columnNames = columns.stream().map(e -> e.getName()).collect(Collectors.toList());
        String columnName = columnNames.get(0);

        List<Map<String, ?>> body = new ArrayList<>();

        // 解析value记录
        SQLExpr where = ((MySqlDeleteStatement) statement).getWhere();

        // String sql = " delete from  user where  id  = 1";
        if (where instanceof SQLBinaryOpExpr) {
            if (((SQLBinaryOpExpr) where).getOperator() != SQLBinaryOperator.Equality) {
                throw new Ha3DriverException(ErrorCode.INVALID_PARAM, "unsupported where type: " + ((SQLBinaryOpExpr) where).getOperator());
            }
            SQLExpr left = ((SQLBinaryOpExpr) where).getLeft();
            if (!(left instanceof SQLIdentifierExpr)) {
                throw new Ha3DriverException(ErrorCode.INVALID_PARAM, "column should in front of value.");
            }
            SQLExpr right = ((SQLBinaryOpExpr) where).getRight();

            java.util.Map<String, Object> record = new HashMap<>();
            record.put("cmd", "delete");
            Map<String, Object> fields= new HashMap<>();
            Object value;

            if (right instanceof SQLValuableExpr) {
                value = ((SQLValuableExpr) right).getValue();
            } else if (right instanceof SQLVariantRefExpr) {
                value = sqlAndParams[paramIndex++];
            } else {
                throw new IllegalArgumentException("not support type");
            }
            fields.put(columnName, value);
            record.put("fields", fields);
            body.add(record);
        } else if (where instanceof SQLInListExpr) {
            // String sql = " delete from  user where id  in (1,2,3,?,5)";
            if (((SQLInListExpr) where).isNot()) {
                throw new IllegalArgumentException("only support in, not support not in");
            }
            List<SQLExpr> exprs = ((SQLInListExpr) where).getTargetList();
            for (SQLExpr expr : exprs) {
                java.util.Map<String, Object> record = new HashMap<>();
                record.put("cmd", "delete");
                Map<String, Object> fields= new HashMap<>();
                Object value;

                if (expr instanceof SQLValuableExpr) {
                    value = ((SQLValuableExpr) expr).getValue();
                } else if (expr instanceof SQLVariantRefExpr) {
                    value = sqlAndParams[paramIndex++];
                } else {
                    throw new IllegalArgumentException("unsupported expr type: " + expr.getClass().getName());
                }
                fields.put(columnName, value);
                record.put("fields", fields);
                body.add(record);
            }
        } else {
            throw new IllegalArgumentException("unsupported where type");
        }
        return body;
    }

    private StringBuilder getSql(String sql) throws Ha3DriverException {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(sql);

        if (null != sqlAndParams) {
            String kvpairs = String.valueOf(sqlAndParams[0]).replaceAll("\'", "");
            if ("null".equals(kvpairs)) {
                kvpairs = "";
            }
            List<Object> values = new ArrayList<>();
            for (int i = 1; i < sqlAndParams.length; i++) {
                values.add(sqlAndParams[i]);
            }

            if (values.size() > 0) {
                // 替换
                if (kvpairs.contains("dynamic_params:dynamic_params")) {

                    String dynamicParams = this.replaceJsonDynamicParams(values);
                    dynamicParams = JsonUtils.string2Unicode(dynamicParams);
                    if (kvpairs.contains("urlencode_data")) {
                        try {
                            kvpairs = new StringBuilder(
                                kvpairs.replace("dynamic_params:dynamic_params",
                                    new StringBuilder().append("dynamic_params:").append(URLEncoder.encode(dynamicParams,ENCODING))))
                                .toString();
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    } else {
                        kvpairs = new StringBuilder(
                            kvpairs.replace("dynamic_params:dynamic_params",
                                new StringBuilder().append("dynamic_params:").append(dynamicParams)))
                            .toString();
                    }

                } else {
                    StringBuilder kvPairs = new StringBuilder(kvpairs);
                    if ("" == kvpairs) {
                        kvpairs = kvPairs.append("dynamic_params:").append(this.replaceDynamicParams(values)).toString();
                    } else {
                        kvpairs = kvPairs.append(";").append("dynamic_params:").append(this.replaceDynamicParams(values)).toString();
                    }

                }
            }
            if (!sql.contains("&&kvpair=")) {
                sqlBuilder.append("&&kvpair=").append(kvpairs);
            } else {
                if (sql.endsWith(";")) {
                    sqlBuilder.append(kvpairs);
                } else {
                    sqlBuilder.append(";").append(kvpairs);
                }
            }

        }
        return sqlBuilder;
    }

    private String replaceJsonDynamicParams(List<Object> values) throws Ha3DriverException {
        StringBuilder dynamicParams = new StringBuilder();
        dynamicParams.append("[");
        dynamicParams.append(JsonUtils.toJson(values));
        dynamicParams.append("]");
        return dynamicParams.toString();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) {
        this.sqlAndParams[parameterIndex] = null;
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        this.sqlAndParams[parameterIndex] = new Boolean(x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) {
        this.sqlAndParams[parameterIndex] = new Byte(x);
    }

    @Override
    public void setShort(int parameterIndex, short x) {
        this.sqlAndParams[parameterIndex] = new Short(x);
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        this.sqlAndParams[parameterIndex] = new Integer(x);
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        this.sqlAndParams[parameterIndex] = new Long(x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        this.sqlAndParams[parameterIndex] = new Float(x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        this.sqlAndParams[parameterIndex] = new Double(x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) {
        this.sqlAndParams[parameterIndex] = x;
    }

    @Override
    public void setString(int parameterIndex, String x) {
        this.sqlAndParams[parameterIndex] = x;
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) {
        this.sqlAndParams[parameterIndex] = x;
    }

    @Override
    public void setDate(int parameterIndex, Date x) {
        this.sqlAndParams[parameterIndex] = x;
    }

    @Override
    public void setTime(int parameterIndex, Time x) {
        this.sqlAndParams[parameterIndex] = new Long(x.getTime());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) {
        this.sqlAndParams[parameterIndex] = new Long(x.getTime());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("AsciiStream not supported");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("UnicodeStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("BinaryStream not supported");
    }

    @Override
    public void clearParameters() {
        for (int i = 1; i < sqlAndParams.length; i += 2) {
            sqlAndParams[i] = null;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) {
        this.sqlAndParams[parameterIndex] = x;
    }

    @Override
    public void setObject(int parameterIndex, Object x) {
        this.sqlAndParams[parameterIndex] = x;
    }

    @Override
    public boolean execute() throws SQLException {
        if (firstStmtChar == 'S') {
            return this.execute(preparedSql);
        } else if (firstStmtChar == 'I' || firstStmtChar == 'U') {
            throw new SQLException("Please use executeUpdate!");
        } else {
            throw new SQLException(
                "Provided query type '" + preparedSql.substring(0, preparedSql.indexOf(' ')) + "' is not supported!");
        }
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("CharacterStream not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setArray(int parameterIndex, Array x) {
        this.sqlAndParams[parameterIndex] = x;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return ha3ResultSet.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) {
        this.sqlAndParams[parameterIndex] = null;
    }

    @Override
    public void setURL(int parameterIndex, URL x) {
        this.sqlAndParams[parameterIndex] = x;
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return new Ha3ParameterMetaData(sqlAndParams);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setNString(int parameterIndex, String value) {
        this.sqlAndParams[parameterIndex] = value;
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("[ERROR]");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("Forbidden method on PreparedStatement");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Forbidden method on PreparedStatement");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        char firstStmtCharFromSql = getFirstStmtCharFromSql(sql);
        if (firstStmtCharFromSql == 'S') {
            initResultSet(sql);
            return true;
        } else if (firstStmtCharFromSql == 'I' || firstStmtCharFromSql == 'U') {
            throw new SQLException("Please use executeUpdate!");
        } else {
            throw new SQLException(
                    "Provided query type '" + firstStmtCharFromSql + "' is not supported!");
        }
    }

    /**
     * 发起远端调用，请求Ha3 Server获取数据
     *
     * @param sql
     * @throws SQLException
     */
    protected void initResultSet(String sql) throws Ha3DriverException {
        if (this.ha3Connection.getHa3Config().isEnableDetailLog()) {
            logger.info("Ha3PreparedStatement sql:" + sql);
        }
        StringBuilder sqlBuilder = getSql(sql);
        String result;
        if (this.ha3Connection.getHa3Config().isEnableDynamicParams()) {
            String finalSql = sqlBuilder.toString();
            int finalSqlLength = finalSql.length();
            int kvpairLength = finalSql.lastIndexOf("&&kvpair=");
            String kvpairSize = finalSql.substring(kvpairLength, finalSqlLength);
            //开启sql动态参数化
            if (kvpairSize.length() > 9) {
                //sql带了kvpair，默认用户自己拼了动态参数等
                result = cloudClient.query(finalSql);
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
                        kvPairBuilder.setFormatType("flatbuffers");
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
                        kvPairBuilder.setFormatType("flatbuffers");
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
            result = cloudClient.query(sqlBuilder.toString());
            this.ha3ResultSet = new Ha3ResultSet(result, this);
        }
    }

    // 生成动态参数二维列表
    private StringBuilder replaceDynamicParams(List<Object> values) throws Ha3DriverException {
        StringBuilder dynamicParams = new StringBuilder();
        return dynamicParams.append("[[")
            .append(join(values, ", "))
            .append("]]");
    }

    // 生成动态参数列表
    private String join(List<Object> values, String splitter) throws Ha3DriverException {
        StringBuilder res = new StringBuilder();
        int cur = 0;
        for (Object value : values) {
            if (null == value) {
                throw new Ha3DriverException(ErrorCode.EMPTY_PARAM, "empty params!");
            }
            if (value.getClass() == String.class) {
                value = "\"" + value + "\"";
            }

            if (value.getClass() == Date.class) {
                value = "\'" + dateFormat.format((Date)value) + "\'";
            }

            if (values.size() - 1 == cur) {
                res.append(value);
            } else {
                res.append(value).append(splitter);
            }
            cur++;
        }
        return res.toString();
    }
}
