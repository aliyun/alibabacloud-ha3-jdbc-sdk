package com.aliyun.ha3engine.jdbc.sdk.client;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.aliyun.ha3engine.Client;
import com.aliyun.ha3engine.jdbc.common.config.Ha3Config;
import com.aliyun.ha3engine.jdbc.common.exception.ErrorCode;
import com.aliyun.ha3engine.jdbc.common.exception.Ha3DriverException;
import com.aliyun.ha3engine.jdbc.common.utils.FileTools;
import com.aliyun.ha3engine.jdbc.common.utils.Ha3ToolUtils;
import com.aliyun.ha3engine.jdbc.common.utils.JsonUtils;
import com.aliyun.ha3engine.models.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cloud client to connect to Cloud Ha3 engine
 *
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class CloudClient {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Ha3Config ha3Config;
    private Client client;

    /**
     * 构造通过endpoint访问的CloudClient
     *
     * @param ha3Config
     * @throws SQLException
     */
    public CloudClient(Ha3Config ha3Config) throws Exception {
        this.ha3Config = ha3Config;
        Config config = new Config();
        config.setAccessPassWord(this.ha3Config.getPassword());
        config.setEndpoint(this.ha3Config.getServiceName());
        config.setAccessUserName(this.ha3Config.getUsername());
        client = new Client(config);
    }

    /**
     * 通过aliyun-sdk-ha3engine发起请求，并拼装结果数据为JsonString
     *
     * @param sql
     * @return
     */
    public String query(String sql) {
        JsonObject resultJsonObject = new JsonObject();
        JsonObject errorJsonObject = new JsonObject();
        resultJsonObject.add("error", errorJsonObject);

        sql = Ha3ToolUtils.getFullJsonSql(sql);

        /** build query */
        SearchRequestModel sqlQueryRequestModel = new SearchRequestModel();
        SearchQuery sqlRawQuery = new SearchQuery();
        if (ha3Config.isEnableDetailLog()) {
            logger.info("sql:" + sql);
        }
        sqlRawQuery.setSql(sql);
        sqlQueryRequestModel.setQuery(sqlRawQuery);

        try {
            SearchResponseModel sqlResponseModel = new SearchResponseModel();
            if (ha3Config.isLocalMode()) {
                sqlResponseModel.setBody(FileTools.loadResource("ha3_sql_result"));
            } else {
                sqlResponseModel = client.Search(sqlQueryRequestModel);
            }
            JsonObject ha3SqlResult = JsonUtils.fromJson(sqlResponseModel.getBody(), JsonObject.class);
            JsonObject error_info = ha3SqlResult.getAsJsonObject("error_info");
            if (null != error_info) {
                errorJsonObject.addProperty("errorCode", error_info.get("ErrorCode").getAsInt());
                errorJsonObject.addProperty("message", error_info.get("Message").getAsString());
                errorJsonObject.addProperty("error", error_info.get("Error").getAsString());
                if (error_info.get("ErrorCode").getAsInt() != 0) {
                    logger.error(
                            "ERROR: query result has error：errorCode: " + error_info.get("ErrorCode").getAsInt() + " errorInfo: "
                                    + error_info.get("Error").getAsString());
                }
            } else {
                errorJsonObject.addProperty("errorCode", 404);
                errorJsonObject.addProperty("message", "ERROR: query result is empty,ha3Result is null!");
                errorJsonObject.addProperty("error", "ERROR: query result is empty,ha3Result is null!");
                logger.error(
                        "ERROR: query result is empty,ha3Result is null!");
            }

            JsonArray fieldsNames = ha3SqlResult.getAsJsonObject("sql_result").getAsJsonArray("column_name");
            JsonArray fieldsTypes = ha3SqlResult.getAsJsonObject("sql_result").getAsJsonArray("column_type");
            JsonArray columnsJsonArray = new JsonArray();
            for (int i = 0; i < fieldsNames.size(); i++) {
                String fieldName = fieldsNames.get(i).getAsString();
                String columnType = fieldsTypes.get(i).getAsString();
                JsonObject columnJsonObject = new JsonObject();
                columnJsonObject.addProperty("name", fieldName);
                columnJsonObject.addProperty("type", columnType);
                columnsJsonArray.add(columnJsonObject);
            }
            resultJsonObject.add("columns", columnsJsonArray);

            JsonArray data = ha3SqlResult.getAsJsonObject("sql_result").getAsJsonArray("data");

            JsonArray rowsArray = new JsonArray();
            for (int i = 0; i < data.size(); i++) {
                rowsArray.add(data.get(i));
            }
            resultJsonObject.add("rows", rowsArray);

        } catch (Exception e) {
            errorJsonObject.addProperty("errorCode", 404);
            errorJsonObject.addProperty("message", e.getMessage());
            errorJsonObject.addProperty("error", e.getMessage());
            logger.error("ERROR:" + e.getMessage());
        }
        return resultJsonObject.toString();
    }

    /**
     * 通过sql-client发起请求，将数据写入Ha3 Server，返回更新的行数
     *
     * @param body
     * @return
     */
    public void insertOrUpdate(String table, String primaryKey, List<Map<String, ?>> body) throws Ha3DriverException {
        if (ha3Config.isLocalMode()) {
            return;
        }
        try {
            PushDocumentsRequestModel request = new PushDocumentsRequestModel();
            request.setBody(body);
            client.pushDocuments(table, primaryKey, request);
        } catch (Exception e) {
            logger.error("Failed to insert data:" + e.getMessage(), e);
            throw new Ha3DriverException(ErrorCode.INSERT_FAIL, "call ha3 engine error, push fail. " + e.getMessage());
        }
    }

}
