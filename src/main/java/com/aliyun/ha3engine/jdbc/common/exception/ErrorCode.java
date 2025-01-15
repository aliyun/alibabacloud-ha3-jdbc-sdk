package com.aliyun.ha3engine.jdbc.common.exception;

/**
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public enum ErrorCode {
    ERROR_INPUT_SCHEMA_NULL(ErrorType.ERROR_BUILD_REQUEST, 3, "schema null when build request"),
    FAIL_TO_CONVERT_COLUMN_TYPE(ErrorType.ERROR_BUILD_RESULT_SET, 1, "failed to convert column type"),
    EMPTY_PARAM(ErrorType.EMPTY_PARAM, 4, "empty param"),
    INVALID_PARAM(ErrorType.INVALID_PARAM, 5, "empty param"),
    INSERT_FAIL(ErrorType.INSERT_FAIL, 6, "insert fail"),
    CONNECTION_SIZE_EXCEEDED_LIMIT(ErrorType.CONNECTION_SIZE_EXCEEDED_LIMIT, 2,
        "The connection pool exceeded the limit");


    private ErrorType errorType;
    private int errorCode;
    private int code;
    private String description;

    ErrorCode(ErrorType type, int code, String description) {
        this.errorType = type;
        this.errorCode = code;
        this.code = type.getTypeCode() * 1000 + errorCode;
        this.description = description;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "ErrorCode[" +
            "code:" + code + "," +
            "description:" + description +
            "]";
    }
}
