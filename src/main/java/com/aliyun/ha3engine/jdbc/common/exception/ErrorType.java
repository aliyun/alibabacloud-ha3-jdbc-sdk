package com.aliyun.ha3engine.jdbc.common.exception;

/**
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public enum ErrorType {
    ERROR_BUILD_REQUEST(3),
    ERROR_BUILD_RESULT_SET(1),
    EMPTY_PARAM(4),
    INVALID_PARAM(5),
    INSERT_FAIL(6),
    CONNECTION_SIZE_EXCEEDED_LIMIT(2);

    private int typeCode;

    ErrorType(int typeCode) {
        this.typeCode = typeCode;
    }

    public int getTypeCode() {
        return typeCode;
    }
}
