package com.owdp.dbutil;

/**
 * 在数据库查询，加载connection等的过程中遇到的异常。
 */
public class QueryException extends RuntimeException {
    private static final long serialVersionUID = 5629090854832009911L;

    public QueryException() {
    }

    public QueryException(String message) {
        super(message);
    }

    public QueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryException(Throwable cause) {
        super(cause);
    }
}
