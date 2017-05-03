package com.owdp.dbutil.page;

import java.sql.PreparedStatement;
import java.sql.SQLException;
/**
 * PostgreSQL数据库的分页查询实现
 */
public class PostgreSQLPageRequest extends PageRequest {
    private static final long serialVersionUID = -7671941773517964917L;

    /**
     * 初始化分页查询
     * @param pageNumber 页码编号，从1开始
     * @param pageSize 单页数量
     */
    public PostgreSQLPageRequest(int pageNumber, int pageSize) {
        super(pageNumber, pageSize);
    }

    @Override
    public String pageSelect(String sql) {
        return sql + " limit ? offset ?";
    }

    @Override
    public void fillState(PreparedStatement ps, int index) throws SQLException {
        ps.setInt(index,limit());
        ps.setInt(index+1,offset());
    }
}
