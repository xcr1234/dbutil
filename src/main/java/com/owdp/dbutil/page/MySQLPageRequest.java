package com.owdp.dbutil.page;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * MySQL数据库的分页查询实现
 */
public class MySQLPageRequest extends PageRequest {
    private static final long serialVersionUID = 5233572473214973949L;

    /**
     * 初始化分页查询
     * @param pageNumber 页码编号，从1开始
     * @param pageSize 单页数量
     */
    public MySQLPageRequest(int pageNumber, int pageSize) {
        super(pageNumber, pageSize);
    }

    @Override
    public String pageSelect(String sql) {
        return sql + " limit ?,?" ;
    }

    @Override
    public void fillState(PreparedStatement ps, int index) throws SQLException {
        ps.setInt(index,offset());
        ps.setInt(index+1,limit());
    }
}
