package com.owdp.dbutil.page;


import java.sql.PreparedStatement;
import java.sql.SQLException;
/**
 * H2数据库的分页查询实现
 */
public class H2PageRequest extends PageRequest{
    private static final long serialVersionUID = -2529784193676797988L;

    /**
     * 初始化分页查询
     * @param pageNumber 页码编号，从1开始
     * @param pageSize 单页数量
     */
    public H2PageRequest(int pageNumber, int pageSize) {
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
