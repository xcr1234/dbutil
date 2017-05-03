package com.owdp.dbutil.page;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * DB2数据库的分页查询实现
 */
public class DB2PageRequest extends PageRequest {
    private static final long serialVersionUID = 7755699613113012353L;

    /**
     * 初始化分页查询
     * @param pageNumber 页码编号，从1开始
     * @param pageSize 单页数量
     */
    public DB2PageRequest(int pageNumber, int pageSize) {
        super(pageNumber, pageSize);
    }

    @Override
    public String pageSelect(String sql) {
        if(offset() == 0){
            return sql + " fetch first " + limit() + " rows only";
        }
        return "select * from ( select inner2_.*, rownumber() over(order by order of inner2_) as rownumber_ from ( "
                + sql + " fetch first " + limit() + " rows only ) as inner2_ ) as inner1_ where rownumber_ > "
                + offset() + " order by rownumber_";
    }

    @Override
    public void fillState(PreparedStatement ps, int index) throws SQLException {

    }
}
