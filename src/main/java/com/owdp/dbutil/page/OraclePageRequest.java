package com.owdp.dbutil.page;

import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * Oracle数据库的分页查询实现
 */
public class OraclePageRequest extends PageRequest {
    private static final long serialVersionUID = -7506834556355123865L;

    /**
     * 初始化分页查询
     * @param pageNumber 页码编号，从1开始
     * @param pageSize 单页数量
     */
    public OraclePageRequest(int pageNumber, int pageSize) {
        super(pageNumber, pageSize);
    }

    @Override
    public String pageSelect(String sql) {
        StringBuilder pagingSelect = new StringBuilder( sql.length()+100 );
        if(offset() > 0){
            pagingSelect.append("select * from ( select row_.*, rownum rownum_ from ( ");
        }else{
            pagingSelect.append("select * from ( ");
        }
        pagingSelect.append(sql);
        if(offset() > 0){
            pagingSelect.append(" ) row_ where rownum <= ?) where rownum_ > ?");
        }else{
            pagingSelect.append(" ) where rownum <= ?");
        }
        return pagingSelect.toString();
    }

    @Override
    public void fillState(PreparedStatement ps, int index) throws SQLException {
        if(offset() > 0){
            ps.setInt(index,to());
            ps.setInt(index+1,from());
        }else{
            ps.setInt(index,limit());
        }
    }
}
