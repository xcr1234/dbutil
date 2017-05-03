package com.owdp.dbutil.page;


import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * <pre>
 * 分页查询的接口
 *
 * 由于每种数据库分页查询语句不同，我们设计了 Pageable 接口， 同时对不同数据库有不同
 * 的实现。目前实现了 DB2、 Derby、 H2、 MySQL、 Oracle、 PostgreSQL、 SQLServer(2005 以上)
 * 这 7 种数据库的分页查询。
 * 例子：
 * MySQL 查询第 1 页的数据，每页 10 条
 * Pageable page = new MySQLPageRequest(1,10);
 * Oracle 查询第 2 页的数据，每页 20 条
 * Pageable page = new OraclePageRequest (2,20);
 * </pre>
 *
 */
public interface Pageable {
    int limit();
    int offset();
    int from();
    int to();

    /**
     * 封装分页查询sql语句
     * @param sql 原始sql语句
     * @return 封装后的sql语句
     */
    String pageSelect(String sql);

    /**
     * 填充分页查询PreparedStatement
     * @param ps 待填充的PreparedStatement
     * @param index 起始编号
     * @throws SQLException 接收填充过程中抛出的SQLException，一并处理
     */
    void fillState(PreparedStatement ps, int index) throws SQLException;
}
