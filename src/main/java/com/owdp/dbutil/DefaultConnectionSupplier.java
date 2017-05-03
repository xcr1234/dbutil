package com.owdp.dbutil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 默认的，从DriverManager中得到Connection的ConnectionSupplier
 */
public class DefaultConnectionSupplier implements ConnectionSupplier{

    private String url;
    private String user;
    private String pwd;

    public DefaultConnectionSupplier(String driver, String url, String user, String pwd) {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new QueryException("jdbc driver not found :"+driver);
        }
        this.url = url;
        this.user = user;
        this.pwd = pwd;
    }

    @Override
    public Connection get() throws SQLException{
        if(user == null && pwd == null){
            return DriverManager.getConnection(url);
        }
        return DriverManager.getConnection(url,user,pwd);
    }
}
