package com.owdp.dbutil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;


public class DataSourceConnectionSupplier implements ConnectionSupplier {
    private DataSource dataSource;
    private String user;
    private String pwd;

    public DataSourceConnectionSupplier(DataSource dataSource, String user, String pwd) {
        this.dataSource = dataSource;
        this.user = user;
        this.pwd = pwd;
    }

    @Override
    public Connection get() throws SQLException {
        if(user == null && pwd == null){
            return dataSource.getConnection();
        }
        return dataSource.getConnection(user,pwd);
    }
}
