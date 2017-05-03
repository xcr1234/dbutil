package com.owdp.dbutil;


import java.lang.reflect.Method;
import java.sql.SQLException;

public interface ExceptionCaught {

    void sqlError(SQLException e);

}
