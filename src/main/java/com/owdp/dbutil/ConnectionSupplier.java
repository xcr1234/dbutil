package com.owdp.dbutil;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 定义如何得到Connection对象的
 */
public interface ConnectionSupplier {
    Connection get() throws SQLException;
}
