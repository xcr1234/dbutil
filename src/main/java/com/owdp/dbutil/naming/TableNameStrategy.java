package com.owdp.dbutil.naming;

/**
 * 数据库表的命名策略，定义类名如何向表名转换
 */
public interface TableNameStrategy {
    String format(String className);
}
