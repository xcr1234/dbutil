package com.owdp.dbutil.naming;

/**
 * 默认的表命名策略，表名就是类名
 */
public class DefaultTableNameStrategy implements TableNameStrategy {
    @Override
    public String format(String className) {
        return className;
    }
}
