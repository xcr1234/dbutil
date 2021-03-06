package com.owdp.dbutil.naming;

/**
 * 小写命名策略，表名就是类名的小写形式。
 */
public class LowerCaseTableNameStrategy implements TableNameStrategy{
    @Override
    public String format(String className) {
        return className.toLowerCase();
    }
}
