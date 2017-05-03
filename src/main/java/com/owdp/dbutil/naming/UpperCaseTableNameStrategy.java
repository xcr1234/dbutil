package com.owdp.dbutil.naming;


public class UpperCaseTableNameStrategy implements TableNameStrategy{
    @Override
    public String format(String className) {
        return className.toUpperCase();
    }
}
