package com.owdp.dbutil;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * <pre>
 * DbUtil的事务管理器，主要方法：
 *
 * {@link #begin()}
 * {@link #commit()}
 *
 * 一般的，回滚（rollback）操作会在发生{@link SQLException}时自动触发，无需手动执行。
 * </pre>
 */
public final class Transactional {


    private static ThreadLocal<Boolean> local = new ThreadLocal<Boolean>();

    /**
     * <pre>
     * 开始一个新的事务。
     * 如果一个事务已开始，在执行sql查询时，如果发生{@link SQLException}异常，会自动回滚（rollback），无需手动回滚。
     * </pre>
     */
    public static void begin(){
        try {
            Connection connection = DbUtils.getConnection();
            connection.setAutoCommit(false);
            local.set(true);
        }catch (SQLException e){
            DbUtils.getExceptionCaught().sqlError(e);
            throw new QueryException(e);
        }
    }

    /**
     * 事务是否已经开始
     * @return 事务是否已经开始
     */
    public static boolean hasBegin(){
        return local.get() == null ? false : local.get();
    }

    /**
     * 回滚（rollback）一个已开始的事务。
     */
    public static void rollback(){
        try {
            Connection connection = DbUtils.getConnection();
            connection.rollback();
        }catch (SQLException e){
            DbUtils.getExceptionCaught().sqlError(e);
            throw new QueryException(e);
        }
    }

    /**
     * 提交一个一开始的事务
     */
    public static void commit(){
        Connection connection = null;
        try {
            connection = DbUtils.getConnection();
            if(!connection.getAutoCommit()){
                connection.commit();
                connection.setAutoCommit(true);
            }
        }catch (SQLException e){
            DbUtils.getExceptionCaught().sqlError(e);
            throw new QueryException(e);
        }finally {
            local.set(false);
            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {}
            }
        }
    }


}
