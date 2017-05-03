package com.owdp.dbutil;


import com.owdp.dbutil.annotation.Dao;
import com.owdp.dbutil.cache.CacheManager;
import com.owdp.dbutil.impl.DaoInterceptor;
import com.owdp.dbutil.impl.MethodFilter;
import com.owdp.dbutil.impl.ServiceInterceptor;
import com.owdp.dbutil.impl.DbDaoImpl;
import com.owdp.dbutil.meta.Meta;
import com.owdp.dbutil.naming.DefaultTableNameStrategy;
import com.owdp.dbutil.naming.TableNameStrategy;
import com.owdp.dbutil.util.BeanUtils;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.NoOp;

import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 设置DbUtils如何获取connection，以及得到DbDao的实现。
 */
public final class DbUtils {

    private static SQLLogger SQLLogger = new SQLLogger() {
        @Override
        public void logSQL(String sql) {
            System.out.println(sql);
        }
    };
    private static ConnectionSupplier connectionSupplier;
    private static TableNameStrategy tableNameStrategy = new DefaultTableNameStrategy();
    private static CacheManager cacheManager ;
    private static ExceptionCaught exceptionCaught = new ExceptionCaught() {
        @Override
        public void sqlError(SQLException e) {

        }
    };

    public static synchronized ExceptionCaught getExceptionCaught() {
        return exceptionCaught;
    }

    public static synchronized void setExceptionCaught(ExceptionCaught exceptionCaught) {
        DbUtils.exceptionCaught = exceptionCaught;
    }

    private static int maxRecursion = 3;

    private static boolean showsql = false;

    public static synchronized SQLLogger getSQLLogger() {
        return SQLLogger;
    }

    public static synchronized void setSQLLogger(SQLLogger SQLLogger) {
        if(SQLLogger == null){
            throw new IllegalArgumentException("the sql show can't be null.");
        }
        DbUtils.SQLLogger = SQLLogger;
    }

    public static synchronized CacheManager getCacheManager() {
        return cacheManager;
    }

    /**
     * 设置jdbc cache管理器
     * @param cacheManager cache管理器，不可为空
     */
    public static synchronized void setCacheManager(CacheManager cacheManager) {
        DbUtils.cacheManager = cacheManager;
    }

    public static synchronized TableNameStrategy getTableNameStrategy() {
        return tableNameStrategy;
    }

    /**
     * 设置jdbc 表的命名策略
     * @param tableNameStrategy 表的命名策略，不可为空
     */
    public static synchronized void setTableNameStrategy(TableNameStrategy tableNameStrategy) {
        if(tableNameStrategy == null){
            throw new IllegalArgumentException("table name strategy can't be null");
        }
        DbUtils.tableNameStrategy = tableNameStrategy;
    }

    public static synchronized boolean isShowsql() {
        return showsql;
    }

    /**
     * 是否打印输出sql语句
     * @param showsql 是否打印输出sql语句，默认为false.
     */
    public static synchronized void setShowsql(boolean showsql) {
        DbUtils.showsql = showsql;
    }

    public static synchronized int getMaxRecursion() {
        return maxRecursion;
    }

    public static synchronized void setMaxRecursion(int maxRecursion) {
        DbUtils.maxRecursion = maxRecursion;
    }

    public static synchronized ConnectionSupplier getConnectionSupplier() {
        return connectionSupplier;
    }

    public static synchronized void setConnectionSupplier(ConnectionSupplier cs) {
        connectionSupplier = cs;
    }

    /**
     * 设置全局jdbc配置，如何得到Connection
     * @param driver jdbc驱动
     * @param url jdbc url
     * @param user jdbc用户名
     * @param pwd jdbc密码
     */
    public static synchronized void setConnection(String driver,String url,String user,String pwd){
        if(driver == null || url == null){
            throw new IllegalArgumentException("null driver or url!");
        }
        connectionSupplier = new DefaultConnectionSupplier(driver, url, user, pwd);
    }

    /**
     * 设置jdbc数据源，在执行jdbc代码时，从数据源中得到Connection
     * @param dataSource jdbc数据源
     */
    public static void setDataSource(DataSource dataSource){
        setDataSource(dataSource,null,null);
    }

    public static synchronized void setDataSource(DataSource dataSource,String user,String pwd){
        if(dataSource == null){
            throw new IllegalArgumentException("null data source!");
        }
        connectionSupplier = new DataSourceConnectionSupplier(dataSource,user,pwd);
    }

    private static ThreadLocal<Connection> local = new ThreadLocal<Connection>();

    /**
     * 得到jdbc Connection对象，该Connection是线程安全的。
     * @return Connection对象
     */
    public static Connection getConnection(){
        ConnectionSupplier connectionSupplier = getConnectionSupplier();
        if(connectionSupplier == null){
            throw new QueryException("Connection has not been configured." );
        }
        Connection connection = local.get();
        try {
            if(connection == null || connection.isClosed()){
                connection = connectionSupplier.get();
            }
            local.set(connection);
        }catch (SQLException e) {
            DbUtils.getExceptionCaught().sqlError(e);
            throw new QueryException("cannot get connection!",e);
        }
        return connection;
    }

    private static Map<Class,Object> daoCache = Collections.synchronizedMap(new HashMap<Class,Object>());
    private static Map<Class,Object> serviceCache = Collections.synchronizedMap(new HashMap<Class,Object>());

    /**
     * 获得jdbc Dao的实现类
     * @param daoClass dao的Class类型
     * @param <Dao> dao的数据类型
     * @param <T> dao对应的实体数据类型
     * @param <ID> dao对应的实体的ID的数据类型
     * @return Dao的实现类
     * @throws BeanException 当dao/bean实体不符合java bean规范时，抛出该异常
     */
    @SuppressWarnings("unchecked")
    public static <Dao extends DbDao<T,ID>,T,ID extends Serializable> Dao getDaoImpl(Class<Dao> daoClass){
        if(daoClass == null){
            throw new IllegalArgumentException("dao class can't be null!");
        }
        if(!DbDao.class.isAssignableFrom(daoClass)){
            throw new BeanException("illegal dao:"+daoClass+",dao interface class should extends DbDao<T,ID>");
        }
        Type[] types = daoClass.getGenericInterfaces();
        for(Type t:types){
            if(t instanceof ParameterizedType){
                ParameterizedType pt = (ParameterizedType) t;
                Type[] arguments = pt.getActualTypeArguments();
                if(arguments.length == 2){
                    Type t0 = arguments[0] , t1 = arguments[1];
                    if(t0 instanceof Class && t1 instanceof Class && BeanUtils.isSerializable((Class<?>) t1)){
                        Object dao = daoCache.get(daoClass);
                        if(dao == null){
                            try {
                                dao = getDaoImpl(daoClass,(Class<T>)t0,(Class<ID>)t1);
                                daoCache.put(daoClass,dao);
                            }catch (ClassCastException e){
                                throw new BeanException("illegal dao:"+daoClass);
                            }
                        }
                        return (Dao) dao;
                    }
                }
            }
        }
        throw new BeanException("illegal dao:"+daoClass+",dao interface class should extends DbDao<T,ID>");
    }

    /**
     * 获得jdbc service的实现类，该service支持注解式cache的实现
     * @param serviceClass service的Class类型
     * @param <Service> service的数据类型
     * @return service的实现类
     * @throws BeanException 当service/bean实体不符合java bean规范时，抛出该异常
     */
    @SuppressWarnings("unchecked")
    public static <Service> Service getServiceImpl(Class<Service> serviceClass){
        if(serviceClass == null){
            throw new IllegalArgumentException("service class can't be null!");
        }
        if(Modifier.isFinal(serviceClass.getModifiers()) || Modifier.isAbstract(serviceClass.getModifiers())){
            throw new BeanException("illegal service:" + serviceClass + ",class is final or abstract.");
        }
        Object value = serviceCache.get(serviceClass);
        if(value == null){
            Enhancer enhancer = new Enhancer();
            enhancer.setSuperclass(serviceClass);
            enhancer.setCallbackFilter(MethodFilter.getInstance());
            enhancer.setCallbacks(new Callback[]{new ServiceInterceptor(serviceClass), NoOp.INSTANCE});
            value = enhancer.create();
            serviceCache.put(serviceClass,value);
            Field[] fields = serviceClass.getDeclaredFields();
            for(Field field:fields){
                if(field.isAnnotationPresent(Dao.class)){
                    Class daoClass = field.getType();
                    if(!DbDao.class.isAssignableFrom(daoClass)){
                        throw new BeanException("illegal service:"+serviceClass+",illegal dao:"+daoClass+",dao interface class should extends DbDao<T,ID>");
                    }
                    boolean access = field.isAccessible();
                    try {
                        DbDao dao = DbUtils.getDaoImpl(daoClass);
                        if(!access){
                            field.setAccessible(true);
                        }
                        field.set(value,dao);
                    } catch (Exception e) {
                        throw new BeanException("can't create service :" + serviceClass + ",dao:"+daoClass,e);
                    }finally {
                        if(!access){
                            field.setAccessible(false);
                        }
                    }
                }
            }
        }
        return (Service) value;
    }

    /**
     * 根据java bean的类型，得到java bean的实现dao，该方式不支持动态查询。此时Id类型退化为{@link Serializable}
     * @param type java bean的Class类型
     * @param <T> java bean的泛型
     * @return java bean的实现dao
     * @deprecated 建议定义一个Dao接口，继承DbDao接口，然后用{@link #getDaoImpl(Class)}方法获取。因为该方法不支持动态查询。
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public static <T> DbDao<T,Serializable> getDaoImplByType(Class<T> type){
        if(type == null){
            throw new IllegalArgumentException("dao class can't be null!");
        }
        return DbDaoImpl.create(type);
    }




    @SuppressWarnings("unchecked")
    private static <Dao extends DbDao<T,ID>,T,ID extends Serializable> Dao getDaoImpl(Class<Dao> daoClass, Class<T> tClass, Class<ID> idClass){
        if(Modifier.isFinal(tClass.getModifiers()) || Modifier.isAbstract(tClass.getModifiers())){
            throw new BeanException("entity class can't be final or abstract :"+tClass);
        }
        Meta meta = Meta.parse(tClass);
        if(meta.getIdColumn().getType() != idClass){
            throw new BeanException("illegal bean,id type mismatch :" + tClass +",dao:"+daoClass);
        }
        Enhancer enhancer = new Enhancer();
        enhancer.setInterfaces(new Class[]{daoClass});
        enhancer.setCallbacks(new Callback[]{new DaoInterceptor(tClass,daoClass),NoOp.INSTANCE});
        enhancer.setCallbackFilter(MethodFilter.getInstance());
        return (Dao) enhancer.create();

    }

}
