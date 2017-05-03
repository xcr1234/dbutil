package com.owdp.dbutil.impl;

import com.owdp.dbutil.DbDao;
import com.owdp.dbutil.DbUtils;
import com.owdp.dbutil.QueryException;
import com.owdp.dbutil.QueryResult;
import com.owdp.dbutil.Transactional;
import com.owdp.dbutil.annotation.Reference;
import com.owdp.dbutil.meta.ColumnMeta;
import com.owdp.dbutil.meta.Meta;
import com.owdp.dbutil.page.Pageable;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.NoOp;


import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * DbDao接口的实现类，用该类来实现动态代理
 */
public final class DbDaoImpl implements DbDao{

    private Meta meta;

    private static Map<Class,DbDaoImpl> map = Collections.synchronizedMap(new HashMap<Class,DbDaoImpl>());

    public static DbDaoImpl create(Class type){
        DbDaoImpl dbDao = map.get(type);
        if(dbDao == null){
            dbDao = new DbDaoImpl(type);
            map.put(type,dbDao);
        }
        return dbDao;
    }

    private DbDaoImpl(Class<?> type) {
        meta = Meta.parse(type);
    }


    private static void setObject(PreparedStatement ps,int index,Object value) throws SQLException {
        if(value == null){
            ps.setObject(index,null);
        }else if(value instanceof String){
            ps.setString(index, (String) value);
        }else if(value instanceof Boolean){
            ps.setBoolean(index, (Boolean) value);
        }else if(value instanceof Byte){
            ps.setByte(index, (Byte) value);
        }else if(value instanceof Short){
            ps.setShort(index, (Short) value);
        }else if(value instanceof Integer){
            ps.setInt(index, (Integer) value);
        }else if(value instanceof Long){
            ps.setLong(index, (Long) value);
        }else if(value instanceof Float){
            ps.setFloat(index, (Float) value);
        }else if(value instanceof Double){
            ps.setDouble(index, (Double) value);
        }else if(value instanceof BigDecimal){
            ps.setBigDecimal(index, (BigDecimal) value);
        }else if(value instanceof byte[]){
            ps.setBytes(index, (byte[]) value);
        }else if(value instanceof Timestamp){
            ps.setTimestamp(index, (Timestamp) value);
        } else if(value instanceof java.sql.Date){
            ps.setDate(index, (Date) value);
        }else if(value instanceof Time){
            ps.setTime(index, (Time) value);
        }else if(value instanceof java.util.Date){
            ps.setDate(index,new java.sql.Date(((java.util.Date) value).getTime()));
        }else if(value instanceof Blob){
            ps.setBlob(index, (Blob) value);
        }else if(value instanceof InputStream ){
            ps.setBlob(index, (InputStream) value);
        }else if(value instanceof Clob){
            ps.setClob(index, (Clob) value);
        }else if(value instanceof Reader){
            ps.setClob(index, (Reader) value);
        }else {
            ps.setObject(index,value);
        }
    }


    @Override
    public Object save(Object object) {
        if(object == null){
            throw new QueryException("cannot save null!");
        }
        String sql = SQLBuilder.insert(meta);

        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);

        Connection connection = DbUtils.getConnection();

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            int index = 1;
            for(ColumnMeta columnMeta : meta.getColumnMetas().values()){
                if(columnMeta.insert()){
                    setObject(ps,index++,columnMeta.get(object));
                }
            }
            int row = ps.executeUpdate();
            if(row <= 0){
                return null;
            }
            ResultSet gkRs = ps.getGeneratedKeys();
            if(gkRs.next()){
                Object gkId = ResultSetHandler.get(gkRs,1,meta.getIdColumn().getType(),meta.getIdColumn().toString());
                meta.getIdColumn().set(object,gkId);
                return object;
            }else {
                return object;
            }
        }catch (SQLException e){
            err(connection,e);
            return null;
        }finally {
            finaly(connection,ps,null);
        }

    }

    private static void finaly(Connection connection,PreparedStatement ps,ResultSet rs){
        if(rs != null){
            try {
                rs.close();
            } catch (SQLException e) {}
        }
        if(ps != null){
            try {
                ps.close();
            } catch (SQLException e) {}
        }
        if(connection != null && !Transactional.hasBegin()){
            try {
                connection.close();
            } catch (SQLException e) {}
        }
    }

    private static void err(Connection connection,SQLException e){
        try {
            if(Transactional.hasBegin()){
                try {
                    connection.rollback();
                } catch (SQLException e1) {
                }finally {
                    try {
                        connection.close();
                    } catch (SQLException e1) {}
                }
            }
        }finally {
            DbUtils.getExceptionCaught().sqlError(e);
        }
        throw new QueryException(e);
    }

    @Override
    public boolean delete(Object object) {
        if(object == null){
            throw new QueryException("cannot delete null!");
        }
        String sql = SQLBuilder.delete(meta);
        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);
        Connection connection = DbUtils.getConnection();
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            setObject(ps,1,meta.getIdColumn().get(object));
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            err(connection,e);
            return false;
        }finally {
            finaly(connection,ps,null);
        }
    }

    @Override
    public boolean deleteByID(Serializable id) {
        if(id == null){
            throw new QueryException("cannot delete null!");
        }
        String sql = SQLBuilder.delete(meta);
        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);
        Connection connection = DbUtils.getConnection();
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            setObject(ps,1,id);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            err(connection,e);
            return false;
        }finally {
            finaly(connection,ps,null);
        }
    }

    @Override
    public int deleteAll() {
        String sql = SQLBuilder.deleteAll(meta);
        Connection connection = DbUtils.getConnection();
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            return ps.executeUpdate();
        } catch (SQLException e) {
            err(connection,e);
            return 0;
        }finally {
            finaly(connection,ps,null);
        }
    }

    @Override
    public void update(Object object) {
        if(object == null){
            throw new QueryException("can't update null!");
        }

        String sql = SQLBuilder.update(meta);
        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);
        Connection connection = DbUtils.getConnection();
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            int count = 1;
            for(ColumnMeta columnMeta:meta.getColumnMetas().values()){
                if(columnMeta.update()){
                    setObject(ps,count ++ ,columnMeta.get(object));
                }
            }
            ps.executeUpdate();
        }catch (SQLException e){
            err(connection,e);
        }finally {
            finaly(connection,ps,null);
        }
    }

    @Override
    public boolean exists(Serializable id) {
        String sql = SQLBuilder.exists(meta);
        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);
        Connection connection = DbUtils.getConnection();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            err(connection,e);
            return false;
        }finally {
            finaly(connection,ps,rs);
        }
    }

    @Override
    public int count() {
        String sql = SQLBuilder.count(meta);
        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);
        Connection connection = DbUtils.getConnection();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            if(rs.next()){
                return rs.getInt(1);
            }else{
                return 0;
            }
        } catch (SQLException e) {
            err(connection,e);
            return 0;
        }finally {
            finaly(connection,ps,rs);
        }
    }

    @Override
    public Object findById(Serializable id) {
        return findByColumn(id,meta,meta.getIdColumn(),1);
    }

    Object findValueBySql(String sql,Object [] params,Class type,String src){
        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);
        Connection connection = DbUtils.getConnection();
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            for(int i=1;i<=params.length;i++){
                setObject(ps,i,params[i-1]);
            }
            rs = ps.executeQuery();

            if(rs.next()){
                return ResultSetHandler.get(rs,1,type,src);
            }

            return null;
        } catch (SQLException e) {
            err(connection,e);
            return null;
        }finally {
            finaly(connection,ps,rs);
        }
    }

    int updateQuery(String sql,Object [] params){
        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);
        Connection connection = DbUtils.getConnection();
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            for(int i=1;i<=params.length;i++){
                setObject(ps,i,params[i-1]);
            }
            return ps.executeUpdate();
        } catch (SQLException e) {
            err(connection,e);
            return 0;
        }finally {
            finaly(connection,ps,null);
        }
    }


    Object findBySql(String sql,Object [] params){
        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);
        Connection connection = DbUtils.getConnection();
        ResultSet rs = null;
        PreparedStatement ps = null;

        try {
            ps = connection.prepareStatement(sql);
            for(int i=1;i<=params.length;i++){
                setObject(ps,i,params[i-1]);
            }
            rs = ps.executeQuery();

            Set<String> set = new HashSet<String>();
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            for(int i=1;i<=resultSetMetaData.getColumnCount();i++){
                set.add(resultSetMetaData.getColumnLabel(i));
            }

            if(rs.next()){
                return fromResultSet(set,rs,meta,1);
            }

            return null;
        } catch (SQLException e) {
            err(connection,e);
            return null;
        }finally {
            finaly(connection,ps,rs);
        }
    }


    QueryResult queryResult(String sql,Pageable pageable,Object [] values){
        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);
        PreparedStatement ps = null;
        ResultSet rs = null;

        Connection connection = DbUtils.getConnection();

        try {
            ps = connection.prepareStatement(pageable == null ? sql : pageable.pageSelect(sql));
            int i = 1;
            if(values != null){
                for(Object value:values){
                    setObject(ps,i++,value);
                }
            }
            if(pageable != null){
                pageable.fillState(ps,i);
            }
            rs = ps.executeQuery();
            return new QueryResultImpl(rs);
        } catch (SQLException e) {
            err(connection,e);
            return null;
        }finally {
            finaly(connection,ps,rs);
        }
    }


    List listBySql(String sql,Pageable pageable,Object [] values) {
        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);
        PreparedStatement ps = null;
        ResultSet rs = null;

        Connection connection = DbUtils.getConnection();

        try {
            ps = connection.prepareStatement(pageable == null ? sql : pageable.pageSelect(sql));
            int i = 1;
            if(values != null){
                for(Object value:values){
                    setObject(ps,i++,value);
                }
            }
            if(pageable != null){
                pageable.fillState(ps,i);
            }
            rs = ps.executeQuery();
            Set<String> set = new HashSet<String>();
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            for(int j=1;j<=resultSetMetaData.getColumnCount();j++){
                set.add(resultSetMetaData.getColumnLabel(j));
            }
            List list = new ArrayList();
            while (rs.next()){
                list.add(fromResultSet(set,rs,meta,1));
            }
            return list;
        } catch (SQLException e) {
            err(connection,e);
            return null;
        }finally {
            finaly(connection,ps,rs);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List listAll() {
        String sql = SQLBuilder.findAll(meta);
        return listBySql(sql,null,null);
    }

    @Override
    public List listAll(Pageable pageable) {
        String sql = SQLBuilder.findAll(meta);
        return listBySql(sql,pageable,null);
    }




    private static Object createObject(Meta meta){
        if(meta.hasReference()){
            Enhancer enhancer = new Enhancer();
            enhancer.setSuperclass(meta.getClazz());
            enhancer.setCallbackFilter(MethodFilter.getInstance());
            enhancer.setCallbacks(new Callback[]{new BeanInterceptor(meta), NoOp.INSTANCE});
            return enhancer.create();
        }
        Class type = meta.getClazz();
        try {
            return type.newInstance();
        } catch (InstantiationException e) {
            throw new QueryException("can't instance object of " +type,e);
        } catch (IllegalAccessException e) {
            throw new QueryException("can't instance object of " +type,e);
        }
    }



    static Object fromResultSet(Set<String> set,ResultSet rs,Meta meta,int recursion) throws SQLException {

        Object object = createObject(meta);
        for(ColumnMeta cm : meta.getColumnMetas().values()){
            if(set.contains(cm.getName())){
                Reference reference = cm.getAnnotation(Reference.class);
                if(reference != null){
                    if(recursion > DbUtils.getMaxRecursion()){
                        cm.set(object,null);
                    }else{
                        Meta targetMeta = Meta.parse(cm.getType());
                        ColumnMeta targetColumn = targetMeta.getColumnMetas().get(reference.target());
                        Object referenceValue = ResultSetHandler.get(rs,cm.getName(),targetColumn.getType(),cm.toString());
                        Object value = findByColumn(referenceValue,targetMeta,targetColumn,recursion+1);
                        cm.set(object,value);
                    }
                }else{
                    Object value = ResultSetHandler.get(rs,cm.getName(),cm.getType(),cm.toString());
                    cm.set(object,value);
                }
            }
        }

        for(Method method:meta.getAfterInstanceMethods()){
            boolean access = method.isAccessible();
            if(!access){
                method.setAccessible(true);
            }
            try {
                method.invoke(object);
            } catch (Exception e) {
                throw new QueryException("can't invoke @AfterInstance method : " + method+" of " + meta.getClazz(),e);
            }finally {
                if(!access){
                    method.setAccessible(false);
                }
            }
        }
        return object;
    }

    private static Object findByColumn(Object columnValue,Meta meta,ColumnMeta columnMeta,int recursion){
        String sql = SQLBuilder.findByColumn(meta,columnMeta);
        if(DbUtils.isShowsql()) DbUtils.getSQLLogger().logSQL(sql);
        Connection connection = DbUtils.getConnection();
        ResultSet rs = null;
        PreparedStatement ps = null;

        try {
            ps = connection.prepareStatement(sql);
            setObject(ps,1,columnValue);

            rs = ps.executeQuery();

            Set<String> set = new HashSet<String>();
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            for(int i=1;i<=resultSetMetaData.getColumnCount();i++){
                set.add(resultSetMetaData.getColumnLabel(i));
            }

            if(rs.next()){

                return fromResultSet(set,rs,meta,recursion);
            }

            return null;
        } catch (SQLException e) {
            err(connection,e);
            return null;
        }finally {
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {}
            }
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {}
            }
        }

    }

}
