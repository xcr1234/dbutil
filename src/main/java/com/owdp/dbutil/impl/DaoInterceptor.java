package com.owdp.dbutil.impl;

import com.owdp.dbutil.BeanException;
import com.owdp.dbutil.QueryException;
import com.owdp.dbutil.QueryResult;
import com.owdp.dbutil.annotation.Query;
import com.owdp.dbutil.annotation.UpdateQuery;
import com.owdp.dbutil.meta.Meta;
import com.owdp.dbutil.page.Pageable;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;


public final class DaoInterceptor implements MethodInterceptor {

    private DbDaoImpl dbDao;
    private Meta meta;
    private Class daoClass;

    public DaoInterceptor(Class c,Class daoClass){
        meta = Meta.parse(c);
        this.daoClass = daoClass;
        dbDao = DbDaoImpl.create(c);
    }


    @Override
    public Object intercept(Object self, Method thisMethod, Object[] args, MethodProxy proxy) throws Throwable {
        if(thisMethod.isAnnotationPresent(Query.class)){
            Query query = thisMethod.getAnnotation(Query.class);
            if(thisMethod.getReturnType() == QueryResult.class){
                Pageable pageable = null;
                Object[] params = null;
                if(args.length > 0){
                    if(args[args.length-1] instanceof Pageable){
                        pageable = (Pageable) args[args.length-1];
                        params = new Object[args.length -1];
                        System.arraycopy(args, 0, params, 0, args.length - 1);
                    }else{
                        pageable = null;
                        params = args;
                    }
                }else{
                    pageable = null;
                    params = args;
                }
                return dbDao.queryResult(query.sql(),pageable,params);
            }
            if(thisMethod.getReturnType() == List.class){
                Pageable pageable = null;
                Object[] params = null;
                if(args.length > 0){
                    if(args[args.length-1] instanceof Pageable){
                        pageable = (Pageable) args[args.length-1];
                        params = new Object[args.length -1];
                        System.arraycopy(args, 0, params, 0, args.length - 1);
                    }else{
                        pageable = null;
                        params = args;
                    }
                }else{
                    pageable = null;
                    params = args;
                }
                return dbDao.listBySql(query.sql(),pageable,params);
            }else if(thisMethod.getReturnType() == meta.getClazz()){
                return dbDao.findBySql(query.sql(),args);
            }else{
                try {
                    return dbDao.findValueBySql(query.sql(),args,thisMethod.getReturnType(),null);
                }catch (QueryException e){
                    if(e.getMessage() != null && e.getMessage().startsWith("unsupported sql type")){
                        throw new BeanException("illegal dao : "+daoClass+" ,method:"+thisMethod+",@Query method 's return-type doesn't support:" + thisMethod.getReturnType());
                    }
                    throw e;
                }
            }
        }
        if(thisMethod.isAnnotationPresent(UpdateQuery.class)){
            UpdateQuery updateQuery = thisMethod.getAnnotation(UpdateQuery.class);
            Class returnType = thisMethod.getReturnType();
            if(returnType == Void.class || returnType == void.class){
                dbDao.updateQuery(updateQuery.sql(),args);
                return null;
            }else if(returnType == int.class || returnType == Integer.class){
                return dbDao.updateQuery(updateQuery.sql(),args);
            }else if(returnType == boolean.class || returnType == Boolean.class){
                return dbDao.updateQuery(updateQuery.sql(),args) > 0;
            }else{
                throw new BeanException("illegal dao:" + daoClass+",invalid @UpdateQuery method "+thisMethod+" ,return type:"+returnType);
            }
        }
        try {
            //如果是dbDao中的实现方法.
            Method method = DbDaoImpl.class.getDeclaredMethod(thisMethod.getName(),thisMethod.getParameterTypes());
            try {
                return method.invoke(dbDao,args);
            }catch (InvocationTargetException e){
                throw e.getCause() != null ? e.getCause() : e;
            }
        }catch (NoSuchMethodException e){
            return proxy.invokeSuper(self,args);
        }
    }
}
