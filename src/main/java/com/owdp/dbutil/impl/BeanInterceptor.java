package com.owdp.dbutil.impl;

import com.owdp.dbutil.BeanException;
import com.owdp.dbutil.annotation.JoinColumn;
import com.owdp.dbutil.annotation.Reference;
import com.owdp.dbutil.meta.ColumnMeta;
import com.owdp.dbutil.meta.Meta;
import com.owdp.dbutil.util.BeanUtils;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


public class BeanInterceptor implements MethodInterceptor {


    private Meta meta;

    public BeanInterceptor(Meta meta) {
        this.meta = meta;
    }


    @Override
    public Object intercept(Object self, Method thisMethod, Object[] args, MethodProxy proxy) throws Throwable {
        if(BeanUtils.isGetter(thisMethod)){
            String entity = BeanUtils.getEntity(thisMethod);
            ColumnMeta columnMeta = meta.getColumnMetas().get(entity);
            JoinColumn joinColumn = null;
            if(columnMeta != null && (joinColumn =columnMeta.getAnnotation(JoinColumn.class)) != null){
                Object queryValue = null;
                if(thisMethod.getReturnType() != List.class && thisMethod.getReturnType() != ArrayList.class){
                    throw new BeanException("illegal join column :" + columnMeta+",return type must be java.util.List");
                }
                Meta targetMeta = Meta.parse(joinColumn.target());
                ColumnMeta targetColumn = targetMeta.getColumnMetas().get(joinColumn.mappedBy());
                if(targetColumn == null){
                    throw new BeanException("illegal join column :" + columnMeta+" ,target column not found:"+joinColumn.target()+"#"+joinColumn.mappedBy());
                }
                if(!targetColumn.getType().isAssignableFrom(self.getClass())){
                    throw new BeanException("illegal join column:" + columnMeta+" ,target type mismatches.expected:"+self.getClass()+" ,but meet "+targetColumn.getType());
                }
                DbDaoImpl dbDao = DbDaoImpl.create(targetColumn.getType());
                Reference reference = targetColumn.getAnnotation(Reference.class);
                if(reference != null){
                    ColumnMeta thisColumn = meta.getColumnMetas().get(reference.target());
                    if(thisColumn == null){
                        throw new BeanException("illegal join column :" +columnMeta +" ,reference not found: column "+meta.getClazz()+"#"+reference.target());
                    }
                    if(thisColumn.getAble()){
                        Object thisValue = thisColumn.get(self);
                        return dbDao.listBySql("select * from " + targetMeta.getTableName() + " where " + targetColumn.getName() + " = ?",null,new Object[]{thisValue});
                    }
                }
                return dbDao.listAll();
            }
        }
        return proxy.invokeSuper(self,args);
    }


}
