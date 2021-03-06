package com.owdp.dbutil.impl;

import com.owdp.dbutil.BeanException;
import com.owdp.dbutil.DbUtils;
import com.owdp.dbutil.Transactional;
import com.owdp.dbutil.annotation.Transaction;
import com.owdp.dbutil.cache.Cache;
import com.owdp.dbutil.cache.CacheEvict;
import com.owdp.dbutil.cache.CacheEvictAll;
import com.owdp.dbutil.cache.CacheManager;
import com.owdp.dbutil.cache.CachePut;
import com.owdp.dbutil.cache.Cacheable;
import com.owdp.dbutil.cache.Name;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import ognl.Ognl;
import ognl.OgnlException;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;


public class ServiceInterceptor implements MethodInterceptor {

    private Class serviceClass;

    public ServiceInterceptor(Class serviceClass) {
        this.serviceClass = serviceClass;
    }

    @Override
    public Object intercept(Object self, Method thisMethod, Object[] args, MethodProxy proxy) throws Throwable {
        boolean trans = false;
        try {
            if(thisMethod.isAnnotationPresent(Transaction.class)){
                Transaction transaction = thisMethod.getAnnotation(Transaction.class);
                String condition = transaction.condition();
                if (condition.isEmpty() || filter(condition, thisMethod, args, null)) {
                    trans = true;
                    Transactional.begin();
                }
            }
            return invoke0(self,thisMethod,proxy,args);
        }finally {
            if(trans){
                Transactional.commit();
            }
        }
    }



    private String createKey(String key,Method method,Object args[],Object result) {
        /*if(args.length == 0){
            return key;
        }*/
        if(key.isEmpty()){
            return method.getName();
        }
        Map<String,Object> context = createOgnlContext(method,args);
        if(result != null){
            context.put("_result_",result);
        }
        try {
            return String.valueOf(Ognl.getValue(key,context));
        } catch (OgnlException e) {
            throw new BeanException("illegal service "+ serviceClass + ",invalid key expression",e);
        }

    }

    private boolean filter(String condition,Method method,Object args[],Object result) {
        if(condition.isEmpty()){
            return true;
        }
        Map<String , Object> context = createOgnlContext(method,args);
        if(result != null){
            context.put("_result_",result);
        }
        try {
            Object value = Ognl.getValue(condition,context);
            if(!(value instanceof Boolean)){
                throw new BeanException("illegal service "+ serviceClass + ",condition \"" + condition + "\" should return boolean value.");
            }
            return (Boolean)value;
        } catch (OgnlException e) {
            throw new BeanException("illegal service "+ serviceClass + ",invalid key expression",e);
        }
    }



    private Map<String , Object> createOgnlContext(Method method,Object[] args){
        Map<String , Object> context = new HashMap<String , Object>();
        for(int i=0;i<args.length;i++){
            context.put("arg"+i,args[i]);
        }
        Annotation[][] annotations = method.getParameterAnnotations();
        for(int i=0;i<annotations.length;i++){
            for(Annotation annotation:annotations[i]){
                if(annotation instanceof Name){
                    context.put(((Name) annotation).value(),args[i]);
                    break;
                }
            }
        }
        return context;
    }

    private  Object invoke0(Object self, Method thisMethod, MethodProxy proxy, Object[] args) throws Throwable{
        Object value = null;
        CacheManager cacheManager = DbUtils.getCacheManager();
        if(cacheManager != null){
            //有Cacheable注解.
            if(thisMethod.isAnnotationPresent(Cacheable.class)){
                Cacheable cacheable = thisMethod.getAnnotation(Cacheable.class);

                String key = createKey(cacheable.key(),thisMethod,args,null);
                Cache cache = cacheManager.getCache(cacheable.value());

                if(cache != null && filter(cacheable.condition(),thisMethod,args,null)){  //可以从cache中取值
                    value = cache.get(key,thisMethod.getReturnType());
                    if(value != null){
                        return value;
                    }
                    value = proxy.invokeSuper(self,args);   //value中值为null或者class不匹配的情况
                    if(value != null){
                        cache.put(key,value);
                    }
                }else {
                    value = proxy.invokeSuper(self,args); //不满足从cache中取值的条件
                }
                return value;
            }

            //如果有Cacheput注解，则执行原方法，然后放入cache.
            if(thisMethod.isAnnotationPresent(CachePut.class)){
                Object thisValue = proxy.invokeSuper(self,args);//执行原方法
                CachePut cachePut = thisMethod.getAnnotation(CachePut.class);
                String key = createKey(cachePut.key(),thisMethod,args,thisValue);

                Cache cache = cacheManager.getCache(cachePut.value());
                if(cache != null && filter(cachePut.condition(),thisMethod,args,thisValue) && thisValue != null){
                    cache.put(key,thisValue);
                }
                return thisValue;
            }

            //如果有CacheEvict注解
            if(thisMethod.isAnnotationPresent(CacheEvict.class)){
                CacheEvict cacheEvict = thisMethod.getAnnotation(CacheEvict.class);
                String key = createKey(cacheEvict.key(),thisMethod,args,null);
                Cache cache = cacheManager.getCache(cacheEvict.value());
                if(cache != null && filter(cacheEvict.condition(),thisMethod,args,null) && cacheEvict.beforeInvocation()){
                    if(cacheEvict.allEntries()){
                        cache.clear();
                    }else{
                        cache.remove(key);
                    }
                }
                Object thisValue = proxy.invokeSuper(self,args);   //执行原方法
                if(cache != null && filter(cacheEvict.condition(),thisMethod,args,thisValue) && !cacheEvict.beforeInvocation()){
                    key = createKey(cacheEvict.key(),thisMethod,args,thisValue);
                    if(cacheEvict.allEntries()){
                        cache.clear();
                    }else{
                        cache.remove(key);
                    }
                }
                return thisValue;
            }

            //如果有CacheEvictAll注解，则清除全部cache.
            if(thisMethod.isAnnotationPresent(CacheEvictAll.class)){
                CacheEvictAll cacheEvictAll = thisMethod.getAnnotation(CacheEvictAll.class);
                if(cacheEvictAll.beforeInvocation()){
                    cacheManager.clearAll();
                }
                Object thisValue = proxy.invokeSuper(self,args);   //执行原方法
                if(!cacheEvictAll.beforeInvocation()){
                    cacheManager.clearAll();
                }
                return thisValue;
            }
        }
        return proxy.invokeSuper(self,args);
    }

}
