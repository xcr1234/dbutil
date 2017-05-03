package com.owdp.dbutil.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <pre>
 * 自动注入Dao
 *  在Service中使用Dao，可以用这样的形式：

 *  public class XXService {

 *  {@code @Dao}
 *  private XXDao xxDao;
 *
 *  {@code @Cacheable(value = "xxx",key = "xxx")}
 *  public List&lt;XX&gt; listAll(){
 *  xxDao.listAll();
 *  }
 *  }
 *
 *  在调用
 *  XXService xxService = DbUtils.getServiceImpl(XXService.class);
 *  后，xxDao会被自动赋值
 *
 * </pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Dao {
}
