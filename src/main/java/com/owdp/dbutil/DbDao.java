package com.owdp.dbutil;

import com.owdp.dbutil.page.Pageable;

import java.io.Serializable;
import java.util.List;

/**
 * <pre>
 * 数据库查询的接口，所有数据库查询接口都必须继承该接口。
 * 如果查询过程中发生异常，如sql异常，则抛出{@link QueryException}。
 * 如果参数错误（参数为null），则抛出 {@link IllegalArgumentException}
 * </pre>
 */
public interface DbDao <T,ID extends Serializable>{
    /**
     * 将实体对象保存（insert）到数据库中
     * @param t 待保存的对象，不可为null
     * @return 若保存成功则返回该对象（对自增主键会自动处理），若不成功则返回null.
     */
    T save(T t);

    /**
     * 删除存在的java实体（根据id删除）
     * @param t 存在的java实体，不可为null
     * @return 是否删除成功
     */
    boolean delete(T t);

    /**
     * 删除存在的java实体（根据id删除）
     * @param id 存在的java实体的id，不可为null
     * @return 是否删除成功
     */
    boolean deleteByID(ID id);

    /**
     * 删除数据表的全部内容
     * @return 删除成功的数量，失败返回0
     */
    int deleteAll();

    /**
     * 更新（update）一个实体对象
     * @param t 待更新的对象，不可为null
     */
    void update(T t);

    /**
     * 判断实体id是否在数据表中
     * @param id 实体对象的id
     * @return id是否在数据表
     */
    boolean exists(ID id);

    /**
     * 计算数据表中的总数
     * @return 总数量
     */
    int count();

    /**
     * 根据id的值查询实体
     * @param id 实体对象的id
     * @return 返回查询到的实体，如果id不存在则返回null
     */
    T findById(ID id);

    /**
     * 返回所有的实体List，返回值其实是一个{@link java.util.ArrayList}
     * @return 所有的实体List
     */
    List<T> listAll();

    /**
     * 返回所有的实体List，支持分页查询，返回值其实是一个{@link java.util.ArrayList}
     * @param pageable 分页查询，建议使用{@link com.owdp.dbutil.page.PageRequest}实现类。参阅{@link Pageable}
     * @return 所有的实体List（已分页）
     */
    List<T> listAll(Pageable pageable);
}
