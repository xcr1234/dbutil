package com.owdp.dbutil.meta;


import com.owdp.dbutil.QueryException;
import com.owdp.dbutil.annotation.AfterInstance;
import com.owdp.dbutil.annotation.Column;
import com.owdp.dbutil.annotation.Id;
import com.owdp.dbutil.annotation.JoinColumn;
import com.owdp.dbutil.annotation.Reference;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class ColumnMeta implements Externalizable{
    private static final long serialVersionUID = -3924528163394766579L;
    private Field field;
    private Method setter;
    private Method getter;
    private String name;
    private Meta meta;

    @Deprecated
    public ColumnMeta(){
        //used for Externalizable
    }

    public ColumnMeta(Meta meta,Field field, Method setter, Method getter,String name) {
        assert meta != null && name != null && (field != null || setter != null || getter != null);
        this.meta = meta;
        this.field = field;
        this.setter = setter;
        this.getter = getter;
        this.name = name;
    }

    public Class getType(){
        if(getter != null){
            return getter.getReturnType();
        }else if(field != null){
            return field.getDeclaringClass();
        }else if(setter != null){
            return setter.getParameterTypes()[0];
        }
        return null;
    }

    public  <T extends Annotation> T getAnnotation(Class<T> type) {
        if (getter != null && getter.isAnnotationPresent(type)) {
            return getter.getAnnotation(type);
        } else if (field != null && field.isAnnotationPresent(type)) {
            return field.getAnnotation(type);
        } else if (setter != null && setter.isAnnotationPresent(type)) {
            return setter.getAnnotation(type);
        }
        return null;
    }

    public String getName() {
        Column column = getAnnotation(Column.class);
        if (column != null && !"".equals(column.value())) {
            return column.value();
        }
        return name;
    }

    public boolean select(){
        if(setter == null){
            return false;
        }
        if(getAnnotation(AfterInstance.class) != null){
            return false;
        }
        Column column = getAnnotation(Column.class);
        return column == null || column.select();
    }

    public boolean insert(){
        if(getter == null){
            return false;
        }
        if(getAnnotation(AfterInstance.class) != null){
            return false;
        }
        if(getAnnotation(JoinColumn.class) != null){
            return false;
        }
        Column column = getAnnotation(Column.class);
        Id id = getAnnotation(Id.class);
        return id == null ? column == null || column.insert() : (!id.identity() || !id.value().isEmpty());
    }

    public boolean update(){
        if(getter == null){
            return false;
        }
        if(getAnnotation(AfterInstance.class) != null){
            return false;
        }
        if(getAnnotation(JoinColumn.class) != null){
            return false;
        }
        Column column = getAnnotation(Column.class);
        return column == null || column.update();
    }

    public Field getField() {
        return field;
    }

    public Method getSetter() {
        return setter;
    }

    public Method getGetter() {
        return getter;
    }

    public boolean getAble(){
        return getter != null;
    }

    public boolean setAble(){
        return setter != null;
    }

    public Object get(Object obj)  {
        try {
            Object value = getter.invoke(obj);
            Reference reference = getAnnotation(Reference.class);
            if(reference != null && value != null){
                Class type = getType();
                return Meta.parse(type).getColumnMetas().get(reference.target()).get(value);
            }
            return value;
        } catch (IllegalAccessException e) {
            throw new QueryException("can't get the value of "+toString(),e);
        } catch (InvocationTargetException e) {
            throw new QueryException("can't get the value of "+toString(),e);
        }
    }

    public Meta getMeta() {
        return meta;
    }

    public void set(Object object, Object value)  {
        try {
            setter.invoke(object,value);
        } catch (IllegalAccessException e) {
            throw new QueryException("can't get the value of " +toString() ,e);
        } catch (InvocationTargetException e) {
            throw new QueryException("can't get the value of " +toString() ,e);
        }
    }

    @Override
    public String toString() {
        return "column:"+meta.getClazz() + "#"+ name;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        ColumnMeta that = (ColumnMeta) object;

        if (field != null ? !field.equals(that.field) : that.field != null) return false;
        if (setter != null ? !setter.equals(that.setter) : that.setter != null) return false;
        if (getter != null ? !getter.equals(that.getter) : that.getter != null) return false;
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (setter != null ? setter.hashCode() : 0);
        result = 31 * result + (getter != null ? getter.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(meta.getClazz());
        out.writeObject(name);
        if(field != null){
            out.writeInt(1);
            out.writeObject(field.getName());
        }else{
            out.writeInt(0);
        }
        if(getter != null){
            out.writeInt(1);
            out.writeObject(getter.getName());
            out.writeObject(getter.getParameterTypes());
        }else{
            out.writeInt(0);
        }
        if(setter != null){
            out.writeInt(1);
            out.writeObject(setter.getName());
            out.writeObject(setter.getParameterTypes());
        }else{
            out.writeInt(0);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        Class clazz = (Class) in.readObject();
        meta = Meta.parse(clazz);
        name = (String) in.readObject();
        int n = in.readInt();
        if( n > 0){
            String fieldName = (String) in.readObject();
            try {
                field = clazz.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                throw new IOException("invalid data for com.owdp.dbutil.meta.ColumnMeta");
            }
        }
        n = in.readInt();
        if(n > 0){
            String getterName = (String) in.readObject();
            Class<?>[] getReturn = (Class<?>[]) in.readObject();
            try {
                getter = clazz.getDeclaredMethod(getterName,getReturn);
            } catch (NoSuchMethodException e) {
                throw new IOException("invalid data for com.owdp.dbutil.meta.ColumnMeta");
            }
        }
        n = in.readInt();
        if(n > 0){
            String setterName = (String) in.readObject();
            Class<?>[] setReturn = (Class<?>[]) in.readObject();
            try {
                setter = clazz.getDeclaredMethod(setterName,setReturn);
            } catch (NoSuchMethodException e) {
                throw new IOException("invalid data for com.owdp.dbutil.meta.ColumnMeta");
            }
        }
    }
}
