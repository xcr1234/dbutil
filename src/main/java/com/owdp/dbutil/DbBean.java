package com.owdp.dbutil;


import com.owdp.dbutil.meta.ColumnMeta;
import com.owdp.dbutil.meta.Meta;

import java.io.Serializable;


public abstract class DbBean implements Serializable{
    private static final long serialVersionUID = -2749113043154647433L;




    @Override
    public int hashCode() {
        Meta meta = Meta.parse(getClass());
        Object id = meta.getIdColumn().get(this);
        return id == null ? 0 : id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null){
            return false;
        }
        if(obj.getClass() != getClass()){
            return false;
        }
        if(obj instanceof DbBean){
            Meta meta = Meta.parse(getClass());
            Object id = meta.getIdColumn().get(this);
            Object objId = meta.getIdColumn().get(obj);
            return id == null ? objId == null : id.equals(objId);
        }
        return false;
    }

    @Override
    public String toString() {
        ColumnMeta idColumn = Meta.parse(getClass()).getIdColumn();
        return getClass().getSimpleName() +
                "{" +
                idColumn.getName() + "=" + idColumn.get(this) +
                "}";
    }
}
