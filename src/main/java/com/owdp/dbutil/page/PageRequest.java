package com.owdp.dbutil.page;


import java.io.Serializable;

public abstract class PageRequest implements Pageable,Serializable{
    private static final long serialVersionUID = -953282950834244130L;
    //注意：pageNumber从1开始
    private int pageNumber;
    private int pageSize;


    public PageRequest(int pageNumber, int pageSize) {
        if(pageNumber < 1){
            throw new IllegalArgumentException("pageNumber must be larger than 1");
        }
        if(pageSize < 1){
            throw new IllegalArgumentException("pageNumber must be larger than 0");
        }
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
    }

    @Override
    public int limit() {
        return pageSize;
    }

    @Override
    public int offset() {
        return (pageNumber - 1) * pageSize;
    }

    @Override
    public int from() {
        return (pageNumber - 1) * pageSize;
    }

    @Override
    public int to() {
        return pageNumber * pageSize;
    }
}
