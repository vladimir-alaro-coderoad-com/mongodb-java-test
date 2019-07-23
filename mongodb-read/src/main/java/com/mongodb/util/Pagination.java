package com.mongodb.util;

public class Pagination {
    private Integer pageSize;
    private Integer pageNum;

    private Integer skip;
    private Integer limit;

    public Pagination(Integer pageSize, Integer pageNum) {
        this.pageSize = pageSize == null ? 1 : pageSize;
        this.pageNum = pageNum == null || pageNum > 100 ? 100 : pageNum;
        calcualte();
    }

    private void calcualte() {
        // calculate skip and limit
        skip = pageSize * (pageNum - 1);
        limit = pageNum;
    }

    public Integer getSkip() {
        return skip;
    }

    public Integer getLimit() {
        return limit;
    }
}
