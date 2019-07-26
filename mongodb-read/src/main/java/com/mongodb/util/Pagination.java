package com.mongodb.util;

public class Pagination {
    private Integer pageSize;
    private Integer pageNum;

    private Integer skip;
    private Integer limit;

    public Pagination(Integer pageSize, Integer pageNum) {
        this.pageSize = pageSize == null || pageSize > 100 ? 100 : pageSize;
        this.pageNum = pageNum == null ? 1 : pageNum;
        calculate();
    }

    private void calculate() {
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
