package com.lb.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 购物车更新单元类，用于表示购物车中商品数量的更新信息
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CartUu {

    /**
     * 起始日期，表示更新周期的开始时间
     */
    private String start;

    /**
     * 结束日期，表示更新周期的结束时间
     */
    private String end;

    /**
     * 当前日期，表示购物车更新的当前时间
     */
    private String curDate;

    /**
     * 计数，表示购物车中商品数量的更新量
     */
    private Long count;
}

