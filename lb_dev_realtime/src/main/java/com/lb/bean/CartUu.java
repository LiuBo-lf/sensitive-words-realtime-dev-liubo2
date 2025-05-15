package com.lb.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CartUu {

    private String start;
    private String end;
    private String curDate;
    private Long count;
}
