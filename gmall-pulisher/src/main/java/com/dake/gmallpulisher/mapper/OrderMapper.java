package com.dake.gmallpulisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {
    //查询当日订单总金额
    public Double selectOrderAmountTotal(String date);
    //当日订单金额分时统计
    public List<Map> selectOrderAmountHourMap(String date);

}
