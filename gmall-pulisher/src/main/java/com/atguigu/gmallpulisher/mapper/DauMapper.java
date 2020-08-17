package com.atguigu.gmallpulisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    //获取Phoenix表中的日活总数
    public Integer selectDauTotal(String date);
    public List<Map> selectDauTotalHourMap(String date);

}
