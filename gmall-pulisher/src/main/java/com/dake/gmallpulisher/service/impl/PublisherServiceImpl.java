package com.dake.gmallpulisher.service.impl;

import com.dake.gmallpulisher.mapper.DauMapper;
import com.dake.gmallpulisher.mapper.OrderMapper;
import com.dake.gmallpulisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHour(String date) {
        //1.查询Phoenix
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map用于存放调整结构后的数据
        HashMap<String, Long> result = new HashMap<>();

        //3.调整结构
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        //4.返回数据
        return result;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHourMap(String date) {
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);
        HashMap<String, Double> result = new HashMap<>();

        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"),(Double) map.get("SUM_AMOUNT"));
        }

        return result;
    }
}
