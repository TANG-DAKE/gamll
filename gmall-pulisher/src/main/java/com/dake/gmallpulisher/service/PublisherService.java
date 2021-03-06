package com.dake.gmallpulisher.service;

import java.util.Map;

public interface PublisherService {

    public Integer getDauTotal(String date);

    public Map getDauTotalHour(String date);

    public Double getOrderAmountTotal(String date);

    public Map getOrderAmountHourMap(String date);

    public Map getSaleDetail(String date,int startPage,int size,String keyWord);
}
