package com.dake.gmallpulisher.service.impl;

import com.dake.gmallpulisher.bean.Option;
import com.dake.gmallpulisher.bean.Stat;
import com.dake.gmallpulisher.mapper.DauMapper;
import com.dake.gmallpulisher.mapper.OrderMapper;
import com.dake.gmallpulisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private JestClient jestClient;

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
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        return result;
    }

    @Override
    public Map getSaleDetail(String date, int startPage, int size, String keyWord) {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyWord).operator(MatchQueryBuilder.Operator.AND));
        sourceBuilder.query(boolQueryBuilder);

        TermsBuilder ageAggs = AggregationBuilders.terms("groupByAge").field("user_age").size(100);
        sourceBuilder.aggregation(ageAggs);

        TermsBuilder genderAggs = AggregationBuilders.terms("groupByGender").field("user_gender").size(3);
        sourceBuilder.aggregation(genderAggs);

        sourceBuilder.from((startPage - 1) * size);
        sourceBuilder.size(size);

        Search search = new Search.Builder(sourceBuilder.toString())
                .addIndex("")
                .addType("")
                .build();
        SearchResult searchResult = null;
        try {
            searchResult = jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //获取结果集  解析查询  解析聚合组
        HashMap<String, Object> result = new HashMap<>();
        Long total = searchResult.getTotal();
        result.put("total",total );
        //封装年龄，性别聚合组数据
        MetricAggregation aggregations = searchResult.getAggregations();
        ArrayList<Stat> stats = new ArrayList<>();
        TermsAggregation groupByAge = aggregations.getTermsAggregation("groupByAge");
        //的定义每个年龄段人数变量
        Long lower20 = 0L;
        Long upper20To30 = 0L;
        for (TermsAggregation.Entry entry : groupByAge.getBuckets()) {
            long age = Long.parseLong(entry.getKey());
            if(age <20){
                lower20 += entry.getCount();
            } else if (age <= 30) {
                upper20To30 += entry.getCount();
            }
        }
        //将人数准换为比例
        double lower20Ratio = (Math.round(lower20 * 1000D / total)) / 10D;
        double upper20To30Ratio = (Math.round(upper20To30 * 1000D / total)) / 10D;
        double upper30Ratio = 100D - lower20Ratio - upper20To30Ratio;

        Option lower20Opt = new Option("20岁以下",lower20Ratio);
        Option upper20To30Opt = new Option("20岁到30岁",upper20To30Ratio);
        Option upper30Opt = new Option("30岁及20岁以上",upper30Ratio);

        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(lower20Opt);
        ageOptions.add(upper20To30Opt);
        ageOptions.add(upper30Opt);

        Stat ageStat = new Stat(ageOptions, "用户年龄占比");

        stats.add(ageStat);

        TermsAggregation groupByGender = aggregations.getTermsAggregation("groupByGender");
        Long femaleCount = 0L;

        for (TermsAggregation.Entry entry : groupByGender.getBuckets()) {
            if ("F".equals(entry.getKey())) {
                femaleCount+=entry.getCount();
            }
        }

        double femaleRatio = (Math.round(femaleCount * 1000D / total)) / 10D;
        double maleRatio = 100D - femaleRatio;

        Option femaleOpt = new Option("男", femaleRatio);
        Option maleOpt = new Option("女", maleRatio);

        ArrayList<Option> genderOptions = new ArrayList<>();
        genderOptions.add(femaleOpt);
        genderOptions.add(maleOpt);

        Stat genderStat = new Stat(genderOptions, "用户性别占比 ");

        stats.add(genderStat);

        result.put("stat",stats);
        //添加明细数据
        ArrayList<Map> details = new ArrayList<>();

        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            details.add(hit.source);
        }

        result.put("detail",details);

        return result;
    }
}
