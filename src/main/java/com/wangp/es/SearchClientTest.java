package com.wangp.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author wangp
 * @Date 2020/3/9
 * @Version 1.0
 */
public class SearchClientTest {

    private TransportClient client;

    @Before
    public void init() throws Exception {
        //        1.创建Settings对象，相当于是一个配置信息，主要配置集群的名称。
        Settings settings = Settings.builder()
                .put("cluster.name", "my-es")
                .build();
//        2.创建一个客户端Client对象
        this.client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
    }


    @Test
    public void x () throws Exception{
//        1）创建client
//        2）创建一个查询对象，可以使用QueryBuilders工具类创建QueryBuilder
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds("1", "2");
//        3）使用client执行查询
        SearchResponse searchResponse = client.prepareSearch("index_hello")
                .setTypes("article")
                .setQuery(idsQueryBuilder)
                .get();
//        4）得到查询结果
        SearchHits hits = searchResponse.getHits();
//        5）取查询结果的总记录数
        System.out.println("查询到记录数："+hits.getTotalHits());
//        6）取查询结果列表
        System.out.println(hits.getHits());
        System.out.println("----------------------");
        Iterator<SearchHit> iterator = hits.iterator();
        while(iterator.hasNext()){
            SearchHit next = iterator.next();
            String sourceAsString = next.getSourceAsString();
            System.out.println("文档内容(json输出)："+ sourceAsString);
            Map<String, Object> source = next.getSource();
            System.out.println("文档内容属性：");
            System.out.println(source.get("id"));
            System.out.println(source.get("title"));
            System.out.println(source.get("content"));
        }
//        6）关闭Client对象
        client.close();
    }
}
