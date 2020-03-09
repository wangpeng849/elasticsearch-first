package com.wangp.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;
import sun.rmi.transport.Transport;

import java.net.InetAddress;

/**
 * @Author wangp
 * @Date 2020/3/9
 * @Version 1.0
 */
public class ElasticSearchClientTest {

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
    public void createIndex() throws Exception {
//        1.创建Settings对象，相当于是一个配置信息，主要配置集群的名称。
        Settings settings = Settings.builder()
                .put("cluster.name", "my-es")
                .build();
//        2.创建一个客户端Client对象
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
//        3.使用Client对象创建一个索引库
        AdminClient admin = client.admin();
        IndicesAdminClient indices = admin.indices();
        CreateIndexRequestBuilder index_hello = indices.prepareCreate("index_hello");
        //执行操作
        index_hello.get();
        //     等价于     ---->      client.admin().indices().prepareCreate("创建索引库的名称").get();
//        4.关闭Client对象
        client.close();
    }

    @Test
    public void settingsMappings() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "my-es").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        //创建mapping信息
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()//相对于json的开始的花括号
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type", "long")
                .field("store", true)
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("store", true)
                .field("analyzer", "ik_smart")
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("store", true)
                .field("analyzer", "ik_smart")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        //使用客户端将mapping设置到索引库
        client.admin()
                .indices()
                //设置要做映射的索引库
                .preparePutMapping("index_hello")
                //设置要做映射的type
                .setType("article")
                //mapping信息  可以是String 也可以是JSON 也可以XContentBuilder对象
                .setSource(builder)
                .get();
    }


    @Test
    public void testAddDocument() throws Exception{
        //创建一个clinet对象
        //创建一个文档对象
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("id",1L)
                    .field("title","ElasticSearch分布式全文检索")
                    .field("content","Elasticsearch全文检索java")
                .endObject();
        client.prepareIndex("index_hello","article")
                //设置文档id 不设置会自动生成一个id
                .setId("1")
                .setSource(builder)
                .get();
        client.close();
    }

    @Test
    public void testAddDocument2() throws Exception{
        //创建一个clinet对象
        //创建article对象
        Article article = new Article(2l,"我只想打一把游戏","你只想看一集电视");
        //创建json数据
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(article);
        System.out.println(json);
        //写入文档
        client.prepareIndex("index_hello","article")
                .setId("2")
                .setSource(json, XContentType.JSON)
                .get();
        client.close();
    }
}
