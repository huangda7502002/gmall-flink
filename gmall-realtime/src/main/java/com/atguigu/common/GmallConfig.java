package com.atguigu.common;

public class GmallConfig {

    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";
    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop200,hadoop201,hadoop202:2181";

}
