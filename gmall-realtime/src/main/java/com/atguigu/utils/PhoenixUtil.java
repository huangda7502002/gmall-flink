package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {
    // 定义数据库连接对象
    private static Connection conn;

    /**
     * 初始化 SQL 执行环境
     */
    public static void initializeConnection() {
        try {
            // 1. 注册驱动
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            // 2. 获取连接对象
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            // 3. 设置 Phoenix SQL 执行使用的 schema（对应 mysql 的 database）
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (ClassNotFoundException classNotFoundException) {
            System.out.println("注册驱动异常");
            classNotFoundException.printStackTrace();
        } catch (SQLException sqlException) {
            System.out.println("获取连接对象异常");
            ;
            sqlException.printStackTrace();
        }
    }

    /**
     * Phoenix 表数据导入方法
     * @param sinkTable 写入数据的 Phoenix 目标表名
     * @param data 待写入的数据
     */
    public static void insertValues(String sinkTable, JSONObject data) {
        // 双重校验锁初始化连接对象
        if(conn == null) {
            synchronized (PhoenixUtil.class) {
                if(conn == null) {
                    initializeConnection();
                }
            }
        }
        // 获取字段名
        Set<String> columns = data.keySet();
        // 获取字段对应的值
        Collection<Object> values = data.values();
        // 拼接字段名
        String columnStr = StringUtils.join(columns, ",");
        // 拼接字段值
        String valueStr = StringUtils.join(values, "','");
        // 拼接插入语句
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA
                + "." + sinkTable + "(" +
                columnStr + ") values ('" + valueStr + "')";

        // 为数据库操作对象赋默认值
        PreparedStatement preparedSt = null;

        // 执行 SQL
        try {
            System.out.println("插入语句为:" + sql);
            preparedSt = conn.prepareStatement(sql);
            preparedSt.execute();
            conn.commit();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            throw new RuntimeException("数据库操作对象获取或执行异常");
        } finally {
            if(preparedSt != null) {
                try {
                    preparedSt.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                    throw new RuntimeException("数据库操作对象释放异常");
                }
            }
        }
    }

}
