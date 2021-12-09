package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws Exception {
        // 创建集合用于存放查询结果数据
        ArrayList<T> resultList = new ArrayList<T>();
        // 预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        // 执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        // 解析
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            // 创建泛型对象并赋值
            T t = clz.newInstance();
            // 给泛型对象赋值
            for (int i = 1; i < columnCount + 1; i++) {
                String columnName = metaData.getColumnName(i);
                // 判断是否需要转换为驼峰命令
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                // 获取列值
                Object value = resultSet.getObject(i);
                // 给泛型对象赋值
                BeanUtils.setProperty(t, columnName, value);
            }
            // 将改对象添加至集合
            resultList.add(t);
        }
        preparedStatement.close();
        resultSet.close();
        return resultList;
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        List<JSONObject> jsonObjects = queryList(connection, "select * from " + GmallConfig.HBASE_SCHEMA + ".DIM_USER_INFO", JSONObject.class, true);
        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }
        connection.close();
    }

}
