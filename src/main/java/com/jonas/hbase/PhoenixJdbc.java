package com.jonas.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * phoenix 为 HBase 的 SQL 中间层
 *
 * @author shenjy
 * @version 1.0
 * @date 2021-11-14
 */
public class PhoenixJdbc {

    private static final String URL = "jdbc:phoenix:localhost:2181";

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

        /*
         * 指定数据库地址,格式为 jdbc:phoenix:Zookeeper 地址
         * 如果 HBase 采用 Standalone 模式或者伪集群模式搭建，则 HBase 默认使用内置的 Zookeeper，默认端口为 2181
         */
        Connection connection = DriverManager.getConnection(URL);
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM us_population");
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("city") + " " + resultSet.getInt("population"));
        }
        statement.close();
        connection.close();
    }
}
