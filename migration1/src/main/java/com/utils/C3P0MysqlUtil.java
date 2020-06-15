package com.utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/*
    创建C3P0连接池工具类: 用来从连接池中获取一个连接对象
 */
public class C3P0MysqlUtil {
    //在创建C3P0的连接池对象
    //创建ComboPooledDataSource对象时,
    //会读取src根目录中的c3p0.properties/c3p0-config.xml配置文件,完成相关配置
    //至于说,你怎么读取的,我们不关心
    private static ComboPooledDataSource cpds = new ComboPooledDataSource("mysql");
    private C3P0MysqlUtil(){}

    //定义静态方法,获取连接池对象
    public static DataSource getDataSource() {
        return cpds;
    }

    //定义静态方法,获取一个连接对象
    public static Connection getConnection() throws SQLException {
        //2.连接池对象,调用 getConnection(): 获取连接对象
        return cpds.getConnection();
    }
    //定义静态方法,关闭资源
    public static void release(Connection con, Statement stmt, ResultSet rs) {
        //5.关闭资源: 必须被执行
        if(stmt!=null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(con!=null) {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(rs!=null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
