package com.yszx4;

import com.utils.C3P0MysqlUtil;
import com.utils.C3P0SqlServerUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class mysql {
    public void  structure(String table ,String sql){
        //1.获取连接Connection对象
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = C3P0MysqlUtil.getConnection();
            //2.Connection对象获取执行sql语句的Statement对象
            stmt = con.createStatement();
            //4.Statement对象执行sql语句,获取结果
            int i = stmt.executeUpdate(sql);
            //5.处理结果
            if (i==0){
                System.out.println(table+"表创建成功");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            //6.关闭资源
            C3P0SqlServerUtil.release(con, stmt, rs);
        }
    }
    public void add(String sql){
        //1.获取连接Connection对象
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = C3P0MysqlUtil.getConnection();
            //2.Connection对象获取执行sql语句的Statement对象
            stmt = con.createStatement();
            //4.Statement对象执行sql语句,获取结果
            int i = stmt.executeUpdate(sql);
            //5.处理结果
            if (i==0){
                System.out.println("数据添加成功");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            //6.关闭资源
            C3P0SqlServerUtil.release(con, stmt, rs);
        }
    }
}
