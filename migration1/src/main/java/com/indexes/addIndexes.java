package com.indexes;

import com.utils.C3P0MysqlUtil;
import com.utils.C3P0SqlServerUtil;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

public class addIndexes {
    //添加索引
    public static void main(String[] args) {
        //1.获取连接Connection对象
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            ArrayList<String> lineList = new ArrayList<>();
            ArrayList<String> databaseList = new ArrayList<>();
            //读取文件
            Properties properties = new Properties();
            InputStream in = addIndexes.class.getClassLoader().getResourceAsStream("config.Properties");
            properties.load(in);
            String database = properties.getProperty("addIndexes.database");
            String tableNameList = properties.getProperty("addIndexes.tableNameList");
            String indexesNameList = properties.getProperty("addIndexes.indexesNameList");
            String[] tableNamelist = tableNameList.split(",");
            String[] indexesName = indexesNameList.split(",");
            String indexess="";
            for (String indexes : indexesName) {
                indexess+=(indexes+",");
            }
            indexess = indexess.substring(0, indexess.lastIndexOf(","));
            System.err.println(indexess+"=========");

            con = C3P0MysqlUtil.getConnection();
            //2.Connection对象获取执行sql语句的Statement对象
            stmt = con.createStatement();
            //3.查询数据库中所有的表
            String tomSql ="select table_name tableName from information_schema.tables where table_schema='"+database+"' and table_type='base table'";
            rs = stmt.executeQuery(tomSql);
            while (rs.next()) {
                String databasetableName = rs.getString("tableName");
                databaseList.add(databasetableName);
            }
            int ssdd = 0;

            for (String tablename : databaseList) {
                for (String tableName : tableNamelist) {
                    lineList.clear();
                     if (tablename.startsWith(tableName.replace(" ",""))) {
                        System.err.println(tablename);
                       // String sql ="select column_name columnName from information_schema.COLUMNS where table_name='"+tableName+"'";
                          String sql ="SELECT COLUMN_NAME columnName FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '"+database+"' AND TABLE_NAME = '"+tablename+"'";
                         rs = stmt.executeQuery(sql);
                         while (rs.next()) {
                             String name = rs.getString("columnName");
                             lineList.add(name);
                         }
                         boolean contain = true;
                         for (String s : indexesName) {
                             boolean contains = lineList.contains(s);
                             if (contains==false){
                                 contain=false;
                                 break;
                             }
                         }
                         if (contain){
                             ssdd++;
                             System.out.println(ssdd);
                             //添加索引
                             String addindexesSql = "ALTER TABLE "+tablename+" ADD INDEX index_name ("+indexess+")";
                             //删除索引
                            // String addindexesSql = "alter table "+tablename+" drop index index_name";
                             int i = stmt.executeUpdate(addindexesSql);
                             //5.处理结果
                             if (i==0){
                                 System.out.println(tablename+"表添加索引成功");
                             }
                         }else {
                             System.out.println(tablename+"表中没有列"+indexess);
                         }


                    }

                }
            }
            System.out.println(ssdd);



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //6.关闭资源
            C3P0SqlServerUtil.release(con, stmt, rs);
        }
    }
}
