package com.updataData;

import com.utils.C3P0MysqlUtil;
import com.utils.C3P0SqlServerUtil;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

public class updataData {
    //修改表Tsk_Sheet_Info中i_ScanMaxCol字段值
    public static void main(String[] args) {
        //1.获取连接Connection对象
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            ArrayList<String> dimTableNameList = new ArrayList<>();
            ArrayList<String> tablenameList = new ArrayList<>();
            Properties properties = new Properties();
            InputStream in = updataData.class.getClassLoader().getResourceAsStream("config.Properties");
            properties.load(in);
            String updataTableName = properties.getProperty("updataData.tableName");
            String updataLine = properties.getProperty("updataData.line");
            String databaseName = properties.getProperty("updataData.database");


            con = C3P0MysqlUtil.getConnection();
            //2.Connection对象获取执行sql语句的Statement对象
            stmt = con.createStatement();
            //3.查询数据库中所有的表名
            String tableNameListSql = "select table_name tableName from information_schema.tables where table_schema='" + databaseName + "' and table_type='base table'";
            rs = stmt.executeQuery(tableNameListSql);
            while (rs.next()) {
                String tableName1 = rs.getString("tableName");
                tablenameList.add(tableName1);
            }

            String[] arr = {"b", "c", "n", "r"};
            //查询表中的列
            String sql = "SELECT v_PeriodID,v_TaskCode,v_BookCode,i_BookLevel FROM " + updataTableName;
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String v_PeriodID = rs.getString("v_PeriodID")/*.substring(0, 4)*/;
                String v_TaskCode = rs.getString("v_TaskCode");
                String v_BookCode = rs.getString("v_BookCode")/*.replace("^", "")*/;
                String v_BookName = rs.getString("i_BookLevel");
                String ss = "";
                if (v_PeriodID.length()==4 && !v_BookCode.startsWith("^")){
                    ss = "Ind_@Data" + v_TaskCode + "_" + v_BookCode + "_" + v_BookName + "_" + v_PeriodID;
                    dimTableNameList.add(ss);
                }

            }
            for (String tableName : dimTableNameList) {
                int quantity=0;
                int quantity1;
                for (String s : arr) {
                    quantity1=0;
                    String tablename = tableName.replace("@", s);
                    if (tablenameList.contains(tablename)){
                        String sql1 ="SELECT COLUMN_NAME columnName FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '"+databaseName+"' AND TABLE_NAME = '"+tablename+"'";
                        rs = stmt.executeQuery(sql1);
                        while (rs.next()) {
                            String name = rs.getString("columnName");
                            if (name.startsWith("C")){
                                quantity1++;
                            }
                        }
                        if (quantity1>quantity){
                            quantity=quantity1;
                        }
                       // System.err.println(tablename+"表"+quantity);

                    }else {
                        System.out.println("数据库中没有"+tablename+"表");
                    }
                }
                //取出字段 Ind_@DataCZSZ001FJXB_001_0_2020
                String[] split = tableName.split("@");
                String dim = split[1];
                String[] s = dim.split("_");
                String v_TaskCode = s[0].substring(4);
                String v_BookCode = s[1];
                String i_BookLevel = s[2];
                String v_PeriodID = s[3];


                //修改数据就行了
                String updataMaxColSql="UPDATE "+updataTableName+" SET "+updataLine+"="+quantity +"  WHERE  v_PeriodID LIKE "+"'"+v_PeriodID+"%'" +" AND v_TaskCode='"+v_TaskCode+"' AND v_BookCode = '" +v_BookCode +"' AND i_BookLevel="+i_BookLevel+" OR ("+" v_PeriodID LIKE "+"'"+v_PeriodID+"%'" +" AND v_TaskCode='"+v_TaskCode+"' AND v_BookCode = "+"'^"+v_BookCode +"' AND i_BookLevel="+i_BookLevel+" )";
                System.out.println(updataMaxColSql);
                int i = stmt.executeUpdate(updataMaxColSql);
                System.out.println(i);
                if (i>0){
                    System.out.println("修改数据成功");
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //6.关闭资源
            C3P0SqlServerUtil.release(con, stmt, rs);
        }
    }
}
