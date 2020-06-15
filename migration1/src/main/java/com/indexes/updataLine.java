package com.indexes;

import com.utils.C3P0MysqlUtil;
import com.utils.C3P0SqlServerUtil;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

public class updataLine {
    //Ind_cData开头的表中C列改为varchar(255)
    public static void main(String[] args) {
        //1.获取连接Connection对象
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            ArrayList<String> lineList = new ArrayList<>();
            ArrayList<String> databaseList = new ArrayList<>();

            con = C3P0MysqlUtil.getConnection();
            //2.Connection对象获取执行sql语句的Statement对象
            stmt = con.createStatement();
            //3.查询数据库中所有的表
            String tomSql ="select table_name tableName from information_schema.tables where table_schema='"+"yszx4"+"' and table_type='base table'";
            rs = stmt.executeQuery(tomSql);
            while (rs.next()) {
                String databasetableName = rs.getString("tableName");
                databaseList.add(databasetableName);
            }
            int ssdd = 0;

            for (String tablename : databaseList) {
                lineList.clear();
                if (tablename.startsWith("Ind_cData")){
                    ssdd++;
                    //查询表中所有列名
                    String sql = "SELECT COLUMN_NAME columnName FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'yszx4' AND TABLE_NAME = '"+tablename+"'";
                    rs = stmt.executeQuery(sql);
                    while (rs.next()) {
                        String name = rs.getString("columnName");
                        lineList.add(name);
                    }
                    for (String s : lineList) {
                        if (s.startsWith("C")){
                            System.out.println(tablename+"表"+s+"列修改成功");
                            String updatasql="alter table "+tablename+" modify column "+s+" varchar(255);";
                            int i = stmt.executeUpdate(updatasql);
                            //5.处理结果
                            if (i==0){
                                System.out.println(tablename+"表"+s+"列修改成功");
                            }

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
