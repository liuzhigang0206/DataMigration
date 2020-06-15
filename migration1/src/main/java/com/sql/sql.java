package com.sql;

import com.google.common.collect.Lists;
import com.utils.C3P0MysqlUtil;
import com.utils.C3P0SqlServerUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class sql {
    public static void main(String[] args) {
        //1.获取连接Connection对象
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            con = C3P0MysqlUtil.getConnection();
            File file = new File("D:\\aa\\youjian\\sql\\ss.sql");

            batchExecute(con, readSqlList(file));


        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            C3P0SqlServerUtil.release(con, stmt, rs);
        }


    }

    /**
     * 将文件中的sql语句以；为单位读取到列表中
     * @param sqlFile /
     * @return /
     * @throws Exception e
     */
    private static List<String> readSqlList(File sqlFile) throws Exception {
        List<String> sqlList = Lists.newArrayList();
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(sqlFile), StandardCharsets.UTF_8))) {
            String tmp;
            while ((tmp = reader.readLine()) != null) {
              //  log.info("line:{}", tmp);
                if (tmp.endsWith(";")) {
                    sb.append(tmp);
                    sqlList.add(sb.toString());
                    sb.delete(0, sb.length());
                } else {
                    sb.append(tmp);
                }
            }
            if (!"".endsWith(sb.toString().trim())) {
                sqlList.add(sb.toString());
            }
        }

        return sqlList;
    }
    /**
     * 批量执行sql
     * @param connection /
     * @param sqlList /
     */
    public static void batchExecute(Connection connection, List<String> sqlList) throws SQLException {
        Statement st = connection.createStatement();
        for (String sql : sqlList) {
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            st.addBatch(sql);
        }
        st.executeBatch();
    }


}
