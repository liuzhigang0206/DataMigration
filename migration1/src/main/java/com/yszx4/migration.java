package com.yszx4;

import com.utils.C3P0SqlServerUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class migration {
    //创建数据库中所有的表结构和所有的数据
    private static Logger logger =Logger.getLogger(migration.class); // 获取logger实例
    public static void main(String[] args) {
        //有表是删除后在建表
        //查主键都是id吗？
        mysql mysql = new mysql();
        //1.获取连接Connection对象
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = C3P0SqlServerUtil.getConnection();
            //2.Connection对象获取执行sql语句的Statement对象
            stmt = con.createStatement();
            //3.查询数据库中所有的表
            String tomSql = "SELECT TABLE_NAME tableName,TABLE_SCHEMA tableSchema FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'";
            //4.Statement对象执行sql语句,获取结果
            rs = stmt.executeQuery(tomSql);
            HashSet<String> primaryKeylist = new HashSet<>();
            ArrayList<String> structureList = new ArrayList<>();
            HashMap<String, String> tableNameMap = new HashMap<>();
            String primary = "";
            String primary1 = "";
            int tableCount = 0;
            int countSum = 0;
            //5.处理结果
            while (rs.next()) {
                String tableName = rs.getString("tableName");
                //dbo或guest
                String tableSchema = rs.getString("tableSchema");
                tableNameMap.put(tableName, tableSchema);
            }
            int size = tableNameMap.size();
            logger.info("数据库一共有"+size+"张表");
            StringBuilder stringBuilder = new StringBuilder();
            //for (String tableName : tableNamelist) {
            long startTime  = System.currentTimeMillis();
            for (String tableName : tableNameMap.keySet()) {
                tableCount++;
                primaryKeylist.clear();
                primary = "";
                structureList.clear();
                //查询表中主键
                String primaryKey = "select b.column_name COLUMN_NAME from information_schema.table_constraints a inner join information_schema.constraint_column_usage b on a.constraint_name = b.constraint_name where a.constraint_type = 'PRIMARY KEY' and a.table_name = " + "'" + tableName + "'";
                //String primaryKey = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME= " + "'" + tableName + "'" + " GROUP BY COLUMN_NAME";
                rs = stmt.executeQuery(primaryKey);
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    primaryKeylist.add(columnName);
                }
                primary = "";
                primary1 = "";
                if (primaryKeylist.size() > 0) {
                    primary1 += " primary key(";
                    for (String s : primaryKeylist) {
                        primary += s + ",";
                        primary1 += (s + ",");
                    }
                    primary1 = primary1.substring(0, primary1.lastIndexOf(",")) + ")";
                    primary = primary.substring(0, primary.lastIndexOf(","));
                }

                //获取表结构
                String construction = "select COLUMN_NAME name,DATA_TYPE type,IS_NULLABLE isnull,CHARACTER_MAXIMUM_LENGTH length from information_schema.COLUMNS where TABLE_NAME=" + "'" + tableName + "'";
                rs = stmt.executeQuery(construction);
                stringBuilder.setLength(0);
                stringBuilder.append("CREATE TABLE If Not Exists ").append(tableName).append("(");
                if (tableName.startsWith("Ind_cData")){
                    while (rs.next()) {
                        String name = rs.getString("name");
                        structureList.add(name);
                        String type = rs.getString("type");
                        String isnull = rs.getString("isnull");
                        int length = rs.getInt("length");
                        stringBuilder.append(name).append(" ");
                        if (name.startsWith("C")){
                            stringBuilder.append("varchar(255)");
                            if ("NO".equals(isnull)) {
                                stringBuilder.append(" NOT NULL");
                                if ("".equals(primary)) {
                                    primary = name;
                                }
                            }
                            stringBuilder.append(",");
                        }else {
                            if ("money".equals(type)) {
                                stringBuilder.append("decimal(25,4)");
                            } else if ("smallmoney".equals(type)) {
                                stringBuilder.append(" decimal(10,4) ");
                            } else if ("uniqueidentifier".equals(type)) {
                                stringBuilder.append(" varchar(40) ");
                            }else if ("xml".equals(type)) {
                                stringBuilder.append(" text ");
                            } else if ("image".equals(type)) {
                                stringBuilder.append("blob");
                            } else if ("nchar".equals(type)) {
                                stringBuilder.append("char");
                            }  else if ("real".equals(type)) {
                                stringBuilder.append("float");
                            }  else if ("datetime2".equals(type)) {
                                stringBuilder.append(" datetime ");
                            }else if ("numeric".equals(type)) {
                                stringBuilder.append(" decimal ");
                            } else if ("datetimeoffset".equals(type)) {
                                stringBuilder.append(" datetime ");
                            } else if ("nvarchar".equals(type)) {
                                stringBuilder.append("varchar").append("(").append(length).append(")");
                            } else if ("smalldatetime".equals(type)) {
                                stringBuilder.append("datetime");
                            } else if ("ntext".equals(type)) {
                                stringBuilder.append("text");
                            }else if ("bit".equals(type)) {
                                stringBuilder.append("tinyint");
                            } else {
                                stringBuilder.append(type);
                            }
                            if ("varchar".equals(type)) {
                                stringBuilder.append("(").append(length).append(")");
                            }
                            if ("varbinary".equals(type)) {
                                stringBuilder.append("(").append(length).append(")");
                            }
                            if ("nchar".equals(type)) {
                                if (length > 255) {
                                    stringBuilder.append("(").append(255).append(")");
                                } else {
                                    stringBuilder.append("(").append(length).append(")");
                                }
                            }
                            if ("char".equals(type)) {
                                if (length > 255) {
                                    stringBuilder.append("(").append(255).append(")");
                                } else {
                                    stringBuilder.append("(").append(length).append(")");
                                }

                            }

                            if ("NO".equals(isnull)) {
                                stringBuilder.append(" NOT NULL");
                                if ("".equals(primary)) {
                                    primary = name;
                                }
                            }
                            stringBuilder.append(",");

                        }

                    }
                }else {
                    while (rs.next()) {
                        String name = rs.getString("name");
                        structureList.add(name);
                        String type = rs.getString("type");
                        String isnull = rs.getString("isnull");
                        int length = rs.getInt("length");
                        stringBuilder.append(name).append(" ");
                        if ("money".equals(type)) {
                            stringBuilder.append("decimal(25,4)");
                        } else if ("smallmoney".equals(type)) {
                            stringBuilder.append(" decimal(10,4) ");
                        } else if ("uniqueidentifier".equals(type)) {
                            stringBuilder.append(" varchar(40) ");
                        }else if ("xml".equals(type)) {
                            stringBuilder.append(" text ");
                        } else if ("image".equals(type)) {
                            stringBuilder.append("blob");
                        } else if ("nchar".equals(type)) {
                            stringBuilder.append("char");
                        }  else if ("real".equals(type)) {
                            stringBuilder.append("float");
                        }  else if ("datetime2".equals(type)) {
                            stringBuilder.append(" datetime ");
                        }else if ("numeric".equals(type)) {
                            stringBuilder.append(" decimal ");
                        } else if ("datetimeoffset".equals(type)) {
                            stringBuilder.append(" datetime ");
                        } else if ("nvarchar".equals(type)) {
                            stringBuilder.append("varchar").append("(").append(length).append(")");
                        } else if ("smalldatetime".equals(type)) {
                            stringBuilder.append("datetime");
                        } else if ("ntext".equals(type)) {
                            stringBuilder.append("text");
                        }else if ("bit".equals(type)) {
                            stringBuilder.append("tinyint");
                        } else {
                            stringBuilder.append(type);
                        }
                        if ("varchar".equals(type)) {
                            stringBuilder.append("(").append(length).append(")");
                        }
                        if ("varbinary".equals(type)) {
                            stringBuilder.append("(").append(length).append(")");
                        }
                        if ("nchar".equals(type)) {
                            if (length > 255) {
                                stringBuilder.append("(").append(255).append(")");
                            } else {
                                stringBuilder.append("(").append(length).append(")");
                            }
                        }
                        if ("char".equals(type)) {
                            if (length > 255) {
                                stringBuilder.append("(").append(255).append(")");
                            } else {
                                stringBuilder.append("(").append(length).append(")");
                            }

                        }

                        if ("NO".equals(isnull)) {
                            stringBuilder.append(" NOT NULL");
                            if ("".equals(primary)) {
                                primary = name;
                            }
                        }
                        stringBuilder.append(",");

                    }
                }

                stringBuilder.append(primary1);
                if (primaryKeylist.size() == 0) {
                    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                }
                stringBuilder.append(")");
                //调用创建表结构方法---------
                logger.info("创建表建构sql语句"+stringBuilder.toString());
                long l = System.currentTimeMillis();
                mysql.structure(tableName, stringBuilder.toString());
                long l1 = System.currentTimeMillis();
                logger.info("创建表"+tableName+"用时"+(l1-l)+"毫秒");
                String result = "";
                for (String s : structureList) {
                    result += s + ",";
                }
                result = result.substring(0, result.length() - 1);
                if ("".equals(primary)) {
                    primary = result;
                }
                //查询数据总数
                String countSql = "select count(*) amount from " + tableNameMap.get(tableName) + "." + tableName;
                rs = stmt.executeQuery(countSql);
                while (rs.next()) {
                    countSum = rs.getInt("amount");
                }
                logger.info(tableName+"数据表一共有"+countSum+"条数据");
                int countData = countSum;
                if (countSum > 0) {
                    String sql = "";
                    int number = 0;
                    if (countSum <= 500) {
                        long l2 = System.currentTimeMillis();
                        sql = "";
                        sql = "select " + result + " from " + tableNameMap.get(tableName) + "." + tableName + "  order by " + primary + " offset 0 rows fetch next  " + countSum + "  rows only";
                        //sql = "SELECT TOP   "+ countSum +" "+ result + "  FROM  " + tableNameMap.get(tableName) + "." + tableName + " WHERE   " + primary + " NOT IN ( SELECT TOP  " +number+" "+ primary + " FROM   " + tableNameMap.get(tableName) + "." + tableName + "  ORDER BY " + primary + " )  ORDER BY " + primary;
                        rs = stmt.executeQuery(sql);
                        String insertSql = "insert into " + tableName + " values ";
                        while (rs.next()) {
                            insertSql += "(";
                            for (int clunms = 1; clunms <= structureList.size(); clunms++) {
                                Object data = rs.getObject(clunms);
                                if (clunms != structureList.size()) {

                                    if (data instanceof Boolean) {
                                        insertSql += data + ",";
                                    } else if (data == null) {
                                        insertSql += null + ",";
                                    } else if (data == "") {
                                        insertSql += " " + ",";
                                    } else {
                                        insertSql += "\"" + data + "\",";
                                    }

                                } else {
                                    if (data instanceof Boolean) {
                                        insertSql += data;
                                    } else if (data == null) {
                                        insertSql += null;
                                    } else if (data == "") {
                                        insertSql += " ";
                                    } else {
                                        insertSql += "\"" + data + "\"";
                                    }
                                }

                            }
                            insertSql += "),";

                        }
                        insertSql = insertSql.substring(0, insertSql.lastIndexOf(",")) + ";";
                        //添加数据----------
                        mysql.add(insertSql);
                        long l3 = System.currentTimeMillis();
                        logger.info("迁移"+tableName+"表中数据用时"+(l3-l2)+"毫秒");
                        logger.info("迁移"+tableName+"表一共用时"+(l3-l)+"毫秒");

                    } else {
                        while (countSum >= 500) {
                            long l2 = System.currentTimeMillis();
                            sql = "";
                            // sql = "SELECT TOP 500  " + result + "  FROM  " + tableNameMap.get(tableName) + "." + tableName + " WHERE   " + primary + " NOT IN ( SELECT TOP  " +number+" "+ primary + " FROM   " + tableNameMap.get(tableName) + "." + tableName + "  ORDER BY " + primary + " )  ORDER BY " + primary;
                            sql = "select " + result + " from " + tableNameMap.get(tableName) + "." + tableName + "  order by " + primary + " offset " + number + " rows fetch next  " + 500 + "  rows only";

                            number += 500;
                            countSum -= 500;
                            System.out.println(sql);
                            rs = stmt.executeQuery(sql);
                            String insertSql = "insert into " + tableName + " values ";
                            while (rs.next()) {
                                insertSql += "(";
                                for (int clunms = 1; clunms <= structureList.size(); clunms++) {
                                    Object data = rs.getObject(clunms);
                                    if (clunms != structureList.size()) {

                                        if (data instanceof Boolean) {
                                            insertSql += data + ",";
                                        } else if (data == null) {
                                            insertSql += null + ",";
                                        } else if (data == "") {
                                            insertSql += " " + ",";
                                        } else {
                                            insertSql += "\"" + data + "\",";
                                        }

                                    } else {
                                        if (data instanceof Boolean) {
                                            insertSql += data;
                                        } else if (data == null) {
                                            insertSql += null;
                                        } else if (data == "") {
                                            insertSql += " ";
                                        } else {
                                            insertSql += "\"" + data + "\"";
                                        }
                                    }

                                }
                                insertSql += "),";

                            }
                            System.err.println(insertSql);
                            insertSql = insertSql.substring(0, insertSql.lastIndexOf(",")) + ";";
                            System.out.println(insertSql);
                            //添加数据----------
                            mysql.add(insertSql);
                            long l3 = System.currentTimeMillis();
                            logger.info("迁移"+tableName+"表数据从第"+(number-500)+"到"+number+"条数据用时"+(l3-l2)+"毫秒");
                        }
                        if (countSum > 0) {

                            long l2 = System.currentTimeMillis();
                            sql = "";
                            sql = "select " + result + " from " + tableNameMap.get(tableName) + "." + tableName + "  order by " + primary + " offset " + number + " rows fetch next  " + 500 + "  rows only";

                            System.out.println(sql);
                            rs = stmt.executeQuery(sql);
                            String insertSql = "insert into " + tableName + " values ";
                            while (rs.next()) {
                                insertSql += "(";
                                for (int clunms = 1; clunms <= structureList.size(); clunms++) {
                                    Object data = rs.getObject(clunms);
                                    if (clunms != structureList.size()) {
                                        if (data instanceof Boolean) {
                                            insertSql += data + ",";
                                        } else if (data == null) {
                                            insertSql += null + ",";
                                        } else if (data == "") {
                                            insertSql += " " + ",";
                                        } else {
                                            insertSql += "\"" + data + "\",";
                                        }

                                    } else {
                                        if (data instanceof Boolean) {
                                            insertSql += data;
                                        } else if (data == null) {
                                            insertSql += null;
                                        } else if (data == "") {
                                            insertSql += " ";
                                        } else {
                                            insertSql += "\"" + data + "\"";
                                        }
                                    }

                                }
                                insertSql += "),";

                            }
                            System.err.println(insertSql);
                            insertSql = insertSql.substring(0, insertSql.lastIndexOf(",")) + ";";
                            System.out.println(insertSql);
                            //添加数据----------
                            mysql.add(insertSql);
                            long l3 = System.currentTimeMillis();
                            logger.info("迁移"+tableName+"表数据从第"+number+"到"+countData+"条数据用时"+(l3-l2)+"毫秒");
                        }
                    }
                }
                long l2 = System.currentTimeMillis();
                logger.info("迁移"+tableName+"表总用时"+(l2-l)+"毫秒");
                logger.info(tableName+"迁移完成");


            }
            long endTime = System.currentTimeMillis();
            logger.info("数据库一共"+size+"张表,完成迁移"+tableCount+"张表,一共用时"+(endTime-startTime)+"毫秒");


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            //6.关闭资源
            C3P0SqlServerUtil.release(con, stmt, rs);
        }

    }

}
