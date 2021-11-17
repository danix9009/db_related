package com.che300.db.benchmark.sqlparser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

/**
 * @Title
 * @Description
 * @Author hdan
 * @Since 2021/11/17
 * @See https://www.cnblogs.com/etangyushan/p/5490183.html
 */
public class DruidDemo {

    public static void main(String[] args) {
        // String sql = "update t set name = 'x' where id < 100 limit 10";
        // String sql = "SELECT ID, NAME, AGE FROM USER WHERE ID = ? limit 2";
         String sql = "CREATE TABLE t1\n" +
                 "(\n" +
                 "    EventDate DateTime,\n" +
                 "    CounterID UInt32,\n" +
                 "    UserID UInt32,\n" +
                 "    ver UInt16\n" +
                 ") ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{layer}-{shard}/table_name', '{replica}', ver)\n" +
                 "PARTITION BY toYYYYMM(EventDate)\n" +
                 "ORDER BY (CounterID, EventDate, intHash32(UserID))\n" +
                 "SAMPLE BY intHash32(UserID);\n";

//        String sql = "select user from emp_table";
        DbType dbType = JdbcConstants.CLICKHOUSE;

        //格式化输出
        String result = SQLUtils.format(sql, dbType);
        System.out.println(result); // 缺省大写格式
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        //解析出的独立语句的个数
        System.out.println("size is:" + stmtList.size());
        for (int i = 0; i < stmtList.size(); i++) {

            SQLStatement stmt = stmtList.get(i);
            SchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            stmt.accept(visitor);

            //获取表名称
            System.out.println("Tables : " + visitor.getTables());
            //获取操作方法名称,依赖于表名称
            System.out.println("Manipulation : " + visitor.getDbType());
            //获取字段名称
            System.out.println("fields : " + visitor.getColumns());
        }
    }
}
