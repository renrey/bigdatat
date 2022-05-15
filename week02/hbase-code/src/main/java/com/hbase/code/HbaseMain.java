package com.hbase.code;

import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class HbaseMain {

    static String tableName = "junyuan:student";
    static List<String> columnFamilyNames = Arrays.asList("name", "info", "score");

    static String[] columnFamilyNameIndex = {"name", "info", "info", "score", "score"};
    static String[] columnNameIndex = {"name", "student_id", "class", "understanding", "programming"};
    // 使用的数据
    static String[][] data = {
            {"Tom", "20210000000001", "1", "75", "82"},
            {"Jerry", "20210000000002", "1", "85", "67"},
            {"Jack", "20210000000003", "2", "80", "80"},
            {"Rose", "20210000000004", "2", "60", "61"},
            {"junyuan", "G20190343010170", "3", "100", "81"}
    };

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "emr-worker-2.cluster-285604,emr-worker-1.cluster-285604,emr-header-1.cluster-285604");
//        config.set("hbase.zookeeper.quorum", "d1:2181,d2:2181,d3:2181");

        config.set("hbase.zookeeper.property.clientPort", "2181");
        // 参数1 是操作类型
        String op = "insert";
        if (args.length > 0) {
            op = args[1];
        }
        switch (op) {
            // 查询单条数据
            case "query" :
                query(config, args);
                break;
            // 批量插入预设数据
            case "insert":
                addData(config);
                break;
            // 删除单挑数据
            case "delete":
                deleteData(config, args);
                break;
            // 建表
            case "createTable":
                createTable(config);
                break;
            // 删表
            case "deleteTable":
                dropTable(config);
                break;
            // 查询所有
            case "scan":
                scanAll(config);
                break;
            default:
                query(config, args);
        }

    }


    static void createTable(Configuration configuration) {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();
            // 不存在namespace,就创建
            boolean exist = Arrays.stream(admin.listNamespaces()).anyMatch(v -> "junyuan".equalsIgnoreCase(v));
            if (!exist) {
                admin.createNamespace(NamespaceDescriptor.create("junyuan").build());
            }
            // 表名
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

            // 设置列族
            List<ColumnFamilyDescriptor> columnFamilyDescriptorList = columnFamilyNames.stream().map(ColumnFamilyDescriptorBuilder::of).collect(Collectors.toList());
            tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptorList);
            // 执行建表
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println(String.join("", "create table ", tableName, " success"));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(String.join("", "create table ", tableName, " fail"));
        }
    }

    static void dropTable(Configuration configuration) {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Admin admin = connection.getAdmin();
            // 先disable
            admin.disableTable(TableName.valueOf(tableName));
            // 在删除
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println(String.join("", "delete table ", tableName, " success"));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(String.join("", "delete table ", tableName, " fail"));
        }
    }

    static Put createPut(String[] data) {
        // rowkey名
        Put put = new Put(Bytes.toBytes(data[0])); // rowkey
        for (int i = 0; i < data.length; i++) {
            put.addColumn(Bytes.toBytes(columnFamilyNameIndex[i]), Bytes.toBytes(columnNameIndex[i]), Bytes.toBytes(data[i]));
        }
        return put;
    }

    static void addData(Configuration configuration) {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(String.join("", "insert data", tableName, " fail"));
        }
    }

    static void deleteData(Configuration configuration, String[] args) {

        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            // 执行删除
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete de = new Delete(Bytes.toBytes("junyuan"));
            table.delete(de);
            System.out.println(String.join("", "delete data junyuan", tableName, " success"));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(String.join("", "delete data junyuan", tableName, " fail"));
        }
    }

    static void query(Configuration configuration, String[] args) {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes("junyuan"));
            // 执行查询
            Result result = table.get(get);
            System.out.println("query data junyuan succes, data:");
            // 打印所有字段值
            System.out.println("rowkey:" + Bytes.toString(result.getRow()));
            for (int i = 0; i < columnFamilyNameIndex.length; i++) {
                String family = columnFamilyNameIndex[i];
                String name = columnNameIndex[i];
                String value = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(name)));
                System.out.println("columnFamily:" + family + ", name:" + name + ", value:" + value);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(String.join("", "query data junyuan", tableName, " fail"));
        }
    }

    static void scanAll(Configuration configuration) {
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(TableName.valueOf(tableName));

            // 执行查询
            ResultScanner scanner = table.getScanner(new Scan());
            for (Result result : scanner) {
                // 打印所有字段值
                System.out.println("rowkey:" + Bytes.toString(result.getRow()));
                for (int i = 0; i < columnFamilyNameIndex.length; i++) {
                    String family = columnFamilyNameIndex[i];
                    String name = columnNameIndex[i];
                    String value = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(name)));
                    System.out.println("columnFamily:" + family + ", name:" + name + ", value:" + value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(String.join("", "scan ", tableName, " fail"));
        }
    }

}
