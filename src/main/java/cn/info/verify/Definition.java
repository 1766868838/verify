package cn.info.verify;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class Definition {
    String[] basicDef;
    List<List<String>> colDef;
    List<List<String>> partDef;
    List<String> tableOptions;

    public Definition(String[] basicDef, List<List<String>> colDef, List<List<String>> partDef) {
        this.basicDef = basicDef;
        this.colDef = colDef;
        this.partDef = partDef;
    }

    public Definition() {
    }

    public Definition(int num){
        if(num == 1){
            String[] basicDef = {
                    "simple_table",
                    "test_db",
                    "CREATE TABLE user (id int primary key auto_increment, name varchar(50) not null)",
                    "InnoDB",
                    "utf8mb4",
            };
            // 创建列信息列表
            List<List<String>> colDef = Arrays.asList(
                    Arrays.asList("id", "int", "NOT NULL", "PRI", "", "auto_increment"),
                    Arrays.asList("name", "varchar(50)", "NOT NULL", "", "", "")
            );
            // 创建分区信息列表
            List<List<String>> partDef = Arrays.asList(
                    Arrays.asList("p0", "LESS THAN (100)", "100 rows"),
                    Arrays.asList("p1", "LESS THAN (200)", "80 rows")
            );
            List<String> tableOptions = Arrays.asList("ENGINE=InnoDB", "AUTO_INCREMENT=1", "DEFAULT", "CHARSET=utf8mb3");
            this.tableOptions = tableOptions;
            this.basicDef = basicDef;
            this.colDef = colDef;
            this.partDef = partDef;
        }
        if(num == 2){
            String[] basicDef = {
                    "simple_table",
                    "test_db",
                    "CREATE TABLE user (id int primary key auto_increment, name varchar(55) not null)",
                    "InnoDB",
                    "utf8mb4",
            };
            // 创建列信息列表
            List<List<String>> colDef = Arrays.asList(
                    Arrays.asList("id", "int", "NOT NULL", "PRI", "", "auto_increment"),
                    Arrays.asList("name", "varchar(55)", "NOT NULL", "", "", "")
            );
            // 创建分区信息列表
            List<List<String>> partDef = Arrays.asList(
                    Arrays.asList("p0", "LESS THAN (100)", "100 rows"),
                    Arrays.asList("p1", "LESS THAN (200)", "80 rows")
            );
            List<String> tableOptions = Arrays.asList("ENGINE=InnoDB", "AUTO_INCREMENT=1", "DEFAULT", "CHARSET=utf8mb4");
            this.tableOptions = tableOptions;
            this.basicDef = basicDef;
            this.colDef = colDef;
            this.partDef = partDef;
        }

    }
}
