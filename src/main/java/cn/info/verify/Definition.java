package cn.info.verify;

import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class Definition {
    String[] basicDef;
    List<String[]> colDef;
    List<String[]> partDef;

    public Definition(String[] basicDef, List<String[]> colDef, List<String[]> partDef) {
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
            List<String[]> colDef = Arrays.asList(
                    new String[]{"id", "int", "NOT NULL", "PRI", "", "auto_increment"},
                    new String[]{"name", "varchar(50)", "NOT NULL", "", "", ""}
            );
            // 创建分区信息列表
            List<String[]> partDef = Arrays.asList(
                    new String[]{"p0", "LESS THAN (100)", "100 rows"},
                    new String[]{"p1", "LESS THAN (200)", "80 rows"}
            );
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
            List<String[]> colDef = Arrays.asList(
                    new String[]{"id", "int", "NOT NULL", "PRI", "", "auto_increment"},
                    new String[]{"name", "varchar(55)", "NOT NULL", "", "", ""}
            );
            // 创建分区信息列表
            List<String[]> partDef = Arrays.asList(
                    new String[]{"p0", "LESS THAN (100)", "100 rows"},
                    new String[]{"p1", "LESS THAN (200)", "80 rows"}
            );
            this.basicDef = basicDef;
            this.colDef = colDef;
            this.partDef = partDef;
        }

    }
}
