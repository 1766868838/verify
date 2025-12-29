package cn.infocore.dbs.compare.model;


import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class DbObject {

    List<String> tables;
    List<String> views;
    List<String> functions;
    List<String> procedures;
    List<String> triggers;
}
