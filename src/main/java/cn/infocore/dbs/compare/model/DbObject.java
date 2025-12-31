package cn.infocore.dbs.compare.model;


import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class DbObject implements Serializable {

    private static final long serialVersionUID = 1L;

    List<String> tables;
    List<String> views;
    List<String> functions;
    List<String> procedures;
    List<String> triggers;
}
