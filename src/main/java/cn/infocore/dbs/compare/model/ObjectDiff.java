package cn.infocore.dbs.compare.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ObjectDiff {

    private enum ObjectType{
        TABLE, VIEW, FUNCTION, PROCEDURE, TRIGGER
    }

    private String name;

    private boolean structure;

    /**
     * 只有当type为TABLE时才比较content
     */
    private boolean content;

    private String detail;

    public ObjectDiff(String name, boolean structure, boolean content, String detail) {
        this.name = name;
        this.structure = structure;
        this.content = content;
        this.detail = detail;
    }
}
