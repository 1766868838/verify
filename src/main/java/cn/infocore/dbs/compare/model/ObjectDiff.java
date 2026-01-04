package cn.infocore.dbs.compare.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class ObjectDiff implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum ObjectType{
        TABLE, VIEW, FUNCTION, PROCEDURE, TRIGGER
    }

    private ObjectType type;

    /**
     * 对象名
     */
    private String name;

    /**
     * 结构差异
     */
    private boolean structure;

    /**
     * 内容差异，只有当type为TABLE时才比较content
     */
    private boolean content;

    /**
     * 差异描述，大概只有一句话
     */
    private String description;

    /**
     * 详情，可能是sql修复语句
     */
    private String detail;

    /**
     *
     * @param type
     * @param name
     * @param structure
     * @param content
     * @param description
     * @param detail
     */
    public ObjectDiff(ObjectType type, String name, boolean structure, boolean content, String description, String detail) {
        this.type = type;
        this.name = name;
        this.structure = structure;
        this.content = content;
        this.description = description;
        this.detail = detail;
    }

    public ObjectDiff(ObjectType type, String name, boolean structure, boolean content, String description) {
        this.type = type;
        this.name = name;
        this.structure = structure;
        this.content = content;
        this.description = description;
    }
}
