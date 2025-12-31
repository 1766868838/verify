package cn.infocore.dbs.compare.model;

import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Setter
@Getter
@Component
public class DbConnection implements Serializable {

    private static final long serialVersionUID = 1L;
    @Enumerated(EnumType.STRING)
    private DbCompare.DbType dbType;
    private String host;
    private int port;
    private String username;
    private String password;

}
