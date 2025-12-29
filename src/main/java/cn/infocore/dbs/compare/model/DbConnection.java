package cn.infocore.dbs.compare.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

@Setter
@Getter
@Component
public class DbConnection {
    private String dbType;
    private String host;
    private int port;
    private String username;
    private String password;

}
