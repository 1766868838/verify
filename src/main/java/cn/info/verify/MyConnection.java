package cn.info.verify;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MyConnection {
    private String host;
    private String port;
    private String username;
    private String password;
    private String database;

    public MyConnection(String host, String port, String username, String password, String database) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.database = database;
    }

     public MyConnection(){}
}
