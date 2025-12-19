package cn.info.verify;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@SpringBootTest
class VerifyApplicationTests {

    @Test
    void contextLoads() {
        try{
            Connection conn1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/test3","root","infocore");
            Connection conn2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/test4","root","infocore");

            String table1= "user";
            String table2= "user";

            VerifyClient verifyClient = new VerifyClient();
            //System.out.println(verifyClient.Verify(conn1,conn2,table1,table2));

            conn1.close();
            conn2.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

}
