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
            Connection conn1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/test1","root","infocore");
            Connection conn2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/test2","root","infocore");
//            Connection conn1 = DriverManager.getConnection("jdbc:mysql://localhost:13307/test1","root","123456");
//            Connection conn2 = DriverManager.getConnection("jdbc:mysql://localhost:13308/test2","root","123456");

            String table1= "user";
            String table2= "user";

            VerifyClient verifyClient = new VerifyClient();
            System.out.println(verifyClient.Verify(conn1,conn2,table1,table2));

            //System.out.println(verifyClient.CompareDb(conn1,conn2));

            conn1.close();
            conn2.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    void dbTest() {

        try(            Connection conn1 = DriverManager.getConnection("jdbc:mysql://localhost:3306/test3","root","infocore");
                        Connection conn2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/test4","root","infocore"))
        {
//            MyConnection myConnection1 = new MyConnection("localhost","3306","root","infocore","test3");
//            MyConnection myConnection2 = new MyConnection("localhost","3306","root","infocore","test4");
            VerifyClient1 verifyClient = new VerifyClient1();
            System.out.println(verifyClient.CompareDb(conn1,conn2,conn1.getCatalog(),conn2.getCatalog()));
            //System.out.println(verifyClient.Verify(conn1,conn2,"user","user"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    void test() throws SQLException {
        DbCompare dbCompare = new DbCompare();
        System.out.println(dbCompare.databaseCompare("1","2","test3","test4",null));
    }

}
