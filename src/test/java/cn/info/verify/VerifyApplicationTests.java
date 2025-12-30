package cn.info.verify;

import cn.infocore.dbs.compare.VerifyApplication;
import cn.infocore.dbs.compare.model.DbConnection;
import cn.infocore.dbs.compare.model.dto.DbCompareDto;
import cn.infocore.dbs.compare.service.impl.DbCompareServiceImpl;
import cn.infocore.dbs.compare.verify.VerifyClient;
import cn.infocore.dbs.compare.verify.VerifyClient1;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.xdevapi.PreparableStatement;
import jakarta.persistence.Column;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SpringBootTest(classes = VerifyApplication.class)
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

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void dbTest() throws SQLException {
        String definition_query = """
              SELECT %s FROM INFORMATION_SCHEMA.%s WHERE %s
        """;
        String _COLUMN_QUERY = """
              SELECT ORDINAL_POSITION, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE,
                     COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT, COLUMN_KEY
              FROM INFORMATION_SCHEMA.COLUMNS
              WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'
        """;
        // 这后面还有分区的查询语句，暂时先不做
        String column = "TABLE_SCHEMA, TABLE_NAME, ENGINE, AUTO_INCREMENT, " +
                "AVG_ROW_LENGTH, CHECKSUM, TABLE_COLLATION, TABLE_COMMENT, ROW_FORMAT, CREATE_OPTIONS";
        String condition = "TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'";


        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306","root","infocore");

        String basicDef = "";
        try(PreparedStatement statement = connection.prepareStatement(
                String.format(definition_query, column, "TABLES", String.format(condition, "test3", "user")));
        ResultSet set = statement.executeQuery();){
            while(set.next()){
                basicDef = set.getString("TABLE_SCHEMA")+" "+set.getString("TABLE_NAME")+" "+set.getString("ENGINE")+" "+set.getString("AUTO_INCREMENT")+" "+set.getString("AVG_ROW_LENGTH")
                        +" "+set.getString("CHECKSUM")+" "+set.getString("TABLE_COLLATION")+" "+set.getString("TABLE_COMMENT")+" "+set.getString("ROW_FORMAT")+" "+set.getString("CREATE_OPTIONS");
                System.out.println(set.getString("TABLE_SCHEMA")+" "+set.getString("TABLE_NAME")+" "+set.getString("ENGINE")+" "+set.getString("AUTO_INCREMENT")+" "+set.getString("AVG_ROW_LENGTH")
                        +" "+set.getString("CHECKSUM")+" "+set.getString("TABLE_COLLATION")+" "+set.getString("TABLE_COMMENT")+" "+set.getString("ROW_FORMAT")+" "+set.getString("CREATE_OPTIONS"));
            }
        }

        try(PreparedStatement statement = connection.prepareStatement(
                String.format(_COLUMN_QUERY, "test3", "user"));
        ResultSet set = statement.executeQuery();){
            List<Column> colDef = new ArrayList<>();

            while(set.next()){
                colDef.add(new Column(set.getInt("ORDINAL_POSITION"), set.getString("COLUMN_NAME"),
                        set.getString("COLUMN_TYPE"), set.getString("IS_NULLABLE"),
                        set.getString("COLUMN_DEFAULT"), set.getString("COLUMN_KEY")));
            }
        }

    }


    @Autowired
    DbCompareServiceImpl service;

    @Test
    void test() throws SQLException {
        DbCompareDto dbCompareDto = new DbCompareDto();
        DbConnection dbConnection1 = new DbConnection();
        dbConnection1.setDbType("MYSQL");
        dbConnection1.setHost("localhost");
        dbConnection1.setPort(3306);
        dbConnection1.setUsername("root");
        dbConnection1.setPassword("infocore");
        dbCompareDto.setSourceDb(dbConnection1);
        dbCompareDto.setTargetDb(dbConnection1);

        service.start(dbCompareDto);

    }
    @Getter
    @Setter
    public class Column{
        private int ORDINAL_POSITION;
        private String COLUMN_NAME;
        private String COLUMN_TYPE;
        private String IS_NULLABLE;
        private String COLUMN_DEFAULT;
        private String COLUMN_KEY;

        public Column(int ORDINAL_POSITION, String COLUMN_NAME, String COLUMN_TYPE, String IS_NULLABLE, String COLUMN_DEFAULT, String COLUMN_KEY) {
            this.ORDINAL_POSITION = ORDINAL_POSITION;
            this.COLUMN_NAME = COLUMN_NAME;
            this.COLUMN_TYPE = COLUMN_TYPE;
            this.IS_NULLABLE = IS_NULLABLE;
            this.COLUMN_DEFAULT = COLUMN_DEFAULT;
            this.COLUMN_KEY = COLUMN_KEY;
        }
    }
}
