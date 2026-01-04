package cn.infocore.dbs.compare.converter;

import cn.infocore.dbs.compare.model.DbObject;
import cn.infocore.dbs.compare.model.DbResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Convert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Convert
public class DbResultConverter implements AttributeConverter<DbResult,String> {

    private final ObjectMapper objectMapper =  new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(DbResult dbResult) {
        if (dbResult == null) return null;
        try {
            return objectMapper.writeValueAsString(dbResult);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DbResult convertToEntityAttribute(String objStr) {
        if (objStr == null || objStr.isEmpty()) return null;
        try {
            return objectMapper.readValue(objStr, DbResult.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
