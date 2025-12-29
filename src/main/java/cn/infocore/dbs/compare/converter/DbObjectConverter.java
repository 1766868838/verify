package cn.infocore.dbs.compare.converter;

import cn.infocore.dbs.compare.model.DbObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Convert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Convert
public class DbObjectConverter implements AttributeConverter<DbObject,String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(DbObject dbCompare) {
        if (dbCompare == null) return null;
        try {
            return objectMapper.writeValueAsString(dbCompare);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DbObject convertToEntityAttribute(String objStr) {
        if (objStr == null || objStr.isEmpty()) return null;
        try {
            return objectMapper.readValue(objStr, DbObject.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
