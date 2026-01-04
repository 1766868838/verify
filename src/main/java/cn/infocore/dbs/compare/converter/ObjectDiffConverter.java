package cn.infocore.dbs.compare.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Convert;

import java.util.List;

@Convert
public class ObjectDiffConverter implements AttributeConverter<List, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(List diff) {
        if (diff == null) return null;
        try {
            return objectMapper.writeValueAsString(diff);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List convertToEntityAttribute(String objStr) {
        if (objStr == null || objStr.isEmpty()) return null;
        try {
            return objectMapper.readValue(objStr, List.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
