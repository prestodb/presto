package com.facebook.presto.baseplugin;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

/**
 * Created by amehta on 6/13/16.
 */
public class BaseUtils {
    private BaseUtils()
    {}

    public static <T> T getValueAsType(Object value, Class<T> clazz){
        String v = value.toString();
        if (clazz == String.class){
            value = v;
        } else if (clazz == Double.class) {
            value = Double.parseDouble(v);
        }
        else if (clazz == Integer.class) {
            value = Integer.parseInt(v);
        }
        else if (clazz == Long.class) {
            try {
                value = Long.parseLong(v);
            }catch (NumberFormatException e){// in the event of a date string
                value = LocalDateTime.parse(v, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toEpochSecond(ZoneOffset.UTC);
            }
        }
        else if (clazz == Boolean.class) {
            value = Boolean.parseBoolean(v);
        }
        return clazz.cast(value);
    }

    public static <T> T getPropertyFromMap(String propertyName, Class<T> clazz, Map<Object,Object> properties){
        return properties.containsKey(propertyName) ? getValueAsType(properties.get(propertyName), clazz) : null;
    }

    public static <T> T getPropertyFromSessionConfig(String propertyName, Class<T> clazz, ConnectorSession session, BaseConfig config){
        Optional<Object> value = Optional.ofNullable(session.getProperty(propertyName.replace(".", "_"), Object.class));
        return value.isPresent() ? getValueAsType(value.get(), clazz) : config.getProperty(propertyName, clazz, Optional.empty());
    }

    public static Type getTypeForObject(Object o){
        if(o.getClass() == String.class){
            return VarcharType.VARCHAR;
        }
        else if(o.getClass() == Double.class){
            return DoubleType.DOUBLE;
        }
        else if(o.getClass() == Integer.class){
            return IntegerType.INTEGER;
        }
        else if(o.getClass() == Long.class){
            return BigintType.BIGINT;
        }
        else if(o.getClass() == Boolean.class){
            return BooleanType.BOOLEAN;
        }
        return null;
    }

}
