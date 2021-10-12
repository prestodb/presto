package com.facebook.presto.catalog;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * @author ahern
 * @date 2021/10/12 10:46
 * @since 1.0
 */
public class JsonUtil {

    private final static ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        MAPPER.configure(JsonParser.Feature.ALLOW_MISSING_VALUES, true);
        MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }
    private final static Logger logger = LoggerFactory.getLogger(JsonUtil.class);

    /**
     * 使用泛型方法，把json字符串转换为相应的JavaBean对象。
     * (1)转换为普通JavaBean：readValue(json,Student.class)
     * (2)转换为List,如List<Student>,将第二个参数传递为Student
     * [].class.然后使用Arrays.asList();方法把得到的数组转换为特定类型的List
     *
     * @param jsonStr
     * @param valueType
     * @return
     */
    public static <T> T readValue(String jsonStr, Class<T> valueType) {
        try {
            return MAPPER.readValue(jsonStr, valueType);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    /**
     * json数组转List
     * @param jsonStr
     * @param valueTypeRef
     * @return
     */
    public static <T> T readValue(String jsonStr, TypeReference<T> valueTypeRef){
        try {
            return MAPPER.readValue(jsonStr, valueTypeRef);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    /**
     * 把JavaBean转换为json字符串
     *
     * @param object
     * @return
     */
    public static String toJson(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }
    /**
     * 把JavaBean转换为Map
     *
     * @param pojo
     * @return
     */
    @SuppressWarnings("rawtypes")
    public static Map convert2Map(Object pojo) {
        if (Objects.isNull(pojo)) {
            return Maps.newHashMap();
        }
        return MAPPER.convertValue(pojo, Map.class);
    }
}
