package com.facebook.presto.example.prac;

import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @Author LTR
 * @Date 2025/5/7 16:19
 * @注释
 */
public class ExamplePracColumn {
    private final String name;

    private final Type type;

//todo:check the value null or not
    @JsonCreator
    public ExamplePracColumn(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }
}
