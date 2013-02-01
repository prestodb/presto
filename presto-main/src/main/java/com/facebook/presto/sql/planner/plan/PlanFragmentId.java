package com.facebook.presto.sql.planner.plan;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.KeyDeserializer;
import org.codehaus.jackson.map.SerializerProvider;

import javax.annotation.concurrent.Immutable;

import java.io.IOException;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class PlanFragmentId
{
    private final String id;

    @JsonCreator
    public PlanFragmentId(String id)
    {
        checkNotNull(id, "id is null");
        this.id = id;
    }

    @JsonValue
    public String toString()
    {
        return id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PlanFragmentId that = (PlanFragmentId) o;

        if (!id.equals(that.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }
}
