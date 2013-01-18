package com.facebook.presto.spi;

public class SchemaField
{
    public enum Category
    {
        PRIMITIVE
    }

    public enum Type
    {
        LONG, DOUBLE, STRING
    }

    private final String fieldName;
    private final int fieldId;
    private final Category category;
    private final Type primitiveType;

    private SchemaField(String fieldName, int fieldId, Category category, Type primitiveType)
    {
        this.fieldName = fieldName;
        this.fieldId = fieldId;
        this.category = category;
        this.primitiveType = primitiveType;
    }

    public String getFieldName()
    {
        return fieldName;
    }

    public int getFieldId()
    {
        return fieldId;
    }

    public Category getCategory()
    {
        return category;
    }

    public Type getPrimitiveType()
    {
        if (category != Category.PRIMITIVE) {
            throw new IllegalStateException("category is not PRIMITIVE: " + category);
        }
        return primitiveType;
    }

    @Override
    public String toString()
    {
        return "SchemaField{" +
                "fieldName='" + fieldName + '\'' +
                ", fieldId=" + fieldId +
                ", category=" + category +
                ", primitiveType=" + primitiveType +
                '}';
    }

    public static SchemaField createPrimitive(String fieldName, int fieldId, Type primitiveType)
    {
        return new SchemaField(fieldName, fieldId, Category.PRIMITIVE, primitiveType);
    }
}
