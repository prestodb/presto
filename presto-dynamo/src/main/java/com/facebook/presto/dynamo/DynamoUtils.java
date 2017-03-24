package com.facebook.presto.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.facebook.presto.baseplugin.BaseUtils;
import com.facebook.presto.baseplugin.predicate.BaseComparisonOperator;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

import java.util.EnumSet;

/**
 * Created by amehta on 7/25/16.
 */
public final class DynamoUtils {
    private DynamoUtils(){}

    public static final EnumSet<BaseComparisonOperator> HASH_OPERATORS = EnumSet.of(BaseComparisonOperator.EQUAL, BaseComparisonOperator.IN);
    public static final EnumSet<BaseComparisonOperator> RANGE_OPERATORS = EnumSet.of(BaseComparisonOperator.EQUAL, BaseComparisonOperator.BETWEEN, BaseComparisonOperator.GT, BaseComparisonOperator.GTE, BaseComparisonOperator.LT, BaseComparisonOperator.LTE);

    public static Type getTypeForDynamoType(String type){
        switch (type.toLowerCase()){
            case "s":
                return VarcharType.VARCHAR;
            case "n":
                return DoubleType.DOUBLE;
            case "long":
                return BigintType.BIGINT;
            case "b":
                return BooleanType.BOOLEAN;
            default:
                return null;
        }
    }

    public static AttributeValue getAttributeValueForObject(Object o){
        Type type = BaseUtils.getTypeForObject(o);
        AttributeValue value = new AttributeValue();
        if(type == VarcharType.VARCHAR){
            value.withS(o.toString());
        }else if (type == IntegerType.INTEGER || type == DoubleType.DOUBLE || type == BigintType.BIGINT){
            value.withN(o.toString());
        }
        return value;
    }

    public static ComparisonOperator fromBaseComparisonOperator(BaseComparisonOperator comparisonOperator){
        switch (comparisonOperator){
            case IN:
                return ComparisonOperator.IN;
            case EQUAL:
                return ComparisonOperator.EQ;
            case BETWEEN:
                return ComparisonOperator.BETWEEN;
            case GT:
                return ComparisonOperator.GT;
            case GTE:
                return ComparisonOperator.GE;
            case LT:
                return ComparisonOperator.LT;
            case LTE:
                return ComparisonOperator.LE;
            default:
                return null;
        }
    }
}
