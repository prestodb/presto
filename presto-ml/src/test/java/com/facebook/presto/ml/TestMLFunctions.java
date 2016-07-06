package com.facebook.presto.ml;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

import com.facebook.presto.type.TypeRegistry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestMLFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    protected void registerFunctions()
    {
        registerScalar(TestMLFunctions.class);
    }



}