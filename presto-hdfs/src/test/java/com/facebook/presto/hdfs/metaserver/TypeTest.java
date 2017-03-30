package com.facebook.presto.hdfs.metaserver;

import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import org.testng.annotations.Test;

/**
 * presto-root
 *
 * @author guodong
 */
public class TypeTest
{
    @Test
    public void test()
    {
        VarcharType varcharType = VarcharType.createVarcharType(100);
        System.out.println(varcharType.getDisplayName());
        System.out.println(varcharType.toString());
        TypeSignature varcharSig = VarcharType.getParametrizedVarcharSignature("100");
        System.out.println(varcharSig.toString());
    }
}
