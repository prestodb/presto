package com.facebook.presto.spark.launcher;

import com.facebook.presto.spark.classloader_interface.PrestoSparkSession;

public interface PrestoSparkSessionFactory
{
    PrestoSparkSession createSession(PrestoSparkClientOptions clientOptions);
}
