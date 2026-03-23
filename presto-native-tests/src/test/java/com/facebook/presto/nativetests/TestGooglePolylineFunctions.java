package com.facebook.presto.nativetests;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

public class TestGooglePolylineFunctions
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testEncodePolyline() {
        assertQuery(
            "SELECT encode_polyline(ARRAY[38.5], ARRAY[-120.2])",
            "VALUES '_p~iF~ps|U'");
    }

    @Test
    public void testDecodePolyline() {
        assertQuery(
            "SELECT decode_polyline('_p~iF~ps|U')",
            "VALUES ROW(ARRAY[38.5], ARRAY[-120.2])");
    }

    @Test
    public void testRoundTrip() {
        assertQuery(
            "SELECT decode_polyline(encode_polyline(ARRAY[38.5, 40.7], ARRAY[-120.2, -120.95]))",
            "VALUES ROW(ARRAY[38.5, 40.7], ARRAY[-120.2, -120.95])");
    }
}