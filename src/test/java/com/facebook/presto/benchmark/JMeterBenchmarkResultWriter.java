package com.facebook.presto.benchmark;

import com.facebook.presto.JMeterOutputWriter;
import com.google.common.base.Preconditions;
import org.joda.time.DateTimeUtils;

import java.io.OutputStream;

/**
 * Encodes benchmark results in terms of a compatible JMeter output format for tracking purposes
 */
public class JMeterBenchmarkResultWriter
    implements BenchmarkResultHook
{
    private final com.facebook.presto.JMeterOutputWriter JMeterOutputWriter;

    public JMeterBenchmarkResultWriter(OutputStream outputStream)
    {
        this.JMeterOutputWriter = new JMeterOutputWriter(Preconditions.checkNotNull(outputStream, "outputStream is null"));
    }

    @Override
    public BenchmarkResultHook addResult(long result)
    {
        // Encode the benchmark result where
        // label: use an arbitrary constant as all the results are uniform
        // timeStamp: use the current time in millis
        // elapsedTime: encode the test result in this field for graphing purposes
        // success: always true since a benchmark result was successfully created
        JMeterOutputWriter.addSample(new JMeterOutputWriter.Sample("default", DateTimeUtils.currentTimeMillis(), result, true));
        return this;
    }

    @Override
    public void finished()
    {
        JMeterOutputWriter.finished();
    }
}
