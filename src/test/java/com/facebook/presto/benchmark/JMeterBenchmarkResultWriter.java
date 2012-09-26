package com.facebook.presto.benchmark;

import com.facebook.presto.JMeterOutputWriter;
import org.joda.time.DateTimeUtils;

import java.io.OutputStream;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Encodes benchmark results in terms of a compatible JMeter output format for tracking purposes.
 * Currently this only allows for one result type, and so that type must be explicitly selected.
 */
public class JMeterBenchmarkResultWriter
        implements BenchmarkResultHook
{
    private final String selectedResultName;
    private final JMeterOutputWriter JMeterOutputWriter;

    public JMeterBenchmarkResultWriter(String selectedResultName, OutputStream outputStream)
    {
        this.selectedResultName = checkNotNull(selectedResultName, "selectedResult is null");
        this.JMeterOutputWriter = new JMeterOutputWriter(checkNotNull(outputStream, "outputStream is null"));
    }

    @Override
    public BenchmarkResultHook addResults(Map<String, Long> results)
    {
        Long result = results.get(selectedResultName);
        if (result != null) {
            // Encode the benchmark result where
            // label: use an arbitrary constant as all the results are uniform
            // timeStamp: use the current time in millis
            // elapsedTime: encode the test result in this field for graphing purposes
            // success: always true since a benchmark result was successfully created
            JMeterOutputWriter.addSample(new JMeterOutputWriter.Sample(selectedResultName, DateTimeUtils.currentTimeMillis(), result, true));
        }
        return this;
    }

    @Override
    public void finished()
    {
        JMeterOutputWriter.finished();
    }
}
