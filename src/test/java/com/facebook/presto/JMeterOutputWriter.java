package com.facebook.presto;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Encode sample data in JMeter test output format
 */
public class JMeterOutputWriter
{
    private static final String HEADER =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<testResults version=\"1.2\">\n";
    private static final String FOOTER = "</testResults>\n";
    private final OutputStream outputStream;

    public JMeterOutputWriter(OutputStream outputStream)
    {
        this.outputStream = Preconditions.checkNotNull(outputStream, "outputStream is null");
        try {
            outputStream.write(HEADER.getBytes(Charsets.UTF_8));
        } catch (IOException e) {
            Throwables.propagate(e);
        }
    }

    public JMeterOutputWriter addSample(Sample sample) {
        Preconditions.checkNotNull(sample, "sample is null");
        try {
            outputStream.write(
                    String.format(
                            "<sample lb=\"%s\" ts=\"%d\" t=\"%d\" s=\"%b\" />\n",
                            StringEscapeUtils.escapeXml(sample.getLabel()), sample.getTimeStamp(), sample.getElapsedTime(), sample.isSuccess()
                    ).getBytes(Charsets.UTF_8)
            );
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return this;
    }

    public void finished() {
        try {
            outputStream.write(FOOTER.getBytes(Charsets.UTF_8));
        } catch (IOException e) {
            Throwables.propagate(e);
        }
    }


    public static class Sample {
        private final String label;
        private final long timeStamp;
        private final long elapsedTime;
        private final boolean success;

        public Sample(String label, long timeStamp, long elapsedTime, boolean success)
        {
            this.label = Preconditions.checkNotNull(label, "label is null");
            this.timeStamp = timeStamp;
            this.elapsedTime = elapsedTime;
            this.success = success;
        }

        public String getLabel()
        {
            return label;
        }

        public long getTimeStamp()
        {
            return timeStamp;
        }

        public long getElapsedTime()
        {
            return elapsedTime;
        }

        public boolean isSuccess()
        {
            return success;
        }
    }
}
