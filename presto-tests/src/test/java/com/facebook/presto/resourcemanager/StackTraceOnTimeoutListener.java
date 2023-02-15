package com.facebook.presto.resourcemanager;

import com.facebook.airlift.log.Logger;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;
import org.testng.internal.thread.ThreadTimeoutException;

public class StackTraceOnTimeoutListener
        implements ITestListener
{
    private final Logger log = Logger.get(StackTraceOnTimeoutListener.class);

    @Override
    public void onTestStart(ITestResult result)
    { }

    @Override
    public void onTestSuccess(ITestResult result)
    { }

    @Override
    public void onTestFailure(ITestResult result)
    {
        if (result.getThrowable() instanceof ThreadTimeoutException) {
            log.error("Thread timed out, stack trace: ");
            log.error(result.getThrowable());
        }
    }

    @Override
    public void onTestSkipped(ITestResult result)
    { }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result)
    { }

    @Override
    public void onStart(ITestContext context)
    { }

    @Override
    public void onFinish(ITestContext context)
    { }
}
