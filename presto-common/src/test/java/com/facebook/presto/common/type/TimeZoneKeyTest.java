package com.facebook.presto.common.type;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class TimeZoneKeyTest {
  @Test
  public void testEtcGmt() {
    // write tests for TimeZoneKey.normalizeZoneId(), use only etc/gmt

    // test for etc/gmt+1
    String timeZoneKey = TimeZoneKey.normalizeZoneId("etc/gmt+1");
    assertEquals(timeZoneKey, "+01:00");

    // test for etc/gmt-1
    timeZoneKey = TimeZoneKey.normalizeZoneId("etc/gmt-1");
    assertEquals(timeZoneKey, "-01:00");

    // test for etc/gmt+14
    timeZoneKey = TimeZoneKey.normalizeZoneId("etc/gmt+14");
    assertEquals(timeZoneKey, "+14:00");

    // test for etc/gmt-14
    timeZoneKey = TimeZoneKey.normalizeZoneId("etc/gmt-14");
    assertEquals(timeZoneKey, "-14:00");

    // test for eTc/gmt+01
    timeZoneKey = TimeZoneKey.normalizeZoneId("eTc/gmt+01");
    assertEquals(timeZoneKey, "+01:00");
  }

  @Test
  public void testEtcUtc() {
    // test for etc/gmt+1
    String timeZoneKey = TimeZoneKey.normalizeZoneId("etc/utc+1");
    assertEquals(timeZoneKey, "-01:00");

    // test for etc/utc-1
    timeZoneKey = TimeZoneKey.normalizeZoneId("etc/utc-1");
    assertEquals(timeZoneKey, "+01:00");

    // test for etc/utc+14
    timeZoneKey = TimeZoneKey.normalizeZoneId("etc/utc+14");
    assertEquals(timeZoneKey, "-14:00");

    // test for etc/utc-14
    timeZoneKey = TimeZoneKey.normalizeZoneId("etc/utc-14");
    assertEquals(timeZoneKey, "+14:00");
  }

  @Test
  public void testIncorrectValueEtc() {
    String timeZoneKey = TimeZoneKey.normalizeZoneId("ETC/+06:00");
    assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.getTimeZoneKey(timeZoneKey));
  }

  @Test
  public void testIncorrectValueEtcUtc() {
    String timeZoneKey = TimeZoneKey.normalizeZoneId("ETC/UTC*");
    assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.getTimeZoneKey(timeZoneKey));
  }

  @Test
  public void testEtcGreenwich() {
    // test for etc/greenwich
    String timeZoneKey = TimeZoneKey.normalizeZoneId("etc/greenwich+1");
    assertEquals(timeZoneKey, "+01:00");
  }

  @Test
  public void testEtcUniversal() {
    // test for etc/universal
    String timeZoneKey = TimeZoneKey.normalizeZoneId("etc/universal+1");
    assertEquals(timeZoneKey, "+01:00");
  }

  @Test
  public void testEtcZulu() {
    // test for etc/zulu
    String timeZoneKey = TimeZoneKey.normalizeZoneId("etc/zulu+1");
    assertEquals(timeZoneKey, "+01:00");
  }

  @Test
  public void testEtcUCT() {
    // test for etc/UCT
    String timeZoneKey = TimeZoneKey.normalizeZoneId("etc/UCT+1");
    assertEquals(timeZoneKey, "+01:00");
  }
}