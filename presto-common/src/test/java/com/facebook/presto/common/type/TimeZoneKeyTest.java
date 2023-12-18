/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.common.type;

 import org.testng.annotations.Test;

 import javax.inject.Named;

 import static org.testng.Assert.*;

 public class TimeZoneKeyTest {

   @Test
   @Named("GMT TEST")
   public void gmtZoneIdTest() {
     String zoneId = "etc/gmt+8";
     String normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "+08:00");

     zoneId = "etc/gmt-08:20";
     normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "-08:20");
   }

   @Test
   @Named("UTC TEST")
   public void utcZoneIdTest() {
     String zoneId = "etc/utc+1";
     String normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "-01:00");

     zoneId = "etc/utc-1";
     normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "+01:00");
   }

   @Test
   @Named("GREENWICH TEST")
   public void greenwichZoneIdTest() {
     String zoneId = "etc/greenwich+01:00";
     String normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "+01:00");

     zoneId = "etc/greenwich+6";
     normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "+06:00");

     zoneId = "etc/greenwich-6";
     normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "-06:00");

     zoneId = "etc/greenwich+11:23";
     normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "+11:23");
   }

   @Test
   @Named("UNIVERSAL TEST")
   public void universalZoneIdTest() {
     String zoneId = "etc/universal+08:00";
     String normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "+08:00");

     zoneId = "etc/universal-08:00";
     normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "-08:00");

     zoneId = "etc/universal+11:23";
     normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "+11:23");
   }

   @Test
   @Named("UCT TEST")
   public void uctZoneIdTest() {
     String zoneId = "etc/uct+06:00";
     String normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "+06:00");

     zoneId = "etc/uct-10:10";
     normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "-10:10");

     zoneId = "etc/uct+1";
     normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "+01:00");
   }

   @Test
   @Named("ZULU TEST")
   public void zuluZoneIdTest() {
     String zoneId = "etc/zulu+06:00";
     String normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "+06:00");

     zoneId = "etc/zulu-10:10";
     normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "-10:10");

     zoneId = "etc/zulu+1";
     normalizedZoneId = TimeZoneKey.normalizeZoneId(zoneId);
     assertEquals(normalizedZoneId, "+01:00");
   }

   @Test
   @Named("EXCEPTION TEST")
   public void exceptionZoneIdTest() {
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("GMT-13:00"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("etc/6"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("etc/*"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("etc/"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("etc/gmt+24:00"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("etc/gmt+01"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("etc/gmt+01:000"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("etc/utc+27"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("etc/utc-13"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("etc/gmt-16"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("etc/greenwich+01"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("etc/universal+01"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("fsdkjflksdflsdlfj"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("ETC/+06:00"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("ETC/06:00"));
     assertThrows(TimeZoneNotSupportedException.class, () -> TimeZoneKey.normalizeZoneId("ETC/+6"));
   }
 }