/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.cdm.utils


import org.apache.spark.sql.catalyst.util.{DateTimeConstants, DateTimeUtils}

import java.text.ParseException
import java.time._
import java.time.format.DateTimeParseException
import java.time.temporal.{TemporalAccessor, TemporalQueries}
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.{Locale, TimeZone}
import scala.util.control.NonFatal

@SerialVersionUID(100L)
sealed trait TimestampFormatter extends Serializable {
  /**
   * Parses a timestamp in a string and converts it to microseconds.
   *
   * @param s - string with timestamp to parse
   * @return microseconds since epoch.
   * @throws ParseException can be thrown by legacy parser
   * @throws DateTimeParseException can be thrown by new parser
   * @throws DateTimeException unable to obtain local date or time
   */
  @throws(classOf[ParseException])
  @throws(classOf[DateTimeParseException])
  @throws(classOf[DateTimeException])
  def parse(s: String): Long
  def format(us: Long): String
}

class Iso8601TimestampFormatter(
    pattern: String,
    zoneId: ZoneId,
    locale: Locale) extends TimestampFormatter with DateTimeFormatterHelper {
  @transient
  private lazy val formatter = getOrCreateFormatter(pattern, locale)
  //private val NANOSECONDS = 1000L
  private def toInstant(s: String): Instant = {
    val temporalAccessor = formatter.parse(s)
    if (temporalAccessor.query(TemporalQueries.offset()) == null) {
      toInstantWithZoneId(temporalAccessor, zoneId)
    } else {
      Instant.from(temporalAccessor)
    }
  }
  def instantToMicros(instant: Instant): Long = {
    val us = Math.multiplyExact(instant.getEpochSecond, DateTimeConstants.MICROS_PER_SECOND)
    val result = Math.addExact(us, NANOSECONDS.toMicros(instant.getNano))
    result
  }

  private val specialValueRe = """(\p{Alpha}+)\p{Blank}*(.*)""".r
  private def today(zoneId: ZoneId): ZonedDateTime = {
    Instant.now().atZone(zoneId).`with`(LocalTime.MIDNIGHT)
  }
  def getZoneId(timeZoneId: String): ZoneId = ZoneId.of(timeZoneId, ZoneId.SHORT_IDS)
  /**
   * Extracts special values from an input string ignoring case.
   * @param input - a trimmed string
   * @param zoneId - zone identifier used to get the current date.
   * @return some special value in lower case or None.
   */
  private def extractSpecialValue(input: String, zoneId: ZoneId): Option[String] = {
    def isValid(value: String, timeZoneId: String): Boolean = {
      // Special value can be without any time zone
      if (timeZoneId.isEmpty) return true
      // "now" must not have the time zone field
      if (value.compareToIgnoreCase("now") == 0) return false
      // If the time zone field presents in the input, it must be resolvable
      try {
        getZoneId(timeZoneId)
        true
      } catch {
        case NonFatal(_) => false
      }
    }
    assert(input.trim.length == input.length)
    if (input.length < 3 || !input(0).isLetter) return None
    input match {
      case specialValueRe(v, z) if isValid(v, z) => Some(v.toLowerCase(Locale.US))
      case _ => None
    }
  }



  override protected def toZonedDateTime(
                                  temporalAccessor: TemporalAccessor,
                                  zoneId: ZoneId): ZonedDateTime = {
    // Parsed input might not have time related part. In that case, time component is set to zeros.
    val parsedLocalTime = temporalAccessor.query(TemporalQueries.localTime)
    val localTime = if (parsedLocalTime == null) LocalTime.MIDNIGHT else parsedLocalTime
    // Parsed input must have date component. At least, year must present in temporalAccessor.
    val localDate = temporalAccessor.query(TemporalQueries.localDate)

    ZonedDateTime.of(localDate, localTime, zoneId)
  }

  override def parse(s: String): Long = instantToMicros(toInstant(s))

  override def format(us: Long): String = {
    val secs = Math.floorDiv(us, DateTimeConstants.MICROS_PER_SECOND)
    val mos = Math.floorMod(us, DateTimeConstants.MICROS_PER_SECOND)
    val instant = Instant.ofEpochSecond(secs, mos * 1000L)

    formatter.withZone(zoneId).format(instant)
  }
}

object TimestampFormatter {
  val defaultPattern: String = "yyyy-MM-dd HH:mm:ss"
  val defaultLocale: Locale = Locale.US

  def apply(format: String, timeZone: TimeZone, locale: Locale): TimestampFormatter = {
    new Iso8601TimestampFormatter(format, timeZone.toZoneId, locale)
  }

  def apply(format: String, timeZone: TimeZone): TimestampFormatter = {
    apply(format, timeZone, defaultLocale)
  }

  def apply(timeZone: TimeZone): TimestampFormatter = {
    apply(defaultPattern, timeZone, defaultLocale)
  }
}
