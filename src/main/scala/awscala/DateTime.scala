package awscala

import java.time
import java.time.{ Clock, OffsetDateTime, ZoneId }

object DateTime {

  /**
   * @return Current [[DateTime]].
   */
  def now() = OffsetDateTime.now()

  /**
   * @param zoneId [[ZoneId]] to use.
   * @return Current [[DateTime]] at given [[ZoneId]].
   */
  def now(zoneId: ZoneId) = OffsetDateTime.now(zoneId)

  /**
   * @param clock [[Clock]] to use.
   * @return Current [[DateTime]] for given [[Clock]].
   */
  def now(clock: Clock) = OffsetDateTime.now(clock)

  /**
   * @param str String containing default formatting of [[DateTime]] type.
   * @return [[DateTime]] of string.
   */
  def parse(str: String): DateTime = OffsetDateTime.parse(str)

  /**
   * @param str String containing some form of [[DateTime]] formatting.
   * @param formatter [[java.time.format.DateTimeFormatter]] to use to parse the given string.
   * @return [[DateTime]] of string.
   */
  def parse(str: String, formatter: time.format.DateTimeFormatter): DateTime = OffsetDateTime.parse(str, formatter)

  /**
   * @param jud The [[java.util.Date]] to convert into a [[DateTime]] type.
   * @return [[DateTime]] using system default [[ZoneId]].
   */
  def from(jud: java.util.Date): DateTime = from(jud, ZoneId.systemDefault())

  /**
   * @param jud The [[java.util.Date]] to convert into a [[DateTime]] type.
   * @param zoneId The [[ZoneId]] to use to create a [[DateTime]] instance.
   * @return [[DateTime]] using given [[ZoneId]].
   */
  def from(jud: java.util.Date, zoneId: ZoneId): DateTime = if (jud == null) OffsetDateTime.now(zoneId) else OffsetDateTime.ofInstant(jud.toInstant, zoneId)

}

