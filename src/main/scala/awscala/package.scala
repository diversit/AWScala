package object awscala {
  // Workaround for https://issues.scala-lang.org/browse/SI-7139
  val Region: Region0.type = Region0
  type Region = com.amazonaws.regions.Region
  type DateTime = java.time.OffsetDateTime
  type ByteBuffer = java.nio.ByteBuffer
  type File = java.io.File

  /**
   * Implicit conversion of [[DateTime]] type to a [[java.util.Date]].
   *
   * @param dt [[DateTime]] to convert.
   */
  implicit class DateTimeToJavaDate(val dt: DateTime) extends AnyVal {
    /**
     * @return [[java.util.Date]] instance of [[DateTime]].
     */
    def toDate: java.util.Date = java.util.Date.from(dt.toInstant)
  }

}

