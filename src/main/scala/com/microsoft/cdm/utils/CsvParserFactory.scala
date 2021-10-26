package com.microsoft.cdm.utils

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings, CsvWriter, CsvWriterSettings}

import java.io.OutputStreamWriter
/**
 * Builds Univocity CsvParser instances.
 */
object CsvParserFactory {
  def build(delimiter: Char): CsvParser = {
    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setDelimiter(delimiter)
    settings.setLineSeparatorDetectionEnabled(true)
    settings.setMaxCharsPerColumn(-1)
    settings.setMaxColumns(512 * 4)
    new CsvParser(settings)
  }

  def buildWriter(outputWriter: OutputStreamWriter, delimiter: Char): CsvWriter = {
    val settings = new CsvWriterSettings()
    settings.getFormat.setDelimiter(delimiter)
    settings.getFormat.setLineSeparator("\n")
    settings.setMaxCharsPerColumn(-1)
    new CsvWriter(outputWriter, settings)
  }
}
