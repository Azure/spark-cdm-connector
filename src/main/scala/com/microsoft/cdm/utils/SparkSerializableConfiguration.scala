package com.microsoft.cdm.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import scala.util.control.NonFatal

@SerialVersionUID(100L)
class SparkSerializableConfiguration(@transient var value: Configuration) extends Serializable {

  def getFileSystem() : FileSystem = {
    FileSystem.get(value)
  }

  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }

  def tryOrIOException(block: => Unit) {
    try {
      block
    } catch {
      case e: IOException => {
        throw e
      }
      case NonFatal(t) => {
        throw new IOException(t)
      }
    }
  }
}