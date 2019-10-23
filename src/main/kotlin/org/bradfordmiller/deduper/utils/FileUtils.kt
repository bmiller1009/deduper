package org.bradfordmiller.deduper.utils

import com.opencsv.CSVWriter
import com.opencsv.ICSVParser.DEFAULT_ESCAPE_CHARACTER
import com.opencsv.ICSVWriter.DEFAULT_LINE_END
import com.opencsv.ICSVWriter.NO_QUOTE_CHARACTER
import org.slf4j.LoggerFactory
import java.io.*
import java.nio.charset.Charset
import java.nio.file.*

/**
 * A utility library for file operations
 */
class FileUtils {
    companion object {

        val logger = LoggerFactory.getLogger(FileUtils::class.java)

        /**
         * writes a list of [data] to a [fileName] with a specified [delimiter] and [so] standard open option
         */
        private fun writeRowsToFile(fileName: String, delimiter: Char, data: Array<String>, so: OpenOption) {
            Files.newBufferedWriter(
                Paths.get(fileName), Charset.forName("utf-8"), StandardOpenOption.CREATE, so
            ).use { bw ->
                CSVWriter(bw, delimiter, NO_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER, DEFAULT_LINE_END).use { csvw ->
                    csvw.writeNext(data)
                }
            }
        }
        /**
         * creates a file named [targetNamd] with [columns], and settings for the file [extension] and [delimiter].
         * [deleteIfExists] will determine if the file is deleted if it already exists before creating
         */
        fun prepFile(
            targetName: String,
            columns: Set<String>,
            extension: String,
            delimiter: String,
            deleteIfExists: Boolean
        ) {
            val fileName = "$targetName.$extension"

            val f = File(fileName)

            if(f.exists() && !f.isFile)
                throw FileSystemException("tgt name $fileName is not a file")

            if(deleteIfExists) {
                logger.info("deleteIfExists is set to true, deleting file $fileName before continuing.")
                f.delete()
            }
            writeRowsToFile(fileName, delimiter.single(), columns.toTypedArray(), StandardOpenOption.TRUNCATE_EXISTING)
        }
        /**
         * writes a list of [strings] to a [file] with specified [extension] and [delimiter]
         */
        fun writeStringsToFile(strings: Array<String>, file: String, extension: String, delimiter: String) {
            val fileName = "$file.$extension"
            writeRowsToFile(fileName, delimiter.single(), strings, StandardOpenOption.APPEND)
        }
        /**
         *  writes a list of [strings] lists to a [file] with specified [extension] and [delimiter]
         */
        fun writeStringsToFile(strings: Array<Array<String>>, file: String, extension: String, delimiter: String) {
            strings.forEach{s ->
                writeStringsToFile(s, file, extension, delimiter)
            }
        }
    }
}