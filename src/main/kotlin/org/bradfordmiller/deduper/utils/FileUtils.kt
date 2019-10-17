package org.bradfordmiller.deduper.utils

import com.opencsv.CSVWriter
import com.opencsv.ICSVParser.DEFAULT_ESCAPE_CHARACTER
import com.opencsv.ICSVWriter.DEFAULT_LINE_END
import com.opencsv.ICSVWriter.NO_QUOTE_CHARACTER
import org.slf4j.LoggerFactory
import java.io.*
import java.nio.charset.Charset
import java.nio.file.*

class FileUtils {
    companion object {

        val logger = LoggerFactory.getLogger(FileUtils::class.java)

        private fun writeRowsToFile(fileName: String, delimiter: Char, data: Array<String>, so: OpenOption) {
            Files.newBufferedWriter(Paths.get(fileName), Charset.forName("utf-8"), StandardOpenOption.CREATE, so).use { bw ->
                CSVWriter(bw, delimiter, NO_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER, DEFAULT_LINE_END).use { csvWriter ->
                    csvWriter.writeNext(data)
                }
            }
        }
        fun prepFile(targetName: String, columns: Set<String>, extension: String, delimiter: String, deleteIfExists: Boolean) {
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
        fun writeStringsToFile(strings: Array<String>, file: String, extension: String, delimiter: String) {
            val fileName = "$file.$extension"
            writeRowsToFile(fileName, delimiter.single(), strings, StandardOpenOption.APPEND)
        }
        fun writeStringsToFile(strings: Array<Array<String>>, file: String, extension: String, delimiter: String) {
            strings.forEach{s ->
                writeStringsToFile(s, file, extension, delimiter)
            }
        }
    }
}