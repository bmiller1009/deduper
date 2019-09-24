package org.bradfordmiller.deduper.utils

import org.slf4j.LoggerFactory
import java.io.*
import java.nio.file.FileSystemException

class FileUtils {
    companion object {

        val logger = LoggerFactory.getLogger(FileUtils::class.java)

        fun prepFile(targetName: String, columns: Set<String>, extension: String, delimiter: String, deleteIfExists: Boolean) {
            val fileName = "$targetName.$extension"

            val f = File(fileName)

            if(f.exists() && !f.isFile)
                throw FileSystemException("tgt name $fileName is not a file")

            if(deleteIfExists) {
                logger.info("deleteIfExists is set to true, deleting file $fileName before continuing.")
                f.delete()
            }

            BufferedWriter(OutputStreamWriter(FileOutputStream(fileName), "utf-8")).use { bw ->
                bw.write(columns.joinToString(separator=delimiter,postfix="\n"))
            }
        }
        fun writeStringsToFile(strings: List<String>, file: String, extension: String) {
            val fileName = "$file.$extension"
            BufferedWriter(FileWriter(fileName, true)).use { bw ->
                strings.forEach {
                    bw.write(it + "\n")
                }
            }
        }
    }
}