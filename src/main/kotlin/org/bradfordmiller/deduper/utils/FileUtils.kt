package org.bradfordmiller.deduper.utils

import java.io.*
import java.nio.file.FileSystemException

class FileUtils {
    companion object {
        fun prepFile(targetName: String, columns: Set<String>, extension: String, delimiter: String) {
            val fileName = targetName + "." + extension

            val f = File("$fileName")

            if(f.exists() && !f.isFile)
                throw FileSystemException("tgt name $fileName is not a file")

            BufferedWriter(OutputStreamWriter(FileOutputStream(fileName), "utf-8")).use { bw ->
                bw.write(columns.joinToString(separator=delimiter,postfix="\n"))
            }
        }
        fun writeStringsToFile(strings: List<String>, file: String, extension: String) {
            val fileName = file + "." + extension
            BufferedWriter(FileWriter(fileName, true)).use { bw ->
                strings.forEach {
                    bw.write(it + "\n")
                }
            }
        }
    }
}