package org.bradfordmiller.deduper.config

import org.bradfordmiller.deduper.csv.CsvConfigParser
import org.bradfordmiller.deduper.persistors.*

class Config() {

    internal var targetPersistor: TargetPersistor? = null
    internal var dupePersistor: DupePersistor? = null
    internal var srcJndi: String = ""
    internal var srcName: String = ""
    internal var context: String = ""
    internal var keyOn: Set<String> = setOf()

    private constructor(srcJndi: String,
                        srcName: String,
                        context: String,
                        keyOn: Set<String>): this() {

        this.srcJndi = srcJndi
        this.srcName = srcName
        this.context = context
        this.keyOn = keyOn
    }
    constructor(srcJndi: String,
                srcName: String,
                context: String,
                tgtJndi: String,
                dupesJndi: String,
                keyOn: Set<String> = setOf()): this(srcJndi, srcName, context, keyOn) {

        val tgtConfigMap = CsvConfigParser.getCsvMap(context, tgtJndi)
        val dupesConfigMap = CsvConfigParser.getCsvMap(context, dupesJndi)
        targetPersistor = CsvTargetPersistor(tgtConfigMap)
        dupePersistor = CsvDupePersistor(dupesConfigMap)

    }
    constructor(srcJndi: String,
                srcName: String,
                context: String,
                tgtJndi: String,
                tgtTable: String,
                dupesJndi: String,
                keyOn: Set<String> = setOf()): this(srcJndi, srcName, context, keyOn) {

        targetPersistor = SqlTargetPersistor(tgtTable, tgtJndi, context)
        dupePersistor = SqlDupePersistor(dupesJndi, context)
    }
}