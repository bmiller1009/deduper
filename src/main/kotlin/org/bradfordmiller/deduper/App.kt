package org.bradfordmiller.deduper

import org.bradfordmiller.deduper.config.Config
import org.slf4j.LoggerFactory

fun main(args: Array<String>) {

    val logger = LoggerFactory.getLogger("Main")

    val config = Config.ConfigBuilder()
        .sourceJndi("RealEstateIn")
        .sourceName("Sacramentorealestatetransactions")
        .jndiContext("default_ds")
        .hashColumns(mutableSetOf("street","city", "state", "zip", "price"))
        //.targetJndi("RealEstateOut")
        //.dupesJndi("RealEstateOutDupes")
        .build()

    val deduper = Deduper(config)

    val rpt = deduper.dedupe()

    logger.info(rpt.toString())
}