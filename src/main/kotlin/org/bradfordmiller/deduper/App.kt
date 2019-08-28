package org.bradfordmiller.deduper

import org.bradfordmiller.deduper.config.Config
import org.slf4j.LoggerFactory

fun main(args: Array<String>) {

    val logger = LoggerFactory.getLogger("Main")

    val config = Config(
            "RealEstateIn",
            "Sacramentorealestatetransactions",
            "default_ds",
            "SqlLiteTest",
            "real_estate",
            "SqlLiteTest",
            mutableSetOf("street","city", "state", "zip", "price")
    )

    val deduper = Deduper(config)

    val rpt = deduper.dedupe()

    logger.info(rpt.toString())
}