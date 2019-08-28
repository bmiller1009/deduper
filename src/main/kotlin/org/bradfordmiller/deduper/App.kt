package org.bradfordmiller.deduper

import org.bradfordmiller.deduper.config.Config

class App {
    val greeting: String
        get() {
            return "Hello world."
        }
}

fun main(args: Array<String>) {
    val config = Config(
            "RealEstateIn",
            "Sacramentorealestatetransactions",
            "default_ds",
            "RealEstateIn",
            "tstTable",
            "RealEstateIn",
            mutableSetOf("street","city", "state", "zip", "price")
    )

    val deduper = Deduper(config)

    val rpt = deduper.dedupe()

    println(rpt)
}