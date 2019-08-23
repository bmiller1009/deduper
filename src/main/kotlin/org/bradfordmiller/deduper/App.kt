package org.bradfordmiller.deduper.config

import kotlinx.serialization.ImplicitReflectionSerializer
import org.bradfordmiller.deduper.Deduper

class App {
    val greeting: String
        get() {
            return "Hello world."
        }
}

@ImplicitReflectionSerializer
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