import org.bradfordmiller.deduper.Deduper
import org.junit.Test

class DeduperTest {
    @Test fun DedupeTest() {

        val rpt =
            Deduper.dedupe(
                "RealEstate",
                "Sacramentorealestatetransactions",
                "default_ds",
                "RealEstate",
                "targetName",
                "dupesName",
                mutableSetOf("street","city", "state", "zip", "price")
            )

        println(rpt)
    }

    @Test fun KeysMissing() {
        val rpt =
                Deduper.dedupe(
                        "RealEstate",
                        "Sacramentorealestatetransactions",
                        "default_ds",
                        "RealEstate",
                        "targetName",
                        "dupesName",
                        mutableSetOf("street","city", "state", "zip", "price")
                )

        println(rpt)
    }
}