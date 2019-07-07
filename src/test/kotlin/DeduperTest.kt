import org.bradfordmiller.deduper.Deduper
import org.junit.Test

class DeduperTest {
    @Test fun DedupeTest() {
        Class.forName("org.relique.jdbc.csv.CsvDriver")

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