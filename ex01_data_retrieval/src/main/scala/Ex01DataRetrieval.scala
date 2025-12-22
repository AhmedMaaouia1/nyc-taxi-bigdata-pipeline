import java.io.File
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

import org.apache.spark.sql.SparkSession

object Ex01DataRetrieval {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------------
    // 1. Spark Session (PAS de .master("local[*]"))
    // ------------------------------------------------------------------
    val spark = SparkSession.builder()
      .appName("Ex01 - NYC Taxi Data Retrieval")
      .getOrCreate()

    // ------------------------------------------------------------------
    // 2. Paramètres du dataset
    // ------------------------------------------------------------------
    val year = "2023"
    val month = "01"

    val fileName = s"yellow_tripdata_${year}-${month}.parquet"

    val sourceUrl =
      s"https://d37ci6vzurychx.cloudfront.net/trip-data/$fileName"

    // Local Raw Zone
    val localDir =
      s"/opt/data/raw/yellow/$year/$month"

    val localFilePath =
      s"$localDir/$fileName"

    // MinIO (S3A)
    val s3TargetPath =
      s"s3a://nyc-raw/yellow/$year/$month/"

    // ------------------------------------------------------------------
    // 3. Téléchargement du fichier (si absent)
    // ------------------------------------------------------------------
    val localFile = new File(localFilePath)

    if (!localFile.exists()) {
      println(s"[INFO] Downloading NYC Taxi file from $sourceUrl")

      Files.createDirectories(Paths.get(localDir))

      val in = new URL(sourceUrl).openStream()
      Files.copy(in, Paths.get(localFilePath), StandardCopyOption.REPLACE_EXISTING)
      in.close()

      println(s"[INFO] File downloaded to $localFilePath")
    } else {
      println(s"[INFO] File already exists locally, skipping download")
    }

    // ------------------------------------------------------------------
    // 4. Lecture Spark du parquet local
    // ------------------------------------------------------------------
    val df = spark.read.parquet(localFilePath)

    println(s"[INFO] Number of records: ${df.count()}")

    // ------------------------------------------------------------------
    // 5. Upload vers MinIO (S3A)
    // ------------------------------------------------------------------
    df.write
      .mode("overwrite")
      .parquet(s3TargetPath)

    println(s"[INFO] File uploaded to $s3TargetPath")

    spark.stop()
  }
}
