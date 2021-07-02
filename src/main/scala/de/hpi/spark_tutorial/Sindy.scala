package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    val input_dss = inputs.map(csv_file =>
      spark
        .read
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .option("sep", ";")
        .load(csv_file)
        .flatMap(row =>
          row.schema.fieldNames.map(column =>
            (row.getAs(column).toString, column)
          )
        ))

    val tuples = input_dss.reduce((accumulatorDF, nextDF) => accumulatorDF.union(nextDF))

    // Next steps: Column namen durch ints ersetzen, und input/mapping trennen

//    val column_to_id = input_dfs.flatMap(df => df.columns).zipWithIndex.toMap
//    val id_to_column = input_dfs.flatMap(df => df.columns).zipWithIndex.map(_.swap).toMap



//    val column_to_id = input_dfs.flatMap(df => df.columns).zipWithIndex.map(_.swap).toMap
/*val column_to_id = input_dfs.flatMap(df => df.columns).zipWithIndex.map(_.swap).toMap
val id_to_column = input_dfs.flatMap(df => df.columns).zipWithIndex.toMap

val rows = input_dfs.map(df =>
    df.toJavaRDD.map(row =>
      (row.toSeq.zipWithIndex.map {
        case (value, col_idx) => (value, df.columns(col_idx))
      })
    )
  )*/

//rows
//val tuples = input_dfs.map(df => df.map(row => zip(df.columns, row)))

}
}
