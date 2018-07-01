import java.io.{File, FileReader}

import com.google.protobuf.util.JsonFormat
import com.itspawan.protobuf.DynamicJoinSchema
import com.itspawan.protobuf.DynamicJoinSchema.DynamicSchema
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
  * Created by pawanc on 6/29/18.
  */
object DynamicJoin {


  def main(args: Array[String]): Unit = {
    getDynamicSchema
  }


  def getDynamicSchema: DynamicSchema = {
    val file = new File("/Users/pawanc/GitHub/spark-dynamic-join/proto.json")
    val builder = DynamicJoinSchema.DynamicSchema.newBuilder
    val parser = JsonFormat.parser.ignoringUnknownFields
    parser.merge(new FileReader(file), builder)
    return builder.build()
  }

  def join(dynamicSchema: DynamicJoinSchema.DynamicSchema,
                  df1: DataFrame, df2: DataFrame): DataFrame = {
    val columns1 = df1.columns
    val columns2 = df2.columns

    val conditions = dynamicSchema.getConditionsList.asScala

    val joins = getJoinExperssions(conditions, df1, df2)

    df1.join(df2, joins)
  }

  private def getJoinExperssions(conditions: mutable.Buffer[DynamicSchema.JoinCondition],
                                 df1: DataFrame, df2: DataFrame): Column = {
    conditions.map {
      case (c1) => {
        if (c1.getOperation.equals(DynamicJoinSchema.DynamicSchema.Operation.NOT_EQUAL)) {
          df1.col(c1.getLhsColumn) =!= (df2.col(c1.getRhsColumn))
        } else {
          df1.col(c1.getLhsColumn) === (df2.col(c1.getRhsColumn))
        }
      }
    }.reduce(_ && _)
  }
}
