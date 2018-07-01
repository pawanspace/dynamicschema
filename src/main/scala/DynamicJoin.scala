import java.io.{File, FileReader}

import com.google.protobuf.util.JsonFormat
import com.itspawan.protobuf.DynamicJoinSchema
import com.itspawan.protobuf.DynamicJoinSchema.DynamicSchema
import com.itspawan.protobuf.DynamicJoinSchema.DynamicSchema.Operation._
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
  * Created by pawanc on 6/29/18.
  */
object DynamicJoin {

  val equals = (lhs: Column, rhs: Any) => { lhs.equalTo(rhs)}
  val notEquals = (lhs: Column, rhs: Any) => { lhs.notEqual(rhs)}
  val isNull = (col: Column) => { col.isNull }
  val isNotNull = (col: Column) => { col.isNotNull }
  val isEmpty = (value: String) => value == null || value.trim.isEmpty

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

    val andConditions = dynamicSchema.getAndCondition().getConditionsList.asScala
    val orConditions = dynamicSchema.getOrCondition().getConditionsList.asScala

    val andJoinExpr = getAndJoinExpressions(andConditions, df1, df2)
    val orJoinExpr = getOrJoinExpressions(orConditions, df1, df2)

    printf(andJoinExpr.toString())
    printf(orJoinExpr.toString())

    val columns = dynamicSchema.getSelectableColumnsList.asScala.toArray

    //The tail method returns all of the elements except the first one.
    df1.join(df2, (orJoinExpr).and(andJoinExpr)).select(columns.head, columns.tail: _*)
  }


  private def getAndJoinExpressions(conditions: mutable.Buffer[DynamicSchema.JoinCondition],
                                    df1: DataFrame, df2: DataFrame): Column = {
    mapToConditions(conditions,df1, df2).reduce(_ && _)
  }


  private def getOrJoinExpressions(conditions: mutable.Buffer[DynamicSchema.JoinCondition],
                                   df1: DataFrame, df2: DataFrame): Column = {
    mapToConditions(conditions,df1, df2).reduce(_ || _)
  }

  private def mapToConditions(conditions: mutable.Buffer[DynamicSchema.JoinCondition],
                              df1: DataFrame, df2: DataFrame): mutable.Seq[Column] = {
    conditions.map {
      case (c1) => {
        if (c1.getOperation.equals(NOT_EQUAL)) {
          createJoinConditions(df1, df2, c1, notEquals)
        } else if (c1.getOperation.equals(IS_NULL)) {
          createJoinConditionsForNullAndNotNull(df1, df2, c1, isNull)
        } else if (c1.getOperation.equals(IS_NOT_NULL)) {
          createJoinConditionsForNullAndNotNull(df1, df2, c1, isNotNull)
        } else {
          createJoinConditions(df1, df2, c1, equals)
        }
      }
    }
  }

  private def createJoinConditions(df1: DataFrame, df2: DataFrame,
                                   c1: DynamicSchema.JoinCondition,
                                   func: (Column, Any) => Column) = {

    if (!isEmpty(c1.getLhsColumn) && !isEmpty(c1.getRhsColumn)) {
      func.apply(df1.col(c1.getLhsColumn),  df2.col(c1.getRhsColumn))
    } else if (isEmpty(c1.getLhsColumn)) {
      func.apply(df2.col(c1.getRhsColumn), c1.getLhsValue)
    } else if (isEmpty(c1.getRhsColumn) ) {
      func.apply(df1.col(c1.getLhsColumn), c1.getRhsValue)
    } else {
      throw new IllegalStateException
    }
  }

  private def createJoinConditionsForNullAndNotNull(df1: DataFrame, df2: DataFrame,
                                   c1: DynamicSchema.JoinCondition,
                                   func: (Column) => Column) = {

    if (!isEmpty(c1.getLhsColumn)) {
      func.apply(df1.col(c1.getLhsColumn))
    } else if (!isEmpty(c1.getRhsColumn) ) {
      func.apply(df2.col(c1.getRhsColumn))
    } else {
      throw new IllegalStateException
    }
  }

}
