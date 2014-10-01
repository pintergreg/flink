/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.common.expressions.runtime

import org.apache.flink.api.common.expressions._
import org.apache.flink.api.common.expressions.analysis.{Utils, UnarySelectionAnalyzer, GroupByAnalyzer}

import org.apache.flink.api.common.typeinfo.{NothingTypeInfo, BasicTypeInfo}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.operators.Keys
import org.apache.flink.api.scala.expressions.RowDataSetOperations
import org.apache.flink.api.scala._

import org.apache.flink.api.java.{DataSet => JavaDataSet}


import scala.collection.mutable

object ExpressionAggregate {

  private def createGroupedDataSet[I](
      input: DataSet[I],
      inputType: CompositeType[I],
      keyExpressions: Seq[Expression]): GroupedDataSet[I] = {
    val analyzer = new GroupByAnalyzer(inputType)

    val analyzedFields = keyExpressions.map(analyzer.analyze)

    val fieldIndices = analyzedFields map {
      case fe: FieldReference => fe.fieldIndex
      case e => throw new ExpressionException(s"Expression $e is not a valid key expression.")
    }

    val keys = new Keys.ExpressionKeys(fieldIndices.toArray, input.getType, false)

    new GroupedDataSet[I](input, keys)(input.classTag)
  }

  def createSelect[I](
      input: JavaDataSet[I],
      inputType: CompositeType[I],
      keys: Array[Expression],
      fields: Array[Expression]): JavaDataSet[Row] = {

    val analyzer = new UnarySelectionAnalyzer(inputType)
    val normalizedFields = Utils.normalizeFields(fields)

    // Analyze, so that we find errors early on
    fields.map(analyzer.analyze)

    val aggregations = mutable.HashMap[(Expression, Aggregations), String]()
    val intermediateFields = mutable.HashSet[Expression]()
    val aggregationIntermediates = mutable.HashMap[Aggregation, Seq[Expression]]()

    var intermediateCount = 0
    normalizedFields foreach {  f =>
      f.transformPre {
        case agg: Aggregation =>
          val intermediateReferences = agg.getIntermediateFields.zip(agg.getAggregations) map {
            case (expr, basicAgg) =>
              aggregations.get((expr, basicAgg)) match {
                case Some(intermediateName) =>
                  UnresolvedFieldReference(intermediateName)
                case None =>
                  intermediateCount = intermediateCount + 1
                  val intermediateName = s"intermediate.$intermediateCount"
                  intermediateFields += Naming(expr, intermediateName)
                  aggregations((expr, basicAgg)) = intermediateName
                  UnresolvedFieldReference(intermediateName)
              }
          }

          aggregationIntermediates(agg) = intermediateReferences
          // Return a NOP so that we don't add the children of the aggregation
          // to intermediate fields. We already added the necessary fields to the list
          // of intermediate fields.
          NopExpression()

        case fa: UnresolvedFieldReference =>
          if (!fa.name.startsWith("intermediate")) {
            intermediateFields += Naming(fa, fa.name)
          }
          fa
      }
    }

    // also add the grouping keys to the set of intermediate fields, because we use a Set,
    // they are only added when not already present
    keys foreach {
      case fa: UnresolvedFieldReference =>
        intermediateFields += Naming(fa, fa.name)
    }

    val basicAggregations = aggregations.map {
      case ((_, basicAgg), fieldName) => (fieldName, basicAgg)
    }

    val intermediateMap =
      ExpressionSelectFunction.createSelect(intermediateFields.toArray, input, inputType)
    val intermediateType = intermediateMap.getType.asInstanceOf[CompositeType[Row]]

    val aggregateSet: DataSet[Row] = if (keys.nonEmpty && aggregations.nonEmpty) {
      val groupedDataSet = createGroupedDataSet(wrap(intermediateMap), intermediateType, keys)
      val aggregateSet = groupedDataSet.aggregate(
        basicAggregations.head._2,
        intermediateType.getFieldIndex(basicAggregations.head._1))

      basicAggregations.tail foreach {
        case (fieldName, agg) =>
          aggregateSet.and(agg, intermediateType.getFieldIndex(fieldName))
      }
      aggregateSet
    } else if (aggregations.nonEmpty) {
      val aggregateSet = wrap(intermediateMap).aggregate(
        basicAggregations.head._2,
        intermediateType.getFieldIndex(basicAggregations.head._1))

      basicAggregations.tail foreach {
        case (fieldName, agg) =>
          aggregateSet.and(agg, intermediateType.getFieldIndex(fieldName))
      }
      aggregateSet
    } else {
      wrap(intermediateMap)
    }

    val aggregateType = aggregateSet.getType.asInstanceOf[CompositeType[Row]]

    val finalAnalyzer = new UnarySelectionAnalyzer(aggregateType)

    val finalFields = normalizedFields.map {  f =>
      f.transformPre {
        case agg: Aggregation =>
          val intermediates = aggregationIntermediates(agg)
          // we must pre-analyze the intermediate field references because
          // getFinalField might want to use the typeInfo
          val analyzedIntermediates = intermediates.map(finalAnalyzer.analyze)
          val ff = agg.getFinalField(analyzedIntermediates)
          ff
      }
    }

    val normalizedFinalFields = Utils.normalizeFields(finalFields)

    // Analyze, just to be on the safe side
    normalizedFinalFields.map(finalAnalyzer.analyze)

    val finalMap = new RowDataSetOperations(aggregateSet, aggregateType)
      .select(finalFields: _*)

    finalMap.javaSet
  }
}
