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

import org.apache.flink.api.common.expressions.{Row, Naming}
import org.apache.flink.api.common.functions.RichFlatJoinFunction
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.operators.JoinOperator.EquiJoin
import org.apache.flink.api.java.operators.Keys.ExpressionKeys
import org.apache.flink.api.common.expressions._
import org.apache.flink.api.common.expressions.analysis.{BinaryPredicateAnalyzer,
BinarySelectionAnalyzer}
import org.apache.flink.api.common.expressions.codegen.{GenerateBinaryResultAssembler,
GenerateBinaryPredicate}
import org.apache.flink.api.common.expressions.typeinfo.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import org.apache.flink.api.java.{DataSet => JavaDataSet}


class ExpressionJoinFunction[L, R, O](
    predicate: Expression,
    leftType: CompositeType[L],
    rightType: CompositeType[R],
    resultType: CompositeType[O],
    outputFields: Seq[(Expression, String)]) extends RichFlatJoinFunction[L, R, O] {

  var compiledPredicate: (L, R) => Boolean = null
  var resultAssembler: (L, R, O) => O = null
  var result: O = null.asInstanceOf[O]

  override def open(config: Configuration): Unit = {
    result = resultType.createSerializer().createInstance()
    if (compiledPredicate == null) {
      compiledPredicate = predicate match {
        case n: NopExpression => null
        case _ =>
          println("Compiling...")
          val codegen = new GenerateBinaryPredicate[L, R](
            leftType,
            rightType,
            predicate,
            getRuntimeContext.getUserCodeClassLoader)
          val result = codegen.generate()
          println("Compile Done")
          result
      }
    }

    if (resultAssembler == null) {
      val resultCodegen = new GenerateBinaryResultAssembler[L, R, O](
        leftType,
        rightType,
        resultType,
        outputFields,
        getRuntimeContext.getUserCodeClassLoader)

      resultAssembler = resultCodegen.generate()
    }
  }

  def join(left: L, right: R, out: Collector[O]) = {
    if (compiledPredicate == null) {
      result = resultAssembler(left, right, result)
      out.collect(result)
    } else {
      if (compiledPredicate(left, right)) {
        result = resultAssembler(left, right, result)
        out.collect(result)      }
    }
  }
}

object ExpressionJoinFunction {

  def createJoin[L, R](
      predicate: Expression,
      leftInput: JavaDataSet[L],
      rightInput: JavaDataSet[R],
      leftType: CompositeType[L],
      rightType: CompositeType[R],
      joinHint: JoinHint): JavaDataSet[Row] = {

    val leftFieldNames = leftType.getFieldNames
    val rightFieldNames = rightType.getFieldNames

    val uniqueLeftOutputNames = leftFieldNames map {
      name =>
        if (rightFieldNames.contains(name)) {
          "left$" + name
        } else {
          name
        }
    }

    val uniqueRightOutputNames = rightFieldNames map {
      name =>
        if (leftFieldNames.contains(name)) {
          "right" + name
        } else {
          name
        }
    }

    val outputExpressions: Array[Expression] =
      (leftFieldNames.zip(uniqueLeftOutputNames) map {
        case (in, out) =>
          Naming(FieldReference.fieldExpressionFor(leftType, 0, in), out)
      }) ++ (rightFieldNames.zip(uniqueRightOutputNames) map {
        case (in, out) =>
          Naming(FieldReference.fieldExpressionFor(rightType, 1, in), out)
      })

    ExpressionJoinFunction.createJoin(
      predicate,
      outputExpressions,
      leftInput,
      rightInput,
      leftType,
      rightType,
      joinHint)
  }

  def createJoin[L, R](
      predicate: Expression,
      fields: Array[Expression],
      leftInput: JavaDataSet[L],
      rightInput: JavaDataSet[R],
      leftType: CompositeType[L],
      rightType: CompositeType[R],
      joinHint: JoinHint): JavaDataSet[Row] = {
    println("EXPR: " + predicate)

    val analyzer = new BinarySelectionAnalyzer(leftType, rightType)

    val analyzedFields = fields.map(analyzer.analyze)

    val newNames = analyzedFields.zipWithIndex map {
      case (expression, i) => expression match {
        case fe: FieldReference => fe.fieldName
        case Naming(_, newName) => newName
        case _ => s"_${i + 1}"
      }
    }

    if (newNames.toSet.size != newNames.size) {
      throw new ExpressionException(s"Resulting fields names are not unique in expression" +
        s""" "${fields.mkString(", ")}".""")
    }

    val newTypes = analyzedFields.map(_.typeInfo)

    val resultType = new RowTypeInfo(newTypes, newNames)


    createJoin(
      predicate,
      analyzedFields.zip(newNames),
      leftInput,
      rightInput,
      leftType,
      rightType,
      resultType,
      joinHint)
  }

  def createJoin[L, R, O](
      predicate: Expression,
      fields: Array[(Expression, String)],
      leftInput: JavaDataSet[L],
      rightInput: JavaDataSet[R],
      leftType: CompositeType[L],
      rightType: CompositeType[R],
      resultType: CompositeType[O],
      joinHint: JoinHint): JavaDataSet[O] = {

    val analyzer = new BinaryPredicateAnalyzer(leftType, rightType)
    val analyzedPredicate = analyzer.analyze(predicate)
    println("NEW EXPR: " + analyzedPredicate)

    val (leftFields, rightFields) = analyzer.getEquiJoinFields

    val leftKey = new ExpressionKeys[L](leftFields, leftType)
    val rightKey = new ExpressionKeys[R](rightFields, rightType)

    val joiner = new ExpressionJoinFunction[L, R, O](
      analyzedPredicate,
      leftType,
      rightType,
      resultType,
      fields)

    val joinOperator = new EquiJoin[L, R, O](
      leftInput,
      rightInput,
      leftKey,
      rightKey,
      joiner,
      resultType,
      joinHint,
      predicate.toString)


    joinOperator
  }
}
