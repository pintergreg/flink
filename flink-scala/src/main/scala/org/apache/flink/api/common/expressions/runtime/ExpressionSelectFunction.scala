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
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.operators.MapOperator
import org.apache.flink.api.common.expressions.analysis.UnarySelectionAnalyzer
import org.apache.flink.api.common.expressions
import org.apache.flink.api.common.expressions.codegen.GenerateUnaryResultAssembler
import org.apache.flink.api.common.expressions.typeinfo.RowTypeInfo
import org.apache.flink.configuration.Configuration

import org.apache.flink.api.java.{DataSet => JavaDataSet}

import scala.collection.mutable


class ExpressionSelectFunction[I, O](
     inputType: CompositeType[I],
     resultType: CompositeType[O],
     outputFields: Seq[(Expression, String)]) extends RichMapFunction[I, O] {

  var resultAssembler: (I, O) => O = null
  var result: O = null.asInstanceOf[O]

  override def open(config: Configuration): Unit = {
    result = resultType.createSerializer().createInstance()

    if (resultAssembler == null) {
      val resultCodegen = new GenerateUnaryResultAssembler[I, O](
        inputType,
        resultType,
        outputFields,
        getRuntimeContext.getUserCodeClassLoader)

      resultAssembler = resultCodegen.generate()
    }
  }

  def map(in: I): O = {
    resultAssembler(in, result)
  }
}

object ExpressionSelectFunction{

  def createSelect[I](
      fields: Array[Expression],
      input: JavaDataSet[I],
      inputType: CompositeType[I]): JavaDataSet[Row] = {

    val analyzer = new UnarySelectionAnalyzer(inputType)

    val analyzedFields = fields.map(analyzer.analyze)

    analyzedFields foreach {
      f =>
        if (f.exists(_.isInstanceOf[Aggregation])) {
          return ExpressionAggregate.createSelect(input, inputType, Array(), fields)
        }

    }

    val newNames = mutable.MutableList[String]()

    analyzedFields.zipWithIndex foreach {
      case (expression, i) => expression match {
        case fe: FieldReference if !newNames.contains(fe.fieldName)=> newNames += fe.fieldName
        case expressions.Naming(_, newName) => newNames += newName
        case _ => newNames += s"_${i + 1}"
      }
    }

    if (newNames.toSet.size != newNames.size) {
      throw new ExpressionException(s"Resulting fields names are not unique in expression" +
        s""" "${fields.mkString(", ")}".""")
    }

    val newTypes = analyzedFields.map(_.typeInfo)

    val resultType = new RowTypeInfo(newTypes, newNames)


    createSelect(analyzedFields.zip(newNames), input, inputType, resultType)
  }

  def createSelect[I, O](
      fields: Array[(Expression, String)],
      input: JavaDataSet[I],
      inputType: CompositeType[I],
      resultType: CompositeType[O]): JavaDataSet[O] = {

    val analyzer = new UnarySelectionAnalyzer[I](inputType)


    val analyzedFields = fields map {
      case (expr, outputName) =>
        (analyzer.analyze(expr), outputName)
    }

    val function = new ExpressionSelectFunction(inputType, resultType, analyzedFields)

    val opName = s"select(${analyzedFields.mkString(",")})"
    val operator = new MapOperator(input, resultType, function, opName)

    operator
  }
}

