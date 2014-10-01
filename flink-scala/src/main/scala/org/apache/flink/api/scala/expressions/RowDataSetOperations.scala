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
package org.apache.flink.api.scala.expressions

import org.apache.flink.api.common.expressions._
import org.apache.flink.api.common.expressions.analysis.GroupByAnalyzer
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.common.expressions.runtime.{ExpressionFilterFunction,
ExpressionAsFunction, ExpressionSelectFunction}

import org.apache.flink.api.scala._

import scala.reflect.ClassTag

class RowDataSetOperations[T](set: DataSet[T], inputType: CompositeType[T]) {

  def select(fields: Expression*): DataSet[Row] = {

    val result = ExpressionSelectFunction.createSelect(
      fields.toArray,
      set.javaSet,
      inputType)

    wrap(result)
  }

  def as[O: TypeInformation : ClassTag]: DataSet[O] = {
    if (!implicitly[TypeInformation[O]].isInstanceOf[CompositeType[O]]) {
      throw new IllegalArgumentException("Operation as[] can only convert to composite types.")
    }

    val outputType = implicitly[TypeInformation[O]].asInstanceOf[CompositeType[O]]

    val result = ExpressionAsFunction.createAs(set.javaSet, inputType, outputType)
    wrap(result)
  }

  def as(fields: Expression*): DataSet[T] = {
    val result = ExpressionAsFunction.createAs(fields.toArray, set.javaSet, inputType)
    wrap(result)(set.classTag)
  }

  def filter(predicate: Expression): DataSet[T] = {
    val result = ExpressionFilterFunction.createFilter(predicate, set.javaSet, inputType)

    wrap(result)(set.classTag)
  }

  def group(first: Expression, rest: Expression*): GroupingExpressionOperations[T] = {
    val fields = first +: rest

    val analyzer = new GroupByAnalyzer(inputType)

    val analyzedFields = fields.map(analyzer.analyze)

    val illegalKeys = analyzedFields filter {
      case fe: FieldReference => false // OK
      case e => true
    }

    if (illegalKeys.nonEmpty) {
      throw new ExpressionException("Illegal key expressions: " + illegalKeys.mkString(", "))
    }

    new GroupingExpressionOperations[T](set, inputType, fields)
  }
}

class DataSetOperations[T](set: DataSet[T], inputType: CompositeType[T]) {
  def select(fields: Expression*): DataSet[Row] = {

    val result = ExpressionSelectFunction.createSelect(
      fields.toArray,
      set.javaSet,
      inputType)

    wrap(result)
  }

  def as(fields: Expression*): DataSet[Row] = {

    if (fields.length != inputType.getFieldNames.length) {
      throw new ExpressionException("Number of selected fields: '" + fields.mkString(",") +
      "' and number of fields in input type " + inputType + " do not match.")
    }

    val newFieldNames = fields map {
      case UnresolvedFieldReference(name) => name
      case e =>
        throw new ExpressionException("Only field expressions allowed in 'as' operation, " +
        " offending expression: " + e)
    }

    val fieldRenames = inputType.getFieldNames.zip(newFieldNames) map {
      case (oldName, newName) => Naming(UnresolvedFieldReference(oldName), newName)
    }

    val result = ExpressionSelectFunction.createSelect(
      fieldRenames.toArray,
      set.javaSet,
      inputType)

    wrap(result)
  }

}

