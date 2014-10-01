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
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.common.expressions.typeinfo.{RenameOperator, RenamingProxyTypeInfo}

import org.apache.flink.api.java.{DataSet => JavaDataSet}

object ExpressionAsFunction{
  def createAs[T](
      fields: Array[Expression],
      input: JavaDataSet[T],
      inputType: CompositeType[T]): JavaDataSet[T] = {

    fields forall {
      f => f.isInstanceOf[UnresolvedFieldReference]
    } match {
      case true =>
      case false => throw new ExpressionException("Only field expression allowed in as().")
    }

    val newNames = fields map {
      case UnresolvedFieldReference(name) => name
    }

    val proxyType = new RenamingProxyTypeInfo[T](inputType, newNames.toArray)

    new RenameOperator(input, proxyType)
  }

  def createAs[I, O](
      input: JavaDataSet[I],
      inputType: CompositeType[I],
      outputType: CompositeType[O]): JavaDataSet[O] = {

    val inputNames = inputType.getFieldNames
    val outputNames = outputType.getFieldNames

    if (inputNames.toSet != outputNames.toSet) {
      throw new ExpressionException(s"Input $inputType does not have the same fields as " +
        s"output type $outputType")
    }

    for (f <- outputNames) {
      val in = inputType.getTypeAt(inputType.getFieldIndex(f))
      val out = outputType.getTypeAt(outputType.getFieldIndex(f))
      if (!in.equals(out)) {
        throw new ExpressionException(s"Types for field $f differ on input $inputType and " +
          s"output $outputType.")
      }
    }

    val outputFields = outputNames map {
      f =>
        FieldReference.fieldExpressionFor(inputType, 0, f)
    }

    ExpressionSelectFunction.createSelect(
      outputFields.zip(outputNames),
      input,
      inputType,
      outputType)
  }
}

