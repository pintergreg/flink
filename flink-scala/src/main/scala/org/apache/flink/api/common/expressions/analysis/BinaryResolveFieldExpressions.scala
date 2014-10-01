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
package org.apache.flink.api.common.expressions.analysis

import org.apache.flink.api.common.expressions.FieldReference
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.common.expressions._

import scala.collection.mutable

/**
 * Rule that resolved field references for binary operations. We have two [[CompositeType]], one for
 * each input. This rule verifies that field references are unambiguous and
 * creates [[FieldReference]] that replaces the [[UnresolvedFieldReference]] in the expression.
 */
class BinaryResolveFieldExpressions[L, R](
    leftType: CompositeType[L],
    rightType: CompositeType[R]) extends Rule {

  def apply(expr: Expression) = {
    val errors = mutable.MutableList[String]()

    val result = expr.transformPost {
      case fe@UnresolvedFieldReference(fieldName) =>

        if (leftType.hasField(fieldName) && rightType.hasField(fieldName)) {
          errors += s"Ambiguous field reference. Both $leftType and $rightType have field" +
            s" $fe."
          FieldReference.errorFieldExpression(fieldName)
        } else if (leftType.hasField(fieldName)) {
          FieldReference.fieldExpressionFor(
            leftType,
            0,
            fieldName)
        } else if (rightType.hasField(fieldName)) {
          FieldReference.fieldExpressionFor(
            rightType,
            1,
            fieldName)
        } else if (fieldName.startsWith("left$") && leftType.hasField(fieldName.drop(5))) {
          FieldReference.fieldExpressionFor(
            leftType,
            0,
            fieldName.drop(5))
        } else if (fieldName.startsWith("right$") && rightType.hasField(fieldName.drop(6))) {
          FieldReference.fieldExpressionFor(
            rightType,
            1,
            fieldName.drop(6))
        } else {
          errors += s"Invalid field reference $fe for input types $leftType and $rightType."
          FieldReference.errorFieldExpression(fieldName)
        }
    }

    if (errors.length > 0) {
      throw new ExpressionException(
        s"""Invalid expression "$expr: ${errors.mkString("\n")}""")
    }

    result
  }
}
