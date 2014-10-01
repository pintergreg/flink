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
import org.apache.flink.api.common.expressions.Expression
import org.apache.flink.api.common.expressions._

import scala.collection.mutable

/**
 * Rule that resolved field references for unary operations. This rule verifies that field
 * references are unambiguous and creates [[FieldReference]] that replaces
 * the [[UnresolvedFieldReference]] in the expression.
 */
class UnaryResolveFieldExpressions[T](typeInfo: CompositeType[T]) extends Rule {

  def apply(expr: Expression) = {
    val errors = mutable.MutableList[String]()

    val result = expr.transformPost {
      case fe@UnresolvedFieldReference(fieldName) => {

        typeInfo.getFieldIndex(fieldName) match {
          case -2 =>
            errors += s"Field '$fieldName' is ambiguous for $typeInfo."
            FieldReference.errorFieldExpression(fieldName)

          case -1 =>
            errors += s"Field '$fieldName' is not valid for $typeInfo."
            FieldReference.errorFieldExpression(fieldName)

          case _ =>
            FieldReference.fieldExpressionFor(
              typeInfo,
              0,
              fieldName)
        }
      }
    }

    if (errors.length > 0) {
      throw new ExpressionException(
        s"""Invalid expression "$expr": ${errors.mkString(" ")}""")
    }

    result

  }
}
