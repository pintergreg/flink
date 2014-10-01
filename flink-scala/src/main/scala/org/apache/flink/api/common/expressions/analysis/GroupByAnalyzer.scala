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

import org.apache.flink.api.common.expressions._
import org.apache.flink.api.common.typeutils.CompositeType

import scala.collection.mutable


/**
 * Analyzer for grouping expressions. Only field expressions are allowed as grouping expressions.
 */
class GroupByAnalyzer[T](typeInfo: CompositeType[T]) extends Analyzer {

  def rules = Seq(new UnaryResolveFieldExpressions(typeInfo), CheckGroupExpression)

}

object CheckGroupExpression extends Rule {

  def apply(expr: Expression) = {
    val errors = mutable.MutableList[String]()

    expr match {
      case f: FieldReference => // this is OK
      case other =>
        throw new ExpressionException(
          s"""Invalid grouping expression "$expr". Only field references are allowed.""")
    }
    expr
  }
}
