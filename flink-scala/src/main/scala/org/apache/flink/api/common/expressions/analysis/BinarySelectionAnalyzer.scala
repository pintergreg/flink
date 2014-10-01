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
 * This analyzes selection expressions.
 */
class BinarySelectionAnalyzer[L, R](
    leftType: CompositeType[L],
    rightType: CompositeType[R]) extends Analyzer {

  def rules = Seq(
    new BinaryResolveFieldExpressions(leftType, rightType),
    new InsertAutoCasts,
    new TypeCheck,
    VerifyNoAggregates)

  object VerifyNoAggregates extends Rule {
    def apply(expr: Expression) = {
      if (expr.exists(_.isInstanceOf[Aggregation])) {
        throw new ExpressionException(
          s"""Invalid expression "$expr": Joins do not support aggregations.""")
      }
      expr
    }
  }

}
