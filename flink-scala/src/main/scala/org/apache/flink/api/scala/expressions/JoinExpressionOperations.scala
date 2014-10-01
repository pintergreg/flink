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

import org.apache.flink.api.common.expressions.Row
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.common.expressions.runtime.ExpressionJoinFunction
import org.apache.flink.api.common.expressions.Expression
import org.apache.flink.api.scala.{DataSet, UnfinishedJoinOperation, _}

class JoinExpressionOperations[L, R](
    join: UnfinishedJoinOperation[L, R],
    leftType: CompositeType[L],
    rightType: CompositeType[R]) {

  def where(predicate: Expression): ExpressionJoinDataSet[L, R, Row] = {
    val result = ExpressionJoinFunction.createJoin(
      predicate,
      join.leftInput.javaSet,
      join.rightInput.javaSet,
      leftType,
      rightType,
      join.joinHint)

    new ExpressionJoinDataSet[L, R, Row](
      predicate,
      join.leftInput,
      join.rightInput,
      leftType,
      rightType,
      join.joinHint,
      wrap(result))
  }
}

class ExpressionJoinDataSet[L, R, O](
    predicate: Expression,
    leftInput: DataSet[L],
    rightInput: DataSet[R],
    leftType: CompositeType[L],
    rightType: CompositeType[R],
    joinHint: JoinHint,
    defaultJoin: DataSet[O])
  extends DataSet(defaultJoin.javaSet)(defaultJoin.classTag) {

  def select(fields: Expression*): DataSet[Row] = {

    val result = ExpressionJoinFunction.createJoin(
      predicate,
      fields.toArray,
      leftInput.javaSet,
      rightInput.javaSet,
      leftType,
      rightType,
      joinHint)

    wrap(result)
  }
}
