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

import org.apache.flink.api.common.expressions.{EqualTo, FieldReference$}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.common.expressions._

import scala.collection.mutable

/**
 * Analyzer for binary predicates, i.e. Join Predicates and CoGroup predicates. It will remove
 * equi join predicates from the expression and save them. They can be retrieved using
 * `getJoinFields`.
 */
class BinaryPredicateAnalyzer[L, R](
    leftTpe: CompositeType[L],
    rightTpe: CompositeType[R])
  extends Analyzer {

  def rules = Seq(
    new BinaryResolveFieldExpressions(leftTpe, rightTpe),
    ExtractEquiJoins,
    new InsertAutoCasts,
    new TypeCheck,
    new VerifyBoolean)

  val joinFieldsLeft = mutable.MutableList[Int]()
  val joinFieldsRight = mutable.MutableList[Int]()

  object ExtractEquiJoins extends Rule {
    def apply(expr: Expression) = {
      val equiJoinExprs = mutable.MutableList[EqualTo]()
      // First get all `===` expressions that are not below an `Or`
      expr.transformPre {
        case or@Or(_, _) => NopExpression()
        case eq@EqualTo(le: FieldReference, re: FieldReference) =>
          equiJoinExprs += eq
          eq
      }

      // Add them to our field position lists
      equiJoinExprs foreach {
        case EqualTo(le: FieldReference, re: FieldReference) =>
          joinFieldsLeft += le.fieldIndex
          joinFieldsRight += re.fieldIndex
      }

      // then remove the equi join expressions from the predicate
      expr.transformPost {
        // For OR, we can eliminate the OR since the equi join
        // predicate is evaluated before the expression is evaluated
        case or@Or(NopExpression(), _) => NopExpression()
        case or@Or(_, NopExpression()) => NopExpression()
        // For AND we replace it with the other expression, since the
        // equi join predicate will always be true
        case and@And(NopExpression(), other) => other
        case and@And(other, NopExpression()) => other
        case eq : EqualTo if equiJoinExprs.contains(eq) =>
          NopExpression()
      }
    }
  }

  def getEquiJoinFields: (Array[Int], Array[Int]) = {
    (joinFieldsLeft.toArray, joinFieldsRight.toArray)
  }

}