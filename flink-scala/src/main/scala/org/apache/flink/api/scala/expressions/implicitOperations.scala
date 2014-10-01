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

import com.google.common.base.Preconditions
import org.apache.flink.api.common.expressions._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, UnfinishedJoinOperation}

import org.apache.flink.api.common.typeutils.CompositeType

import scala.language.implicitConversions

trait ImplicitExpressionOperators {
  def expr: Expression

  def && (other: Expression) = And(expr, other)
  def || (other: Expression) = Or(expr, other)

  def > (other: Expression) = GreaterThan(expr, other)
  def >= (other: Expression) = GreaterThanOrEqual(expr, other)
  def < (other: Expression) = LessThan(expr, other)
  def <= (other: Expression) = LessThanOrEqual(expr, other)

  def === (other: Expression) = EqualTo(expr, other)
  def !== (other: Expression) = NotEqualTo(expr, other)

  def unary_! = Not(expr)
  def unary_- = UnaryMinus(expr)

  def isNull = IsNull(expr)
  def isNotNull = IsNotNull(expr)

  def + (other: Expression) = Plus(expr, other)
  def - (other: Expression) = Minus(expr, other)
  def / (other: Expression) = Div(expr, other)
  def * (other: Expression) = Mul(expr, other)
  def % (other: Expression) = Mod(expr, other)

  def & (other: Expression) = BitwiseAnd(expr, other)
  def | (other: Expression) = BitwiseOr(expr, other)
  def ^ (other: Expression) = BitwiseXor(expr, other)
  def unary_~ = BitwiseNot(expr)

  def abs = Abs(expr)

  def sum = Sum(expr)
  def min = Min(expr)
  def max = Max(expr)
  def count = Count(expr)
  def avg = Avg(expr)

  def substring(beginIndex: Expression, endIndex: Expression = Literal(Int.MaxValue)) = {
    Substring(expr, beginIndex, endIndex)
  }

  def cast(toType: TypeInformation[_]) = Cast(expr, toType)

  def as(name: Symbol) = Naming(expr, name.name)
}

trait ImplicitExpressionConversions {
  implicit class WithOperators(e: Expression) extends ImplicitExpressionOperators {
    def expr = e
  }

  implicit class SymbolExpression(s: Symbol) extends ImplicitExpressionOperators {
    def expr = UnresolvedFieldReference(s.name)
  }

  implicit class LiteralIntExpression(i: Int) extends ImplicitExpressionOperators {
    def expr = Literal(i)
  }

  implicit class LiteralFloatExpression(f: Float) extends ImplicitExpressionOperators {
    def expr = Literal(f)
  }

  implicit class LiteralDoubleExpression(d: Double) extends ImplicitExpressionOperators {
    def expr = Literal(d)
  }

  implicit class LiteralStringExpression(str: String) extends ImplicitExpressionOperators {
    def expr = Literal(str)
  }

  implicit class LiteralBooleanExpression(bool: Boolean) extends ImplicitExpressionOperators {
    def expr = Literal(bool)
  }

  implicit def symbol2FieldExpression(sym: Symbol): Expression = UnresolvedFieldReference(sym.name)
  implicit def int2Literal(i: Int): Expression = Literal(i)
  implicit def long2Literal(l: Long): Expression = Literal(l)
  implicit def double2Literal(d: Double): Expression = Literal(d)
  implicit def float2Literal(d: Float): Expression = Literal(d)
  implicit def string2Literal(str: String): Expression = Literal(str)
  implicit def boolean2Literal(bool: Boolean): Expression = Literal(bool)
}

trait ImplicitDataSetConversions {

  implicit def unfinishedJoin2ExpressionJoin[L <: Row, R <: Row](
      join: UnfinishedJoinOperation[L, R]): JoinExpressionOperations[L, R] = {
    Preconditions.checkArgument(join.leftInput.getType.isInstanceOf[CompositeType[L]])
    Preconditions.checkArgument(join.rightInput.getType.isInstanceOf[CompositeType[R]])
    new JoinExpressionOperations[L, R](
      join,
      join.leftInput.getType.asInstanceOf[CompositeType[L]],
      join.rightInput.getType.asInstanceOf[CompositeType[R]])
  }

  implicit def dataSet2RowDataSetOperations[T <: Row](set: DataSet[T]): RowDataSetOperations[T] = {
    Preconditions.checkArgument(set.getType.isInstanceOf[CompositeType[T]])
    new RowDataSetOperations[T](set, set.getType.asInstanceOf[CompositeType[T]])
  }

  implicit def dataSet2DataSetOperations[T <: Product](set: DataSet[T]): DataSetOperations[T] = {
    Preconditions.checkArgument(set.getType.isInstanceOf[CompositeType[T]])
    new DataSetOperations[T](set, set.getType.asInstanceOf[CompositeType[T]])
  }

//  implicit def grouping2GroupingExpressionOperations[T <: Row](set: GroupedDataSet[T]) = {
//    Preconditions.checkArgument(set.getType.isInstanceOf[CompositeType[T]])
//    new GroupingExpressionOperations[T](set, set.getType.asInstanceOf[CompositeType[T]])
//  }
}
