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
package org.apache.flink.api.common.expressions

import org.apache.flink.api.common.typeinfo.{NothingTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.common.expressions.typeinfo.{RenamingProxyTypeInfo, RowTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo

case class FieldReference(
    inputType: TypeInformation[_],
    inputIndex: Int, // always zero for single input operators; 0 or 1, for two input operators
    fieldName: String,
    fieldIndex: Int,
    fieldType: TypeInformation[_],
    accessor: FieldAccessor) extends LeafExpression {
  def typeInfo = fieldType

  override def toString = s"'$fieldName"
}

case class UnresolvedFieldReference(name: String) extends LeafExpression {
  def typeInfo = throw new ExpressionException(s"Unresolved field reference: $this")

  override def toString = "\"" + name
}

case class Naming(child: Expression, name: String) extends UnaryExpression {
  def typeInfo = child.typeInfo

  override def toString = s"$child as '$name"
}

sealed abstract class FieldAccessor

case class ObjectFieldAccessor(fieldName: String) extends FieldAccessor
case class ObjectMethodAccessor(methodName: String) extends FieldAccessor
case class ProductAccessor(i: Int) extends FieldAccessor

object FieldReference {
  /**
   * Creates the correct [[FieldAccessor]] for the type of element that is being accessed.
   */
  def fieldExpressionFor(elementType: CompositeType[_], inputIndex: Int, fieldName: String) = {
    FieldReference(
      elementType,
      inputIndex,
      fieldName,
      elementType.getFieldIndex(fieldName),
      elementType.getTypeAt(elementType.getFieldIndex(fieldName)),
      accessorFor(elementType, fieldName))

  }

  def accessorFor(elementType: CompositeType[_], fieldName: String): FieldAccessor = {
    elementType match {
      case ri: RowTypeInfo =>
          ProductAccessor(elementType.getFieldIndex(fieldName))

      case cc: CaseClassTypeInfo[_] =>
          ObjectFieldAccessor(fieldName)

      case proxy: RenamingProxyTypeInfo[_] =>
        val underlying = proxy.getUnderlyingType
        val fieldIndex = proxy.getFieldIndex(fieldName)
        accessorFor(underlying, underlying.getFieldNames()(fieldIndex))
    }
  }

  /**
   * Creates a dummy FieldExpression for an erroneous field reference.
   */
  def errorFieldExpression(fieldName: String) = {
    FieldReference(
      new NothingTypeInfo(),
      -1,
      fieldName,
      -1,
      new NothingTypeInfo(),
      ObjectFieldAccessor("invalid"))
  }
}

