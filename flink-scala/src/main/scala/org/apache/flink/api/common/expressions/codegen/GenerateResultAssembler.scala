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
package org.apache.flink.api.common.expressions.codegen

import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.common.expressions.Expression
import org.apache.flink.api.common.expressions.typeinfo.RowTypeInfo
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo

/**
 * Base class for unary and binary result assembler code generators.
 */
abstract class GenerateResultAssembler[R](cl: ClassLoader)
  extends ExpressionCodeGenerator[R](cl = cl) {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  def createResult[T](
      resultTypeInfo: CompositeType[T],
      outputFields: Seq[(Expression, String)]) = {

    val resultType = typeTermForTypeInfo(resultTypeInfo)

    val fieldsCode = outputFields map {
      case (from, to) =>
        val code = generateExpression(from)
        (code, to)
    }

    resultTypeInfo match {
      case ri: RowTypeInfo =>

        val resultSetters: Seq[Tree] = fieldsCode.zipWithIndex map {
          case ((fieldCode, to), i) =>
            q"""
              out.setField($i, { ..${fieldCode.code}; ${fieldCode.resultTerm} })
            """
        }

        q"""
          ..$resultSetters
          out
        """

      case cc: CaseClassTypeInfo[_] =>
        val resultFields: Seq[Tree] = fieldsCode map {
          case (fieldCode, to) =>
            q"{ ..${fieldCode.code}; ${fieldCode.resultTerm}}"
        }
        q"""
          new $resultType(..$resultFields)
        """
    }
  }

}
