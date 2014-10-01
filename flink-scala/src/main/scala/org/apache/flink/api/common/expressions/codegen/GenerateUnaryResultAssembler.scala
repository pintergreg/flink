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

/**
 * Code generator for assembling the result of a unary operation.
 */
class GenerateUnaryResultAssembler[I, O](
    inputTypeInfo: CompositeType[I],
    resultTypeInfo: CompositeType[O],
    outputFields: Seq[(Expression, String)],
    cl: ClassLoader)
  extends GenerateResultAssembler[(I, O) => O](cl = cl) {
  import scala.reflect.runtime.universe._

  override protected def generateInternal(): ((I, O) => O) = {

    val inputType = typeTermForTypeInfo(inputTypeInfo)
    val resultType = typeTermForTypeInfo(resultTypeInfo)

    val fieldsCode = outputFields map {
      case (from, to) =>
        val code = generateExpression(from)
        (code, to)
    }

    val resultCode = createResult(resultTypeInfo, outputFields)

    val code: Tree =
      q"""
        (input0: $inputType, out: $resultType) => {
          $resultCode
        }
      """

    println(s"Generated unary result-assembler:\n$code")
    toolBox.eval(code).asInstanceOf[(I, O) => O]
  }
}
