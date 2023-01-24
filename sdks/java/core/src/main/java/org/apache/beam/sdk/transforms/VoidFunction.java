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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;

/**
 * A function that is invokes on an input value of type {@code InputT} and is {@link Serializable},
 * but has no return data.
 *
 * <p>This is a general function type provided in this SDK, allowing arbitrary {@code Exception}s to
 * be thrown, and matching Java's expectations of a <i>functional interface</i> that can be supplied
 * as a lambda expression or method reference. It is named {@code VoidFunction} because it has no
 * return type. For a {@link Serializable} function that allows data to be returned, check {@link
 * ProcessFunction}.
 *
 * @param <InputT> input value type
 */
@FunctionalInterface
public interface VoidFunction<InputT> extends Serializable {
  /** Invokes this function on the given input. */
  void apply(InputT input) throws Exception;
}
