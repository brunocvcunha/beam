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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code PTransform}s for running a simple function over the elements of a {@link PCollection},
 * without any results.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class OutputElements<InputT> extends PTransform<PCollection<? extends InputT>, PDone> {

  private final transient @Nullable TypeDescriptor<InputT> inputType;

  private final VoidFunction<InputT> fn;

  private OutputElements(
      VoidFunction<InputT> fn, @Nullable TypeDescriptor<InputT> inputType) {
    this.fn = fn;
    this.inputType = inputType;
  }

  /**
   * For a {@code VoidFunction<InputT>} {@code fn}, returns a {@code PTransform} that takes an input
   * {@code PCollection<InputT>} and calls {@code fn.apply(v)} for every element {@code v} in the
   * input.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * words.apply(
   *     OutputElements.via(System.out::println);
   * }</pre>
   */
  public <NewInputT> OutputElements<NewInputT> via(VoidFunction<NewInputT> fn) {
    return new OutputElements<>(fn, TypeDescriptors.inputOf(fn));
  }

  @Override
  public PDone expand(PCollection<? extends InputT> input) {
    checkNotNull(fn, "Must specify a function on MapElements using .via()");
    input.apply(
        "Map",
        ParDo.of(
            new OutputDoFn() {
              @ProcessElement
              public void processElement(@Element InputT element) throws Exception {
                ((ProcessFunction<InputT, Void>) fn).apply(element);
              }
            }));
    return PDone.in(input.getPipeline());
  }

  /** A DoFn implementation that handles a trivial call without output. */
  private abstract class OutputDoFn extends DoFn<InputT, Void> {
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.delegate(OutputElements.this);
    }

    @Override
    public TypeDescriptor<InputT> getInputTypeDescriptor() {
      return inputType;
    }

    @Override
    public TypeDescriptor<Void> getOutputTypeDescriptor() {
      return TypeDescriptors.voids();
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("class", fn.getClass()));
    if (fn instanceof HasDisplayData) {
      builder.include("fn", (HasDisplayData) fn);
    }
  }
}
