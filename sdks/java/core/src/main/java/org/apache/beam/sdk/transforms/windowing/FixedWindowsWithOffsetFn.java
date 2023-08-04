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
package org.apache.beam.sdk.transforms.windowing;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link WindowFn} that windows values into fixed-size timestamp-based windows, allowing a
 * per-element offset function.
 *
 * <p>For example, in order to partition the data into 10 minute windows, and separate in offsets of
 * 0 to 5 minutes for each key:
 *
 * <pre>{@code
 * PCollection<KV<Integer, Integer>> items = ...;
 * PCollection<KV<Integer, Integer>> windowedItems = items.apply(
 *   Window.<KV<Integer, Integer>>into(FixedWindowsWithOffsetFn.<>of(Duration.standardMinutes(10),
 *    kv -> Duration.ofMinutes(kv.getKey() % 5))
 * );
 * }</pre>
 *
 * This will make sure that there are windows of 10 minutes, but each key fire at a distinct minute,
 * instead of all at the same time.
 */
public class FixedWindowsWithOffsetFn<T> extends NonMergingWindowFn<T, IntervalWindow> {

  /** Size of this window. */
  private final Duration size;

  /**
   * Function to resolve the offset of this window. Windows start at time N * size +
   * offsetFn(element), where 0 is the epoch.
   */
  private final SerializableFunction<T, Duration> offsetFn;

  /**
   * Partitions the timestamp space into half-open intervals of the form [N * size +
   * offsetFn(element), (N + 1) * size + offsetFn(element)), where 0 is the epoch.
   *
   * @throws IllegalArgumentException if offset is not in [0, size)
   */
  public static <T> FixedWindowsWithOffsetFn<T> of(
      Duration size, SerializableFunction<T, Duration> offsetFn) {
    return new FixedWindowsWithOffsetFn<>(size, offsetFn);
  }

  private FixedWindowsWithOffsetFn(Duration size, SerializableFunction<T, Duration> offsetFn) {
    this.size = size;
    this.offsetFn = offsetFn;
  }

  @Override
  public List<IntervalWindow> assignWindows(AssignContext context) {
    return Collections.singletonList(assignWindow(context.timestamp(), context.element()));
  }

  private IntervalWindow assignWindow(Instant timestamp, @Nullable T element) {
    Duration offset;
    if (offsetFn != null && element != null) {
      offset = offsetFn.apply(element);
    } else {
      offset = Duration.ZERO;
    }

    Instant start =
        new Instant(
            timestamp.getMillis()
                - timestamp.plus(size).minus(offset).getMillis() % size.getMillis());

    // The global window is inclusive of max timestamp, while interval window excludes its
    // upper bound
    Instant endOfGlobalWindow = GlobalWindow.INSTANCE.maxTimestamp().plus(Duration.millis(1));

    // The end of the window is either start + size if that is within the allowable range, otherwise
    // the end of the global window. Truncating the window drives many other
    // areas of this system in the appropriate way automatically.
    //
    // Though it is curious that the very last representable fixed window is shorter than the rest,
    // when we are processing data in the year 294247, we'll probably have technology that can
    // account for this.
    Instant end =
        start.isAfter(endOfGlobalWindow.minus(size)) ? endOfGlobalWindow : start.plus(size);

    IntervalWindow intervalWindow = new IntervalWindow(start, end);
    return intervalWindow;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("size", size).withLabel("Window Duration"));
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getCoder();
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return this.equals(other);
  }

  @Override
  public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
    if (!this.isCompatible(other)) {
      throw new IncompatibleWindowException(
          other,
          String.format(
              "Only %s objects with the same size and offset are compatible.",
              FixedWindowsWithOffsetFn.class.getSimpleName()));
    }
  }

  @Override
  public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
    return new WindowMappingFn<IntervalWindow>() {
      @Override
      public IntervalWindow getSideInputWindow(BoundedWindow mainWindow) {
        if (mainWindow instanceof GlobalWindow) {
          throw new IllegalArgumentException(
              "Attempted to get side input window for GlobalWindow from non-global WindowFn");
        }
        return assignWindow(mainWindow.maxTimestamp(), null);
      }
    };
  }

  @Override
  public final boolean assignsToOneWindow() {
    return true;
  }

  public Duration getSize() {
    return size;
  }

  public SerializableFunction<T, Duration> getOffsetFn() {
    return offsetFn;
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (!(object instanceof FixedWindowsWithOffsetFn)) {
      return false;
    }
    FixedWindowsWithOffsetFn<?> other = (FixedWindowsWithOffsetFn<?>) object;
    return getSize().equals(other.getSize()) && getOffsetFn().equals(other.getOffsetFn());
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, offsetFn);
  }
}
