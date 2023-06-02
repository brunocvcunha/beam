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
package org.apache.beam.examples.timeseries;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.timeseries.fs.TickerStream;
import org.apache.beam.sdk.extensions.timeseries.fs.example.NaiveOrderBook;
import org.apache.beam.sdk.extensions.timeseries.fs.example.Order;
import org.apache.beam.sdk.extensions.timeseries.fs.example.Tick;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleStream {

  private static final Logger LOG = LoggerFactory.getLogger(ExampleStream.class);

  public interface TestOptions extends GcpOptions {

    String getPubSubTopic();

    void setPubSubTopic(String value);
  }

  public static void main(String[] args) throws Exception {
    startInjector(args);
    startPipeline(args);
  }

  static void startInjector(String[] args) {

    TestOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().create().as(TestOptions.class);
    pipelineOptions.setRunner(DataflowRunner.class);

    DataflowPipelineOptions dataflowPipelineOptions = pipelineOptions.as(
        DataflowPipelineOptions.class);
    dataflowPipelineOptions.setProject("cloud-teleport-testing");
    dataflowPipelineOptions.setRegion("us-central1");
    dataflowPipelineOptions.setEnableStreamingEngine(true);

    dataflowPipelineOptions.setApiRootUrl(
        "https://dataflow-bvolpato-staging.sandbox.googleapis.com/");
    dataflowPipelineOptions.setPubsubRootUrl("https://staging-pubsub.sandbox.googleapis.com");
    pipelineOptions.setPubSubTopic("projects/cloud-teleport-testing/topics/orderbook-test-staging");

    // pipelineOptions.setPubSubTopic(
    //     "projects/cloud-teleport-testing/topics/orderbook-test-prod");

    Pipeline p = Pipeline.create(pipelineOptions);

    PCollection<Long> numbers =
        p.apply(GenerateSequence.from(0).withRate(100, Duration.millis(100)));
    numbers
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(x -> String.format("{\"key\" : %s, \"value\" : %s}", x % 100, x)))
        .apply(PubsubIO.writeStrings().to(pipelineOptions.getPubSubTopic()));

    p.run();
  }

  static void startPipeline(String[] args) {

    TestOptions pipelineOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().create().as(TestOptions.class);
    pipelineOptions.setRunner(DataflowRunner.class);

    DataflowPipelineOptions dataflowPipelineOptions = pipelineOptions.as(
        DataflowPipelineOptions.class);
    dataflowPipelineOptions.setProject("cloud-teleport-testing");
    dataflowPipelineOptions.setRegion("us-central1");
    dataflowPipelineOptions.setEnableStreamingEngine(true);

    pipelineOptions.setPubSubTopic(
        "projects/cloud-teleport-testing/subscriptions/orderbook-test-staging-sub");
    dataflowPipelineOptions.setApiRootUrl(
        "https://dataflow-bvolpato-staging.sandbox.googleapis.com/");
    dataflowPipelineOptions.setPubsubRootUrl("https://staging-pubsub.sandbox.googleapis.com");
    // pipelineOptions.setPubSubTopic(
    //     "projects/cloud-teleport-testing/subscriptions/orderbook-test-prod-sub");

    dataflowPipelineOptions.setDataflowWorkerJar(
        "/Users/bvolpato/githubworkspace/beam/runners/google-cloud-dataflow-java/worker/build/libs/beam-runners-google-cloud-dataflow-java-legacy-worker-2.49.0-SNAPSHOT.jar");
    Pipeline p = Pipeline.create(pipelineOptions);

    p.apply(PubsubIO.readStrings().fromSubscription(pipelineOptions.getPubSubTopic()))
        .apply(ParDo.of(new ConvertStringToTick()))
        .apply(
            TickerStream.create(
                TickerStream.Mode.MULTIPLEX_STREAM,
                NaiveOrderBook.class,
                SerializableCoder.of(Tick.class)))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))));

    p.run();
  }

  static class ConvertStringToTick extends DoFn<String, KV<Long, KV<String, Tick>>> {

    // Create a POJO class.
    static class Foo {

      private String key;
      private int value;

      public Foo() {
        this("", 0);
      }

      public Foo(String key, int value) {
        this.key = key;
        this.value = value;
      }

      public String getKey() {
        return key;
      }

      public void setKey(String key) {
        this.key = key;
      }

      public int getValue() {
        return value;
      }

      public void setValue(int value) {
        this.value = value;
      }
    }

    static ObjectMapper mapper = new ObjectMapper();

    @ProcessElement
    public void process(@Element String input, OutputReceiver<KV<Long, KV<String, Tick>>> output) {

      try {
        // Use the Jackson object to deserialize the JSON string into the POJO object.
        Foo foo = mapper.readValue(input, Foo.class);
        Tick tick = new Tick();
        Order order =
            new Order(foo.key, 1.00, true, foo.value % 2 == 0 ? Order.TYPE.ADD : Order.TYPE.CANCEL);
        tick.setGlobalSequence((long) foo.value);
        tick.setId(foo.key);
        tick.setOrder(order);
        output.output(KV.of((long) foo.value, KV.of(foo.key, tick)));
      } catch (Exception e) {
        LOG.error("Error processing element {}", input, e);
      }
    }
  }
}