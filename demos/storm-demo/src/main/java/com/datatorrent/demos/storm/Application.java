/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.demos.storm;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.storm.BoltWrapper;
import com.datatorrent.contrib.storm.SpoutWrapper;
import com.datatorrent.contrib.storm.StormTupleStreamCodec;

@ApplicationAnnotation(name = "storm-demo")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    SpoutWrapper input = new SpoutWrapper();
    input.setName("sentence");
    input.setSpout(new WordSpout());

    BoltWrapper tokens = new BoltWrapper();
    tokens.setName("tokens");
    tokens.setBolt(new BoltTokenizer());

    BoltWrapper counter = new BoltWrapper();
    counter.setName("counter");
    counter.setBolt(new BoltCounter());

    BoltWrapper sink = new BoltWrapper();
    sink.setName("output");
    sink.setBolt(new BoltPrintSink(new TupleOutputFormatter()));

    dag.addOperator("input", input);
    dag.addOperator("tokenizer", tokens);
    dag.addOperator("counter", counter);
    dag.addOperator("sink", sink);
    dag.setInputPortAttribute(counter.input, PortContext.STREAM_CODEC, new StormTupleStreamCodec(new int[]{0}));
    dag.addStream("input", input.output, tokens.input);
    dag.addStream("word-count", tokens.output, counter.input);
    dag.addStream("output", counter.output, sink.input);

  }

}
