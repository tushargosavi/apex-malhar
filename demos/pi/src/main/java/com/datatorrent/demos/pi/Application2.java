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
package com.datatorrent.demos.pi;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.stream.DevNullCounter;
import com.datatorrent.lib.stream.StreamMerger;

/**
 * Monte Carlo PI estimation demo : <br>
 * This application computes value of PI using Monte Carlo pi estimation
 * formula.
 * <p>
 * Running Java Test or Main app in IDE:
 *
 * <pre>
 * LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 *
 * Run Success : <br>
 * For successful deployment and run, user should see something like the
 * following output on the console (since the input sequence of random numbers
 * can vary from one run to the next, there will be some variation in the
 * output values):
 *
 * <pre>
 * 3.1430480549199085
 * 3.1423454157782515
 * 3.1431377245508982
 * 3.142078799249531
 * 2013-06-18 10:43:18,335 [main] INFO  stram.StramLocalCluster run - Application finished.
 * </pre>
 *
 * Application DAG : <br>
 * <img src="doc-files/Application.gif" width=600px > <br>
 * <br>
 *
 * Streaming Window Size : 1000 ms(1 Sec) <br>
 * Operator Details : <br>
 * <ul>
 * <li><b>The rand Operator : </b> This operator generates random integer
 * between 0-30k. <br>
 * Class : {@link com.datatorrent.lib.testbench.RandomEventGenerator}<br>
 * StateFull : No</li>
 * <li><b>The calc operator : </b> This operator computes value of pi using
 * monte carlo estimation. <br>
 * Class : com.datatorrent.demos.pi.PiCalculateOperator <br>
 * StateFull : No</li>
 * <li><b>The operator Console: </b> This operator just outputs the input tuples
 * to the console (or stdout). You can use other output adapters if needed.<br>
 * </li>
 * </ul>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name = "PiDemo2")
public class Application2 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomEventGenerator rand1 = dag.addOperator("rand1", new RandomEventGenerator());
    rand1.setWindowsToWait(100);
    RandomEventGenerator rand2 = dag.addOperator("rand2", new RandomEventGenerator());
    rand2.setWindowsToWait(100);
    StreamMerger merge = dag.addOperator("merge", new StreamMerger());
    DevNullCounter console = dag.addOperator("console", new DevNullCounter());

    dag.addStream("s1", rand1.integer_data, merge.data1).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("s2",rand2.integer_data, merge.data2).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("s3", merge.out, console.data);
  }

}
