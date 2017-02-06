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

import org.junit.Test;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.plan.physical.PhysicalPlan;

/**
 *
 */
public class ApplicationTest
{
  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.addResource("dt-site-pi.xml");
    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);

  }

  @Test
  public void testUnifiersAttributes()
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new MockStorageAgent());
    Configuration conf = new Configuration();
    //conf.set("dt.operator.picalc.port.output.unifier.attr.TIMEOUT_WINDOW_COUNT", "400");

    Application app = new Application();
    LogicalPlanConfiguration planConf = new LogicalPlanConfiguration(conf);
    planConf.prepareDAG(dag, app, "piApp");

    PhysicalPlan plan = new PhysicalPlan(dag, new TestPlanContext());
    System.out.println("finished");
  }
}
