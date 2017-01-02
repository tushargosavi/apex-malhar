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

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.BaseOperator;

public class RandomShutdownOperator extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(RandomShutdownOperator.class);
  int windowsToWait = 120;
  int windowsCompleted = 0;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    Random rand = new Random();
    windowsToWait = rand.nextInt(300) + windowsToWait;
    LOG.info("The operator will shutdown after {} windows ", windowsToWait);
  }

  @Override
  public void endWindow()
  {
    windowsCompleted++;
    if (windowsCompleted > windowsToWait) {
      windowsCompleted = 0;
      LOG.info("Total windows processed {}, shutting down operator now ", windowsCompleted);
      throw new ShutdownException();
    }
    super.endWindow();
  }

  public int getWindowsToWait()
  {
    return windowsToWait;
  }

  public void setWindowsToWait(int windowsToWait)
  {
    this.windowsToWait = windowsToWait;
  }
}
