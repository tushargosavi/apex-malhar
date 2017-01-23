/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.demos.pi;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class NumberQueueOperator extends BaseOperator
{
  private static final transient Logger logger = LoggerFactory.getLogger(NumberQueueOperator.class);
  public final List<Integer> queue = new LinkedList();
  public final transient DefaultOutputPort<Integer> outputPort = new DefaultOutputPort<>();
  public int queueSize = 3000000;
  public int count = 0;
  public int emitInterval = 1000;
  public int repeatCount = 1000;
  public final transient DefaultInputPort<Integer> inputPort = new DefaultInputPort<Integer>()
  {
    public void process(Integer tuple)
    {
      for (int i = 0; (i < NumberQueueOperator.this.repeatCount) && (NumberQueueOperator.this.queue
          .size() <= NumberQueueOperator.this.queueSize); i++) {
        NumberQueueOperator.this.queue.add(tuple);
        NumberQueueOperator.this.count += 1;
      }
      Integer v = (Integer)NumberQueueOperator.this.queue.remove(0);
      if (NumberQueueOperator.this.count % NumberQueueOperator.this.emitInterval == 0) {
        NumberQueueOperator.this.outputPort.emit(v);
      }
    }
  };
  private transient boolean isFirstTime = true;
  private transient long currentWindowId = 0L;

  public void setup(Context.OperatorContext context)
  {
    if (this.isFirstTime) {
      logger.debug("queue.size() = {}", new Object[]{Integer.valueOf(this.queue.size())});
      logger.debug("count = {}", new Object[]{Integer.valueOf(this.count)});
    }
    this.isFirstTime = false;
  }

  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.currentWindowId = windowId;
  }

  public void endWindow()
  {
    logger.debug("windowID = {}", new Object[]{Long.valueOf(this.currentWindowId)});
    logger.debug("queue.size() = {}", new Object[]{Integer.valueOf(this.queue.size())});
    logger.debug("count = {}", new Object[]{Integer.valueOf(this.count)});
  }

  public int getQueueSize()
  {
    return this.queueSize;
  }

  public void setQueueSize(int queueSize)
  {
    this.queueSize = queueSize;
  }

  public int getEmitInterval()
  {
    return this.emitInterval;
  }

  public void setEmitInterval(int emitInterval)
  {
    this.emitInterval = emitInterval;
  }

  public int getRepeatCount()
  {
    return this.repeatCount;
  }

  public void setRepeatCount(int repeatCount)
  {
    this.repeatCount = repeatCount;
  }
}

