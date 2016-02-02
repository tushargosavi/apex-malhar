package com.datatorrent.contrib.storm;

import java.io.Serializable;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;

public class BoltOutputCollectorWrapper extends OutputCollector implements Serializable
{

  public BoltOutputCollectorWrapper(IOutputCollector delegate)
  {
    super(delegate);
    // TODO Auto-generated constructor stub
  }

  public BoltOutputCollectorWrapper()
  {
    super(null);
  }

}
