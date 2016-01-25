package com.datatorrent.contrib.storm;

import java.util.Collection;
import java.util.List;

import com.datatorrent.api.DefaultOutputPort;

import backtype.storm.task.IOutputCollector;
import backtype.storm.tuple.Tuple;

public class BoltCollector extends DefaultOutputPort implements IOutputCollector
{

  @Override
  public List<Integer> emit(String s, Collection<Tuple> collection, List<Object> list)
  {
    return null;
  }

  @Override
  public void emitDirect(int i, String s, Collection<Tuple> collection, List<Object> list)
  {

  }

  @Override
  public void ack(Tuple tuple)
  {

  }

  @Override
  public void fail(Tuple tuple)
  {

  }

  @Override
  public void reportError(Throwable throwable)
  {

  }
}
