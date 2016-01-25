package com.datatorrent.contrib.storm;

import java.util.List;

import com.datatorrent.api.DefaultOutputPort;

import backtype.storm.spout.ISpoutOutputCollector;

public class SpoutCollector extends DefaultOutputPort implements ISpoutOutputCollector
{

  @Override
  public List<Integer> emit(String s, List<Object> list, Object o)
  {
    return null;
  }

  @Override
  public void emitDirect(int i, String s, List<Object> list, Object o)
  {

  }

  @Override
  public void reportError(Throwable throwable)
  {

  }
}
