package com.datatorrent.demos.storm;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordSpout extends BaseRichSpout
{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public static final String[] WORDS = new String[] {"To be or not to be that is the question",
    "Whether tis nobler in the mind to suffer" };

  
  private SpoutOutputCollector collector;
  private int counter = 0;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
  {
    this.collector = collector;

  }

  @Override
  public void nextTuple()
  {
    if (this.counter < WORDS.length) {
      this.collector.emit(new Values(WORDS[this.counter++]));
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } else {
      this.counter = 0;
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("sentence"));

  }

}
