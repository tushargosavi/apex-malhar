package com.datatorrent.demos.wordcount.schedular;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "WordCountApp")
public class WordCountApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FileMonitorOperator monitor = dag.addOperator("Monitor", new FileMonitorOperator());
    monitor.setPathStr("/user/hadoop/data");
    List<StatsListener> listeners = new ArrayList<>();
    listeners.add(new FileStatListener());
    dag.getMeta(monitor).getAttributes().put(Context.OperatorContext.STATS_LISTENERS, listeners);
  }
}
