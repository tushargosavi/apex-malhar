package com.datatorrent.demos.wordcount.schedular;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.stram.plan.logical.mod.DAGChangeSet;

public class FileStatListener implements StatsListener, Serializable
{
  private static final Logger LOG = LoggerFactory.getLogger(FileStatListener.class);

  public FileStatListener() { }

  DAGChangeSet getWordCountDag()
  {
    DAGChangeSet dag = new DAGChangeSet();
    LineByLineFileInputOperator reader = dag.addOperator("Reader", new LineByLineFileInputOperator());
    List<StatsListener> listeners = new ArrayList<>();
    listeners.add(this);
    dag.getMeta(reader).getAttributes().put(Context.OperatorContext.STATS_LISTENERS, listeners);
    reader.setDirectory("/user/hadoop/data");
    LineSplitter splitter = dag.addOperator("SplitteR", new LineSplitter());
    UniqueCounter<String> counter = dag.addOperator("Counter", new UniqueCounter<String>());
    ConsoleOutputOperator out = dag.addOperator("Output", new ConsoleOutputOperator());
    dag.addStream("s1", reader.output, splitter.input);
    dag.addStream("s2", splitter.words, counter.data);
    dag.addStream("s3", counter.count, out.input);
    return dag;
  }

  private boolean dagStarted = false;
  private int counter = 0;

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    counter++;
    for(Stats.OperatorStats ws: stats.getLastWindowedStats()) {
      Integer value = (Integer)ws.metrics.get("pendingFiles");
      LOG.info("stats recevied for {} pendingFiles {} counter {}", stats.getOperatorId(), value, counter);
      if (value != null  && value > 100 && !dagStarted && counter > 120) {
        dagStarted = true;
        Response resp = new Response();
        resp.dag = getWordCountDag();
        counter = 0;
        return resp;
      }
    }
    return null;
  }

}
