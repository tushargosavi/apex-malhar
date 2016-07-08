package com.datatorrent.demos.wordcount.schedular;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class FileMonitorOperator extends BaseOperator implements InputOperator
{
  private String pathStr;

  @AutoMetric
  private int pendingFiles;

  private transient Timer timer = new Timer();

  private long scanInterval = 10000;

  private transient Path path;
  private transient FileSystem fs;

  /* current state of the scanner */
  private Set<String> seenFiles = new HashSet<>();
  private Set<String> newFiles = new HashSet<>();
  private String currentPath;

  @Override
  public void emitTuples()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {
      super.setup(context);
      path = new Path(pathStr);
      fs = FileSystem.newInstance(path.toUri(), new Configuration());
      timer.scheduleAtFixedRate(new ScanTask(), scanInterval, 1000);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  class ScanTask extends TimerTask
  {
    @Override
    public void run()
    {
      try {
        FileStatus[] files = fs.listStatus(path);
        for (FileStatus file : files) {
          if (!seenFiles.contains(file.getPath().getName())) {
            newFiles.add(file.getPath().getName());
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void setDagfinished(int stage) {
    seenFiles.addAll(newFiles);
    newFiles.clear();
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    pendingFiles = newFiles.size();
  }

  public String getPathStr()
  {
    return pathStr;
  }

  public void setPathStr(String pathStr)
  {
    this.pathStr = pathStr;
  }

  public long getScanInterval()
  {
    return scanInterval;
  }

  public void setScanInterval(long scanInterval)
  {
    this.scanInterval = scanInterval;
  }
}
