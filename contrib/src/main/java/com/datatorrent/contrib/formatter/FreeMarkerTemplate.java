package com.datatorrent.contrib.formatter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import freemarker.cache.StringTemplateLoader;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

public class FreeMarkerTemplate extends BaseOperator
{

  private String filePath;

  private String strTemplate;

  private transient Template tmpl;

  public transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

  public transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object o)
    {
      try {
        StringWriter sw = new StringWriter();
        tmpl.process(o, sw);
        sw.flush();
        output.emit(sw.toString());
      } catch (TemplateException | IOException  e) {
        throw new RuntimeException("Unable to populate template ", e);
      }
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    try {

      if (filePath != null) {
        Path path = new Path(filePath);
        Path dataFilePath = new Path(filePath);
        FileSystem fs = FileSystem.newInstance(dataFilePath.toUri(), new Configuration());
        FSDataInputStream in = fs.open(path);
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        IOUtils.copy(in, bao);
        strTemplate = new String(bao.toByteArray());
      }

      if (strTemplate == null) {
        throw new RuntimeException("strTemplate provided is null");
      }

      freemarker.template.Configuration cfg = new freemarker.template.Configuration();
      cfg.setDefaultEncoding("UTF-8");
      cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      StringTemplateLoader loader = new StringTemplateLoader();
      loader.putTemplate("tmpl", strTemplate);
      cfg.setTemplateLoader(loader);
      tmpl = cfg.getTemplate("tmpl");

    } catch (IOException ex) {
      throw new RuntimeException("Unable load template ", ex);
    }

  }

  public String getFilePath()
  {
    return filePath;
  }

  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  public String getStrTemplate()
  {
    return strTemplate;
  }

  public void setStrTemplate(String strTemplate)
  {
    this.strTemplate = strTemplate;
  }
}
