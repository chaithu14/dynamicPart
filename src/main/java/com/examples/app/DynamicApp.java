package com.examples.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="DynamicDemo")
public class DynamicApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    RandomDynamicGenerator input = dag.addOperator("Input", new RandomDynamicGenerator());
    input.setTuplesBlast(10);
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    dag.addStream("InputtoOutput", input.string_data,output.input);
  }
}
