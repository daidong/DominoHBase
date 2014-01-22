package org.apache.hadoop.hbase.trigger.example;

import org.apache.hadoop.hbase.trigger.Trigger;
import org.apache.hadoop.hbase.trigger.TriggerConf;
import org.apache.hadoop.hbase.trigger.TriggerConfigured;
import org.apache.hadoop.hbase.trigger.TriggerRunner;
import org.apache.hadoop.hbase.trigger.TriggerTool;

/**
 * 
 * @author daidong
 * The KMeans implementation is based on the MapReduce iterative version of 
 * KMeans. There are two tables: table 'vectors' and table 'clusterings'.'vectors'
 * stores all the vectors we need to cluster. Assuming each of them contains n 
 * dimensions. Table 'clustering' contains k clusterings.
 * 
 * 'vectors':
 * __________________________
 * row-key |     value      |
 *         |     value      |
 * --------------------------
 *    v0   |  3  |  2 | ... |
 *    v1   |  3  |  1 | ... |
 * __________________________
 *
 * 'clusterings'
 * _____________________________________
 * row-key |             vectors       |
 *         |  v1 | v2 | ... |  center  |
 * -------------------------------------
 *    c0   |  v1 | v2 | ... |     c1   |
 * _____________________________________
 */
public class KMeans extends TriggerConfigured implements TriggerTool {

  public int run(String[] args) throws Exception{
    TriggerConf tmp = (TriggerConf)this.getConf();
    
    Trigger tg2 = new Trigger(tmp, "VectorMonitor", "vectors", "value", "value", 
        "org.apache.hadoop.hbase.trigger.example.VectorMonitor");
    tg2.submit();
  
    Trigger tg1 = new Trigger(tmp, "ClusteringMonitor1", "clusterings" ,"vectors", "*" ,
        "org.apache.hadoop.hbase.trigger.example.ClusteringMonitor1", "INITIALWITHCONVERGE");
    tg1.submit();
 
   
    return 0;
  }

  public static void main(String[] args) throws Exception {
    TriggerRunner.run(new TriggerConf(), new KMeans(), args);
    
    
  }

}
