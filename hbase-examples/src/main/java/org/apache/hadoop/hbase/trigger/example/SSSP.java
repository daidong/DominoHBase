package org.apache.hadoop.hbase.trigger.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.trigger.HTriggerAction;
import org.apache.hadoop.hbase.trigger.HTriggerEvent;
import org.apache.hadoop.hbase.trigger.Trigger;
import org.apache.hadoop.hbase.trigger.TriggerConf;
import org.apache.hadoop.hbase.trigger.TriggerConfigured;
import org.apache.hadoop.hbase.trigger.TriggerRunner;
import org.apache.hadoop.hbase.trigger.TriggerTool;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author daidong
 *
 * There is one table in this implementation: table 'graph'.
 * Table 'graph' stores the graph as a weighted graph including 
 * the outgoing edge's weight plus all the ingoing edge list. 
 * The distance value is also stored here.
 * ______________________________________________________
 * row-key |     outgoing   |   distance  |  ingoing    |
 *         |  v1 | v2 | ... |     value   |   set       |
 * ------------------------------------------------------
 *    v0   |  3  |  2 | ... |      0      |   NULL      |
 *    v1   | INF |  1 | ... |     INF     |  v0, v4, v2 |
 * ______________________________________________________
 *
 * The algorithm is in fact a asynchronous algorithm. So, we only needs plain trigger here. 
 * Although there is only one table storing all the information we need, we still need two triggers. 
 * Usually the first trigger 'GraphMonitor' will not work unless the graph is chaning. Most 
 * of the work is done by 'DistMoniotor' which asynchronously updates all the connected 
 * vertex's distance start from setting dist(v0, v0) = 0;
 * 
 * The initial value of every element in table 'dist' is INF. The algorithm starts when we set
 * the dist(v0, v0) = 0.
 * 
 * One thing that the Oolong paper just ignored is if you change the 'graph', the algorithm will
 * fail in some situation. For example, if we remove an edge, say E(vi -> vj), from the graph, it may just cause 
 * current shortest path of vj invalide. Then, what about the new sssp of vj? It is hard to tell unless
 * we rerun the whole Dijstra algorithm again. 
 * 
 * Of course, things are different if we actually reduce the distance of an edge or adding new edges
 * or vertexes into the graph. These incremental inputs only affect the vertex which connected with them,
 * The Domino model helps us guarrant that we only need to process the new inputs.
 */
public class SSSP extends TriggerConfigured implements TriggerTool{

  /**
   * @author daidong
   * There is a core trade-off between what we need to get from current event
   * and the performance we are gonna to reduce. This is a totally implementation
   * tips, we need to consider but no space for the paper. 
   *
   */
  public class GraphMonitor extends HTriggerAction {

    @Override
    public void action(HTriggerEvent hte) {
      byte[] v1 = hte.getRowKey();
      //byte[] v2 = hte.get
    }

    @Override
    public boolean filter(HTriggerEvent hte) {
      return true;
    }
    
  }
  
  public class DistMonitor extends HTriggerAction {

    private HTable graphTable = null;
    private HTable distTable = null;
    public DistMonitor(){
      try {
        Configuration conf = HBaseConfiguration.create();
        this.graphTable = new HTable(conf, "graph".getBytes());
        this.distTable = new HTable(conf, "dist".getBytes());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    @Override
    public void action(HTriggerEvent hte) {
      byte[] v = hte.getRowKey();
      int dist = Bytes.toInt(hte.getNewValue());
      
      try {
        Get g = new Get(v);
        Result r = graphTable.get(g);
        Map<byte[], byte[]> og = r.getFamilyMap("outgoing".getBytes());
        ArrayList<Put> ps = new ArrayList<Put>();
        
        for (byte[] vertex : og.keySet()){
          Get g1 = new Get(vertex);
          int current = Bytes.toInt(distTable.get(g1).getValue("distance".getBytes(), "value".getBytes()));
          int ndist = Bytes.toInt(og.get(vertex)) + dist;
          if (ndist < current){
            Put p = new Put(v);
            p.add("distance".getBytes(), "value".getBytes(), this.getCurrentRound(), Bytes.toBytes(ndist));
            ps.add(p);
          }
        }
        
        distTable.put(ps);
        distTable.flushCommits();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    @Override
    public boolean filter(HTriggerEvent hte) {
      return true;
    }
    
  }
  
  
  public static void main(String[] args) throws Exception {
    TriggerRunner.run(new TriggerConf(), new SSSP(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    TriggerConf tmp = (TriggerConf)this.getConf();
    
    
    Trigger tg2 = new Trigger(tmp, "GraphMonitor", "graph", "outgoing", "*", 
        "org.apache.hadoop.hbase.trigger.example.GraphMonitor", "ORDINARY");
    tg2.submit();
    
    
    Trigger tg1 = new Trigger(tmp, "DistMonitor", "graph" ,"distance", "value" ,
        "org.apache.hadoop.hbase.trigger.example.DistMonitor", "ORDINARY");
    tg1.submit();

   
    return 0;
  }

}
