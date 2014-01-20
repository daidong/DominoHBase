package org.apache.hadoop.hbase.trigger.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.trigger.AccHTriggerAction;
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
 * There are two tables in this implementation: table 'wiki' and 'words'
 * Table wiki stores all the wiki pages we need to count like this:
 * __________________________
 * row-key |     content    |
 *         |  en | zh | ... |
 * -------------------------- 
 * items   |""   | " "| ... |
 * __________________________
 * 
 * Table words accumulates the intermidiate results and the final word count
 * __________________________________
 * row-key |     acc    |   count   |
 *         |w1|w2| ...  |   value   |
 * ----------------------------------
 * words   |1 |1 | ...  |   123     |
 * ----------------------------------
 * 
 * To optimize, in the TextMonitor trigger, we actually calculate the word count 
 * for a single item. 
 */
public class WordCount extends TriggerConfigured implements TriggerTool  {

  public class TextMonitor extends HTriggerAction{

    private HTable wordTable = null;
    public TextMonitor(){
      try {
        Configuration conf = HBaseConfiguration.create();
        this.wordTable = new HTable(conf, "words".getBytes());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    @Override
    public void action(HTriggerEvent hte) {
      // TODO Auto-generated method stub
      byte[] item = hte.getRowKey();
      String content = new String(hte.getNewValue());
      String[] words = content.split(" ");
      HashMap<String, Integer> localCount = new HashMap<String, Integer>();
      for (String word : words){
        if (localCount.containsKey(word)){
          int id = localCount.get(word);
          localCount.put(word, id+1);
        } else {
          localCount.put(word, 1);
        }
      }
      
      ArrayList<Put> ps = new ArrayList<Put>();
      for (String word : localCount.keySet()){
        Put p = new Put(word.getBytes());
        int c = localCount.get(word);
        
        p.add("acc".getBytes(), item, this.getCurrentRound(), Bytes.toBytes(c));
        ps.add(p);
      }
      
      try {
        wordTable.put(ps);
        wordTable.flushCommits();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public boolean filter(HTriggerEvent hte) {
      return true;
    }
    
  }
  
  public class WordMonitor extends AccHTriggerAction{

    private HTable wordTable = null;
    public WordMonitor(){
      try {
        Configuration conf = HBaseConfiguration.create();
        this.wordTable = new HTable(conf, "words".getBytes());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    @Override
    public void action(HTriggerEvent hte) {
      Map<byte[], byte[]> nodes = this.getReader().GetMapValues();
      int count = 0;
      for (byte[] c : nodes.values()){
        count += Bytes.toInt(c);
      }
      
      Put p = new Put(hte.getRowKey());
      p.add("count".getBytes(), "value".getBytes(), this.getCurrentRound(), Bytes.toBytes(count));
      try {
        wordTable.put(p);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public boolean filter(HTriggerEvent hte) {
      return true;
    }
    
  }
  public static void main(String[] args) throws Exception {
    TriggerRunner.run(new TriggerConf(), new WordCount(), args);
  }

  @Override
  public int run(String[] args) throws Exception {

    TriggerConf tmp = (TriggerConf)this.getConf();
    
    Trigger tg2 = new Trigger(tmp, "TextMonitor", "wikis", "content", "en", 
        "org.apache.hadoop.hbase.trigger.example.TextMonitor", "INITIAL");
    tg2.submit();
    
    Trigger tg1 = new Trigger(tmp, "WordMonitor", "words" ,"acc", "*" ,
        "org.apache.hadoop.hbase.trigger.example.WordMonitor", "ACCUMULATOR");
    tg1.submit();

   
    return 0;
  }

}
