package org.apache.hadoop.hbase.trigger.example;

import org.apache.hadoop.hbase.trigger.HTriggerAction;
import org.apache.hadoop.hbase.trigger.HTriggerEvent;

/**
 * PageRankDist
 * @author daidong
 * This trigger will monitor on table 'wbpages', column-family: 'prvalues', column: 'pr'
 * besides each web page also contains the information of its out links in column-family: 
 * 'outlinks', column:'links' and 'number'.
 * 
 * There is also an 'aggragate pattern' table named 'pagerank-acc', containing column-family: 'prvalues' and 
 * column: 'pr'. 
 * 
 * Basically this trigger finishes two things:
 * 1) monitor on wbpages-prvalues-pr
 * 2) write all page
 */

public class PageRankDist extends HTriggerAction{

  @Override
  public void action(HTriggerEvent hte) {
        
  }

  @Override
  public boolean filter(HTriggerEvent hte) {
    return false;
  }
}
