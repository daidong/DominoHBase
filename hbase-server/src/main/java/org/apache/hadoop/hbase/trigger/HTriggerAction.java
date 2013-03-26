package org.apache.hadoop.hbase.trigger;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-1-15
 * Time: 下午11:32
 * To change this template use File | Settings | File Templates.
 */
public abstract class HTriggerAction{
    public String TestAlive(){
      return "Lives";
    }
    public abstract void action(HTriggerEvent hte);
    public abstract boolean filter(HTriggerEvent hte);
}
