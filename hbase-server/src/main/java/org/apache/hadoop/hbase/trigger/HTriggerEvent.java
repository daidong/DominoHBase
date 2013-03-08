package org.apache.hadoop.hbase.trigger;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-1-15
 * Time: 下午11:31
 * To change this template use File | Settings | File Templates.
 */
public class HTriggerEvent {
    private long currTS;
    private long lastTS;
    private byte[] newValue;
    private byte[] oldValue;
    private HTrigger belongTo;

    public HTriggerEvent(HTrigger ht, long tsn, byte[] vn, long tso, byte[] vo){
        this.belongTo = ht;
        this.currTS = tsn;
        this.newValue = vn;
        this.oldValue = vo;
        this.lastTS = tso;

    }
    public HTriggerEvent(long tsn, byte[] vn, long tso, byte[] vo){
      this.currTS = tsn;
      this.newValue = vn;
      this.oldValue = vo;
      this.lastTS = tso;
    }
    
    public HTrigger getBelongTo(){
        return this.belongTo;
    }
}
