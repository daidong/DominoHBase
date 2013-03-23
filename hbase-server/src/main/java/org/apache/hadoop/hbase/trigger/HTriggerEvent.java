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
    private HTriggerKey htk;

    public HTriggerEvent(HTriggerKey htk, long tsn, byte[] vn, long tso, byte[] vo){
        this.htk = htk;
        this.currTS = tsn;
        this.newValue = vn;
        this.oldValue = vo;
        this.lastTS = tso;

    }
    
    public HTriggerKey getEventTriggerKey(){
        return this.htk;
    }
    
    public byte[] getNewValue(){
      return this.newValue;
    }
    public long getNewTS(){
      return this.currTS;
    }
    public byte[] getOldValue(){
      return this.oldValue;
    }
    public long getOldTS(){
      return this.lastTS;
    }
}
