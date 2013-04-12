package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.trigger.HTriggerEvent;
import org.apache.hadoop.hbase.trigger.HTriggerKey;
import org.apache.hadoop.hbase.trigger.LocalTriggerManage;
import org.apache.hadoop.hbase.trigger.HTriggerEventQueue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author daidong Created with IntelliJ IDEA. User: daidong Date: 13-3-2 Time:
 *         6:46 To change this template use File | Settings | File Templates.
 */
public class WALDetection {

  public static boolean checkDispatch(HRegionInfo info, byte[] tableName, WALEdit currWal) {
    //System.out.println("in checkDispatch, tableName: " + new String(tableName));
    List<KeyValue> syncPairs = currWal.getKeyValues();
    for (KeyValue kv : syncPairs) {
      //byte[] rowKey = kv.getKey();
      byte[] rowKey = kv.getRow();
      byte[] columnFamily = kv.getFamily();
      byte[] column = kv.getQualifier();
      
      HTriggerKey triggerMeta = new HTriggerKey(tableName, columnFamily, column);
      System.out.println("processing trigger key: " + triggerMeta.toString() + " at Row: " + new String(rowKey));
      //System.out.println("current registered trigger key: " + LocalTriggerManage.prettyPrint());
      
      if (LocalTriggerManage.containsTrigger(triggerMeta)) {
        byte[] oldValues = null;
        byte[] values = null;
        long oldTs, ts;
        values = kv.getValue();
        ts = kv.getTimestamp();
        oldValues = values;
        oldTs = ts;
        
        try {
          //if contain this trigger, we construct the old value;
          HRegion r = info.theRegion;
          if (r != null){
            Get get = new Get(rowKey);
            get.addFamily(columnFamily);
            Result result = r.get(get, null);
            
            /**
             * No element yet
             */
            if (result.size() == 0){
              oldTs = 0;
              oldValues = "0".getBytes();
            } else {
              KeyValue[] olds = result.raw();
              System.out.println("olds: " + olds.length);
              oldValues = olds[0].getValue();
              oldTs = olds[0].getTimestamp();
            }
          }
          
          System.out.println("this update fires a trigger: values: " + new String(values, "utf-8") + " | "
              + "old values: " + new String(oldValues, "utf-8"));
          
          
          HTriggerKey key = new HTriggerKey(tableName, columnFamily, column);
          HTriggerEvent firedEvent = new HTriggerEvent(key, rowKey, ts, values, oldTs, oldValues);
          HTriggerEventQueue.append(firedEvent);
        } catch (UnsupportedEncodingException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (IOException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }
    }
    return true;
  }
}
