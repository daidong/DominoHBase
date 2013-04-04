package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.trigger.HTriggerEvent;
import org.apache.hadoop.hbase.trigger.HTriggerKey;
import org.apache.hadoop.hbase.trigger.LocalTriggerManage;
import org.apache.hadoop.hbase.trigger.HTriggerEventQueue;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author daidong Created with IntelliJ IDEA. User: daidong Date: 13-3-2 Time:
 *         6:46 To change this template use File | Settings | File Templates.
 */
public class WALDetection {

  public static boolean checkDispatch(byte[] tableName, WALEdit currWal) {
    //System.out.println("in checkDispatch, tableName: " + new String(tableName));
    List<KeyValue> syncPairs = currWal.getKeyValues();

    for (KeyValue kv : syncPairs) {
      //byte[] rowKey = kv.getKey();
      byte[] rowKey = kv.getRow();
      byte[] columnFamily = kv.getFamily();
      byte[] column = kv.getQualifier();
      HTriggerKey triggerMeta = new HTriggerKey(tableName, columnFamily, column);
      //System.out.println("processing trigger key: " + triggerMeta.toString());
      //System.out.println("current registered trigger key: " + LocalTriggerManage.prettyPrint());
      
      if (LocalTriggerManage.containsTrigger(triggerMeta)) {
        byte[] values = kv.getValue();
        long ts = kv.getTimestamp();
        try {
          System.out.println("this update fires a trigger: values: " + new String(values, "utf-8"));
        } catch (UnsupportedEncodingException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        
        HTriggerKey key = new HTriggerKey(tableName, columnFamily, column);
        HTriggerEvent firedEvent = new HTriggerEvent(key, rowKey, ts, values, ts, values);
        HTriggerEventQueue.append(firedEvent);
      }
    }
    return true;
  }
}
