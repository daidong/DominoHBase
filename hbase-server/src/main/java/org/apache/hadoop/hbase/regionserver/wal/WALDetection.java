package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.trigger.HTriggerEvent;
import org.apache.hadoop.hbase.trigger.HTriggerKey;
import org.apache.hadoop.hbase.trigger.LocalTriggerManage;
import org.apache.hadoop.hbase.trigger.HTriggerEventQueue;

import java.util.List;

/**
 * @author daidong
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-3-2
 * Time: 6:46
 * To change this template use File | Settings | File Templates.
 */
public class WALDetection {

    public static boolean checkDispatch(byte[] tableName, WALEdit currWal){
        List<KeyValue> syncPairs = currWal.getKeyValues();

        for (KeyValue kv : syncPairs){
            byte[] columnFamily = kv.getFamily();
            byte[] column = kv.getQualifier();
            HTriggerKey triggerMeta = new HTriggerKey(tableName, columnFamily, column);
            if (LocalTriggerManage.containsTrigger(triggerMeta)){
                byte[] values = kv.getValue();
                long ts = kv.getTimestamp();
                HTriggerKey key = new HTriggerKey(tableName, columnFamily, column);
                HTriggerEvent firedEvent = new HTriggerEvent(key, ts, values, ts, values);
                HTriggerEventQueue.append(firedEvent);
            }
        }
        return true;
    }
}
