package org.apache.hadoop.hbase.trigger;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-3-2
 * Time: 下午6:49
 * To change this template use File | Settings | File Templates.
 */
public class HTriggerKey {
    @Override
    public String toString() {
      return "HTriggerKey [tableName=" + new String(tableName)
          + ", columnFamily=" + new String(columnFamily) + ", column="
          + new String(column) + "]";
    }

    private byte[] tableName;
    private byte[] columnFamily;
    private byte[] column;

    public HTriggerKey(byte[] tableName, byte[] columnFamily, byte[] column){
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.column = column;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HTriggerKey that = (HTriggerKey) o;
        
        if (!Arrays.equals(column, that.column)) return false;
        if (!Arrays.equals(columnFamily, that.columnFamily)) return false;
        if (!Arrays.equals(tableName, that.tableName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tableName != null ? Arrays.hashCode(tableName) : 0;
        result = 31 * result + (columnFamily != null ? Arrays.hashCode(columnFamily) : 0);
        result = 31 * result + (column != null ? Arrays.hashCode(column) : 0);
        return result;
    }
    
    public static void main(String[] args){
      String n1 = "hello";
      String cf1 = "content";
      String c1 = "zh";
      
      String n2 = "hello";
      String cf2 = "content";
      String c2 = "zh";
      
      HTriggerKey ht1 = new HTriggerKey(n1.getBytes(), cf1.getBytes(), c1.getBytes());
      HTriggerKey ht2 = new HTriggerKey(n2.getBytes(), cf2.getBytes(), c2.getBytes());
      
      System.out.println(ht1.hashCode());
      System.out.println(ht2.hashCode());
      
      System.out.println(ht1==ht2);
      System.out.println(ht1.equals(ht2));
      
      HashMap<HTriggerKey, Integer> test = new HashMap<HTriggerKey, Integer>();
      test.put(ht1, 12);
      System.out.println(test.get(ht2));
    }
}
