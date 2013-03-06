package org.apache.hadoop.hbase.trigger;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-3-2
 * Time: 下午6:49
 * To change this template use File | Settings | File Templates.
 */
public class HTriggerKey {
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
}
