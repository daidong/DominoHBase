package org.apache.hadoop.hbase.trigger;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

public class WritePreparedInst {

	private static final Log LOG = LogFactory.getLog(WritePrepared.class);

	private Configuration conf = HBaseConfiguration.create();

	private HTriggerAction belongTo = null;

	LinkedBlockingQueue<WriteUnit> cachedWrites = new LinkedBlockingQueue<WriteUnit>();

	//One Region Server shares the name to table mappping.
	private ConcurrentHashMap<byte[], HTable> nameToTableMap = new ConcurrentHashMap<byte[], HTable>();

	public WritePreparedInst(HTriggerAction trigger){
		this.belongTo = trigger;
	}
	
	public boolean append(WriteUnit write){
		try{
			cachedWrites.put(write);
		} catch (Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private HTable getOrNewHTableInstance(byte[] name) throws IOException{
		HTable ins = nameToTableMap.get(name);
		if (ins == null){
			ins = new HTable(conf, name);
			nameToTableMap.put(name, ins);
		}
		return ins;
	}

	public boolean flush(HTriggerAction action){
		System.out.println("Action " + action + " flush");
		try{
			WriteUnit w = cachedWrites.poll();
			HTable ins = null;
			while (w != null){
				ins = getOrNewHTableInstance(w.getTableName());
				ins.put(w.getPut());
				w = cachedWrites.poll();
			}
			ins.flushCommits();
		} catch (Exception e){
			LOG.info("Exceptions While Calling HTable's Put");
		}
		return true;
	}
	
}
