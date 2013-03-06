package org.apache.hadoop.hbase.trigger;

import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-1-15
 * Time:
 * To change this template use File | Settings | File Templates.
 */
public class HTrigger {
    private long triggerId;
    private long roundId;

    private HTriggerEvent event;
    private HTriggerCond cond;
    private HTriggerAction action;

    public HTrigger(){
        this.triggerId = UUID.randomUUID().getMostSignificantBits();
    }

    public HTrigger(HTriggerEvent e, HTriggerCond c,
                    HTriggerAction a){
        this();
        this.roundId = 0;

        this.event = e;
        this.cond = c;
        this.action = a;
    }

    public long getTriggerId(){
        return this.triggerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HTrigger hTrigger = (HTrigger) o;

        if (triggerId != hTrigger.triggerId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (triggerId ^ (triggerId >>> 32));
    }
}
