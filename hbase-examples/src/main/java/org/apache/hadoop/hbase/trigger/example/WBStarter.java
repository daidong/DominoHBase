package org.apache.hadoop.hbase.trigger.example;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import weibo4j.Comments;
import weibo4j.Timeline;
import weibo4j.model.Comment;
import weibo4j.model.CommentWapper;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;
import weibo4j.model.WeiboException;

public class WBStarter {

  /**
   * @param args
   */
  public static void main(String[] args) {
    final String accessToken = "2.00sNQEpBRmeFHDdea634fa78QtRBLC";
    HTable htclient = null;
    byte[] tableName = "WBContent".getBytes();
    try {
        Configuration conf = HBaseConfiguration.create();
        htclient = new HTable(conf, tableName);
        Timeline tl = new Timeline();
        tl.client.setToken(accessToken);
        StatusWapper status = tl.getUserTimelineByUid("1617676882");
        ArrayList<Put> puts = new ArrayList<Put>();

        for (Status s : status.getStatuses()) {
            //get new msgs and writ into hbase table
            //@TODO: there is a big problem, HTable is too heavy for frequent processing,
            //One possible way to do this is give Action Instance an init method, which allow users do some heavy init        
            //we need some light weight tools to access table in HBase.

            byte[] msgId = s.getId().getBytes();
            Get g = new Get(msgId);
            Result r = htclient.get(g);
            if (true){
            //if (r.isEmpty()) {
                //if this message does not exist, we add it. else, do nothing.
                Put p = new Put(msgId);
                p.add("content".getBytes(), "con".getBytes(), s.getText().getBytes());
                puts.add(p);
            }
        }
        htclient.put(puts);
        System.out.println("----------------------------");
        
        Comments cm = new Comments();
        cm.setToken(accessToken);
        CommentWapper comment;
        String msgId = "3562928269218422";
        comment = cm.getCommentById(msgId);
        for(Comment c : comment.getComments()){
          System.out.println("Get one comment: " + c.getText());
          System.out.println(c.getUser().getId());
        }
    } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    } catch (WeiboException e) {
        e.printStackTrace();
    }

  }

}