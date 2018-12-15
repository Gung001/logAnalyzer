package com.lxgy.storm.rpc.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author Gryant
 */
public class LocalDRPCTopology {

    private static class MyBolt extends BaseRichBolt {

        OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {

            Object requestId = input.getValue(0);
            String name = input.getString(1);

            /**
             * 业务处理 ....
             */
            String result = "add user:" + name;

            this.collector.emit(new Values(requestId, result));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
    }

    public static void main(String[] args) {

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("addUser");
        builder.addBolt(new MyBolt());

        LocalDRPC drpc = new LocalDRPC();


        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("local-drpc", new Config(), builder.createLocalTopology(drpc));

        String executeResult = drpc.execute("addUser1", "张三");
        System.out.println("DRPC:" + executeResult);

        localCluster.shutdown();
        drpc.shutdown();
    }

}
