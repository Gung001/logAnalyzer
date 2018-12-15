package com.lxgy.storm.jdbc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author Gryant
 */
public class LocalWordCount {

    private static final String REDIS_HOST = "data01";
    private static final Integer REDIS_PORT = 6379;

    public static class DataSourceSpout extends BaseRichSpout {

        SpoutOutputCollector collector;
        public static final String[] words = new String[]{"apple", "orange", "pineapple", "banana", "watermelon"};

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            String word = words[new Random().nextInt(words.length)];

            this.collector.emit(new Values(word));

            System.out.println("emit:" + word);

            Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

    public static class CountBolt extends BaseRichBolt {

        OutputCollector collector;
        Map<String, Integer> countMap = new HashMap<>();

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            if (!countMap.containsKey(word)) {
                countMap.put(word, 1);
            } else {
                countMap.put(word, countMap.get(word) + 1);
            }

            this.collector.emit(new Values(word, countMap.get(word)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static class WordCountStoreMapper implements RedisStoreMapper {
        private RedisDataTypeDescription description;
        private final String hashKey = "wordCount";

        public WordCountStoreMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.HASH, hashKey);
        }

        @Override
        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        @Override
        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("word");
        }

        @Override
        public String getValueFromTuple(ITuple tuple) {
            return tuple.getIntegerByField("count") + "";
        }
    }

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("DataSourceSpout");

        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://data01/storm");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","123456");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "wordcount";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);
        builder.setBolt("JdbcInsertBolt", userPersistanceBolt).shuffleGrouping("CountBolt");

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("LocalWordCount", new Config(), builder.createTopology());
    }
}
