package me.training.flink.side;

import java.util.function.Consumer;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;

import java.util.Collections;
import java.util.Map;


public class AsyncRedisReq extends RichAsyncFunction<String, String> {

    private transient RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;
    private RedisAsyncCommands<String, String> async;

    @Override
    public void open(Configuration parameters) throws Exception {
        String URL = "redis://redis";

        redisClient = RedisClient.create(URL);
        connection = redisClient.connect();
        async = connection.async();
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if(redisClient != null) {
            redisClient.shutdown();
        }
    }
    CountTrigger
    @Override
    public void asyncInvoke(String input, final ResultFuture<String> resultFuture) throws Exception {
        String id = "";

        async.hgetall(id).thenAccept(new Consumer<Map<String, String>>() {
            @Override
            public void accept(Map<String, String> s) {
                resultFuture.complete(Collections.singleton(s.get(id)));
            }
        });
    }
}
