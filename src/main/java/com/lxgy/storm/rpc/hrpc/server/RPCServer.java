package com.lxgy.storm.rpc.hrpc.server;

import com.lxgy.storm.rpc.hrpc.server.impl.UserServiceImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 *
 * RPC Server
 * @author Gryant
 */
public class RPCServer {

    public static void main(String[] args) {

        Configuration conf = new Configuration();

        RPC.Builder builder = new RPC.Builder(conf);

        try {

            // Java Builder 模式
            RPC.Server server = builder.setProtocol(IUserService.class)
                    .setInstance(new UserServiceImpl())
                    .setBindAddress("localhost")
                    .setPort(9999)
                    .build();

            server.start();
            System.out.println("server started...");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

