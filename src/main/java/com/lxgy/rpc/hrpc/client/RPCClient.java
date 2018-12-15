package com.lxgy.rpc.hrpc.client;

import com.lxgy.rpc.hrpc.server.IUserService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author Gryant
 */
public class RPCClient {

    public static void main(String[] args) {

        long versionId = 1234567890;

        Configuration configuration = new Configuration();

        try {
            IUserService userService = RPC.getProxy(IUserService.class, versionId, new InetSocketAddress("localhost", 9999), configuration);
            userService.addUser("zhangsan");
            System.out.println("client involved...");
            RPC.stopProxy(userService);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
