package com.lxgy.storm.rpc.hrpc.server.impl;

import com.lxgy.storm.rpc.hrpc.server.IUserService;

/**
 * @author Gryant
 */
public class UserServiceImpl implements IUserService {

    @Override
    public void addUser(String name) {
        System.out.println("server involved：" + name);
    }
}
