package com.lxgy.rpc.hrpc.server.impl;

import com.lxgy.rpc.hrpc.server.IUserService;

/**
 * @author Gryant
 */
public class UserServiceImpl implements IUserService {

    @Override
    public void addUser(String name) {
        System.out.println("server involved：" + name);
    }
}
