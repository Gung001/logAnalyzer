package com.lxgy.storm.rpc.hrpc.server;

/**
 * 客户端接口
 * @author Gryant
 */
public interface IUserService {

    public static final long versionID = 1234567890;

    /**
     * 添加用户接口
     * @param name
     */
    public void addUser(String name);

}

