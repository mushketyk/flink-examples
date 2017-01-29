package com.example.flink;

import org.apache.flink.api.java.tuple.Tuple2;

public class TwitterFollower extends Tuple2<Integer, Integer> {

    public TwitterFollower() {
        super(null, null);
    }

    public TwitterFollower(int user, int follower) {
        super(user, follower);
    }

    public Integer getUser() {
        return f0;
    }

    public Integer getFollower() {
        return f1;
    }

    public void setUser(int user) {
        this.f0 = user;
    }

    public void setFollower(int follower) {
        this.f1 = follower;
    }
}
