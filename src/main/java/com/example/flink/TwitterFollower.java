package com.example.flink;

public class TwitterFollower {
    private int user;
    private int follower;


    public TwitterFollower(int user, int follower) {
        this.user = user;
        this.follower = follower;
    }

    public int getUser() {
        return user;
    }

    public int getFollower() {
        return follower;
    }

    public void setUser(int user) {
        this.user = user;
    }

    public void setFollower(int follower) {
        this.follower = follower;
    }
}
