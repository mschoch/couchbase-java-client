package com.couchbase.client;

public interface TapCheckpointManager {

    long getLastClosedCheckpoint(int vbucket);

}
