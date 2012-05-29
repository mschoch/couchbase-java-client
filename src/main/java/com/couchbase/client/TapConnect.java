package com.couchbase.client;

public class TapConnect {

    public enum TapMode { BACKFILL, DUMP };

    private TapMode mode;

    private String id;

    private long date;

    private TapCheckpointManager checkpointManager;

    private boolean registered;

    private int tapReconnectCount = 0;

    public TapMode getMode() {
        return mode;
    }

    public void setMode(TapMode mode) {
        this.mode = mode;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCurrentConnectId() {
        return id + Integer.toString(tapReconnectCount);
    }

    public void incrementConnectId() {
        tapReconnectCount++;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    public TapCheckpointManager getCheckpointManager() {
        return checkpointManager;
    }

    public void setCheckpointManager(TapCheckpointManager checkpointManager) {
        this.checkpointManager = checkpointManager;
    }

    public boolean isRegistered() {
        return registered;
    }

    public void setRegistered(boolean registered) {
        this.registered = registered;
    }

    private TapConnect(Builder b) {
        this.mode = b.mode;
        this.id = b.id;
        this.date = b.date;
        this.checkpointManager = b.checkpointManager;
        this.registered = b.registered;
    }

    public static class Builder {
        private TapMode mode = TapMode.BACKFILL;
        private String id = null;
        private long date = 0l;
        private TapCheckpointManager checkpointManager = null;
        private boolean registered = false;

        public Builder dump() {
            this.mode = TapMode.DUMP;
            return this;
        }

        public Builder backfill() {
            this.mode = TapMode.BACKFILL;
            return this;
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder since(long date) {
            this.date = date;
            return this;
        }

        public Builder checkpointManager(TapCheckpointManager checkpointManager) {
            this.checkpointManager = checkpointManager;
            return this;
        }

        public Builder registered(boolean registered) {
            this.registered = registered;
            return this;
        }

        public TapConnect build() {
            return new TapConnect(this);
        }
    }

}
