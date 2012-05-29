/**
 * Copyright (C) 2009-2011 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.client;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;

import net.spy.memcached.BroadcastOpFactory;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.TapOperation;
import net.spy.memcached.tapmessage.RequestMessage;
import net.spy.memcached.tapmessage.ResponseMessage;
import net.spy.memcached.tapmessage.TapAck;
import net.spy.memcached.tapmessage.TapOpcode;
import net.spy.memcached.tapmessage.TapStream;

import com.couchbase.client.TapConnect.TapMode;
import com.couchbase.client.vbucket.Reconfigurable;
import com.couchbase.client.vbucket.config.Bucket;

/**
 * A tap client for Couchbase server.
 */
public class TapClient extends net.spy.memcached.TapClient implements
        Reconfigurable {
    private List<URI> baseList;
    private String bucketName;
    private String pwd;

    protected final HashMap<TapStream, TapConnect> cmap;
    protected boolean reconfiguring = false;

    /**
     * Creates a cluster aware tap client for Couchbase Server.
     *
     * This type of TapClient will TAP all servers in the specified cluster and
     * will react to changes in the number of cluster nodes.
     *
     * @param baseList
     *            a list of servers to get the cluster configuration from.
     * @param bucketName
     *            the name of the bucket to tap.
     * @param usr
     *            the buckets username.
     * @param pwd
     *            the buckets password.
     */
    public TapClient(final List<URI> baseList, final String bucketName,
            final String pwd) {
        for (URI bu : baseList) {
            if (!bu.isAbsolute()) {
                throw new IllegalArgumentException(
                        "The base URI must be absolute");
            }
        }
        this.baseList = baseList;
        this.bucketName = bucketName;
        this.pwd = pwd;
        this.cmap = new HashMap<TapStream, TapConnect>();
    }

    /**
     * Gets the next tap message from the queue of received tap messages.
     *
     * @return The tap message at the head of the queue or null if the queue is
     *         empty for more than one second.
     */
    public ResponseMessage getNextMessage() {
        return getNextMessage(1, TimeUnit.SECONDS);
    }

    /**
     * Gets the next tap message from the queue of received tap messages.
     *
     * @param time
     *            the amount of time to wait for a message.
     * @param timeunit
     *            the unit of time to use.
     * @return The tap message at the head of the queue or null if the queue is
     *         empty for the given amount of time.
     */
    public ResponseMessage getNextMessage(long time, TimeUnit timeunit) {
        try {
            Object m = rqueue.poll(time, timeunit);
            if (m == null) {
                return null;
            } else if (m instanceof ResponseMessage) {
                return (ResponseMessage) m;
            } else if (m instanceof TapAck) {
                TapAck ack = (TapAck) m;
                tapAck((com.couchbase.client.TapConnectionProvider) ack
                        .getConn(),
                        ack.getNode(), ack.getOpcode(), ack.getOpaque(), ack
                                .getCallback());
                return null;
            } else {
                throw new RuntimeException("Unexpected tap message type");
            }
        } catch (InterruptedException e) {
            shutdown();
            return null;
        }
    }

    /**
     * Decides whether the client has received tap messages or will receive more
     * messages in the future.
     *
     * @return true if the client has tap responses or expects to have responses
     *         in the future. False otherwise.
     */
    public boolean hasMoreMessages() {
        if (!rqueue.isEmpty()) {
            return true;
        } else if (reconfiguring) {
            // don't stop while we're in the process of restarting

            getLogger().info("TAP reconfiguration started");

            // now walk the TapConnect objects and retap
            synchronized (cmap) {
                for (Map.Entry<TapStream, TapConnect> me : cmap.entrySet()) {
                    try {
                        TapStream stream = me.getKey();
                        TapConnect connect = me.getValue();
                        net.spy.memcached.TapConnectionProvider existingConnection = omap
                                .get(me.getKey());

                        // get current id
                        String existingTapId = connect.getCurrentConnectId();

                        // now increment it
                        getLogger().debug("incrementing the connect id");
                        connect.incrementConnectId();

                        // shutdown existing connection
                        getLogger().debug("Shutting down existing connection");
                        existingConnection.shutdown();

                        // re tap
                        getLogger().debug("Re-TAPing the connection");
                        tapExisting(stream, connect);

                        if (connect.isRegistered()) {
                            // now try to deregister the old name
                            net.spy.memcached.TapConnectionProvider newConnection = omap
                                    .get(me.getKey());
                            getLogger().debug(
                                    "De-registering existing name: "
                                            + existingTapId);
                            tapDeregister(existingTapId, newConnection);
                        }

                    } catch (ConfigurationException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            this.reconfiguring = false;

            getLogger().info("TAP reconfiguration finished");

            return true;
        } else {
            synchronized (omap) {
                Iterator<TapStream> itr = omap.keySet().iterator();
                while (itr.hasNext()) {
                    TapStream op = itr.next();
                    if (op.isCompleted() || op.isCancelled() || op.hasErrored()) {
                        omap.get(op).shutdown();
                        omap.remove(op);
                    }
                }
                if (omap.size() > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Allows the user to specify a custom tap message.
     *
     * This API for TAP is still evolving, and only recommended for advanced
     * usage.
     *
     * See http://www.couchbase.com/wiki/display/couchbase/TAP+Protocol
     *
     * @param id
     *            the named tap id that can be used to resume a disconnected tap
     *            stream
     * @param message
     *            the custom tap message that will be used to initiate the tap
     *            stream.
     * @return the operation that controls the tap stream.
     * @throws ConfigurationException
     *             a bad configuration was received from the Couchbase cluster.
     * @throws IOException
     *             if there are errors connecting to the cluster.
     */
    public TapStream tapCustom(final String id, final RequestMessage message)
            throws ConfigurationException, IOException {
        final TapConnectionProvider conn = new TapConnectionProvider(this,
                baseList, bucketName, pwd);
        final TapStream ts = new TapStream();
        conn.broadcastOp(new BroadcastOpFactory() {
            public Operation newOp(final MemcachedNode n,
                    final CountDownLatch latch) {
                Operation op = conn.getOpFactory().tapCustom(id, message,
                        new TapOperation.Callback() {
                            public void receivedStatus(OperationStatus status) {
                            }

                            public void gotData(ResponseMessage tapMessage) {
                                rqueue.add(tapMessage);
                                messagesRead++;
                            }

                            public void gotAck(MemcachedNode node,
                                    TapOpcode opcode, int opaque) {
                                rqueue.add(new TapAck(conn, node, opcode,
                                        opaque, this));
                            }

                            public void complete() {
                                latch.countDown();
                            }
                        });
                ts.addOp((TapOperation) op);
                return op;
            }
        });
        synchronized (omap) {
            omap.put(ts, conn);
        }
        return ts;
    }

    /**
     * Specifies a tap stream that will send all key-value mutations that take
     * place in the future.
     *
     * @param id
     *            the named tap id that can be used to resume a disconnected tap
     *            stream
     * @param runTime
     *            the amount of time to do backfill for. Set to 0 for infinite
     *            backfill.
     * @param timeunit
     *            the unit of time for the runtime parameter.
     * @return the operation that controls the tap stream.
     * @throws ConfigurationException
     *             a bad configuration was received from the Couchbase cluster.
     * @throws IOException
     *             If there are errors connecting to the cluster.
     */
    public TapStream tapBackfill(String id, final int runTime,
            final TimeUnit timeunit) throws IOException, ConfigurationException {
        return tapBackfill(id, -1, runTime, timeunit);
    }

    /**
     * Specifies a tap stream that will send all key-value mutations that took
     * place after a specific date.
     *
     * @param id
     *            the named tap id that can be used to resume a disconnected tap
     *            stream
     * @param date
     *            the date to begin sending key mutations from. Specify -1 to
     *            send all future key-value mutations.
     * @param runTime
     *            the amount of time to do backfill for. Set to 0 for infinite
     *            backfill.
     * @param timeunit
     *            the unit of time for the runtime parameter.
     * @return the operation that controls the tap stream.
     * @throws ConfigurationException
     *             a bad configuration was received from the Couchbase cluster.
     * @throws IOException
     *             If there are errors connecting to the cluster.
     */
    public TapStream tapBackfill(final String id, final long date,
            final int runTime, final TimeUnit timeunit) throws IOException,
            ConfigurationException {
        final TapConnectionProvider conn = new TapConnectionProvider(this,
                baseList, bucketName, pwd);
        final TapStream ts = new TapStream();
        conn.broadcastOp(new BroadcastOpFactory() {
            public Operation newOp(final MemcachedNode n,
                    final CountDownLatch latch) {
                Operation op = conn.getOpFactory().tapBackfill(id, date,
                        new TapOperation.Callback() {
                            public void receivedStatus(OperationStatus status) {
                            }

                            public void gotData(ResponseMessage tapMessage) {
                                rqueue.add(tapMessage);
                                messagesRead++;
                            }

                            public void gotAck(MemcachedNode node,
                                    TapOpcode opcode, int opaque) {
                                rqueue.add(new TapAck(conn, node, opcode,
                                        opaque, this));
                            }

                            public void complete() {
                                latch.countDown();
                            }
                        });
                ts.addOp((TapOperation) op);
                return op;
            }
        });
        synchronized (omap) {
            omap.put(ts, conn);
        }

        if (runTime > 0) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(TimeUnit.MILLISECONDS.convert(runTime,
                                timeunit));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    conn.shutdown();
                    synchronized (omap) {
                        omap.remove(ts);
                    }
                }
            };
            new Thread(r).start();
        }
        return ts;
    }

    public TapStream tapBackfill(final String id,
            final TapCheckpointManager checkpointManager, final int runTime,
            final TimeUnit timeunit) throws IOException, ConfigurationException {
        final TapConnectionProvider conn = new TapConnectionProvider(this,
                baseList, bucketName, pwd);

        final TapStream ts = new TapStream();
        conn.broadcastOp(new BroadcastOpFactory() {
            public Operation newOp(final MemcachedNode n,
                    final CountDownLatch latch) {

                // find out which vbucket masters are on this node
                List<Integer> vbuckets = conn.getVBucketsForNode(n);
                getLogger().debug("Master vbuckets on this node are: %s",
                        vbuckets);

                // build a map of closed checkpoints for these vbuckets
                Map<Short, Long> closedCheckpointMap = new HashMap<Short, Long>();
                for (Integer vbucket : vbuckets) {
                    Long checkpoint = checkpointManager
                            .getLastClosedCheckpoint(vbucket);
                    // NOTE: converting to short here, really should have been
                    // short everywhere
                    closedCheckpointMap.put(vbucket.shortValue(),
                            checkpoint != null ? checkpoint : 0);
                }
                getLogger().debug("Closed checkpoint map for this node is: %s",
                        closedCheckpointMap);

                // now build a TAP connect for this node with this checkpoint
                // map

                Operation op = conn.getOpFactory().tapBackfill(id,
                        closedCheckpointMap, new TapOperation.Callback() {
                            public void receivedStatus(OperationStatus status) {
                            }

                            public void gotData(ResponseMessage tapMessage) {
                                rqueue.add(tapMessage);
                                messagesRead++;
                            }

                            public void gotAck(MemcachedNode node,
                                    TapOpcode opcode, int opaque) {
                                rqueue.add(new TapAck(conn, node, opcode,
                                        opaque, this));
                            }

                            public void complete() {
                                latch.countDown();
                            }
                        });
                ts.addOp((TapOperation) op);
                return op;
            }
        });
        synchronized (omap) {
            omap.put(ts, conn);
        }

        if (runTime > 0) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(TimeUnit.MILLISECONDS.convert(runTime,
                                timeunit));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    conn.shutdown();
                    synchronized (omap) {
                        omap.remove(ts);
                    }
                }
            };
            new Thread(r).start();
        }
        return ts;
    }

    /**
     * Specifies a tap stream that will take a snapshot of items in Couchbase
     * and send them through a tap stream.
     *
     * @param id
     *            the named tap id that can be used to resume a disconnected tap
     *            stream
     * @return the operation that controls the tap stream.
     * @throws ConfigurationException
     *             a bad configuration was received from the Couchbase cluster.
     * @throws IOException
     *             If there are errors connecting to the cluster.
     */
    public TapStream tapDump(final String id) throws IOException,
            ConfigurationException {
        final TapConnectionProvider conn = new TapConnectionProvider(this,
                baseList, bucketName, pwd);
        final TapStream ts = new TapStream();
        conn.broadcastOp(new BroadcastOpFactory() {
            public Operation newOp(final MemcachedNode n,
                    final CountDownLatch latch) {
                Operation op = conn.getOpFactory().tapDump(id,
                        new TapOperation.Callback() {
                            public void receivedStatus(OperationStatus status) {
                            }

                            public void gotData(ResponseMessage tapMessage) {
                                rqueue.add(tapMessage);
                                messagesRead++;
                            }

                            public void gotAck(MemcachedNode node,
                                    TapOpcode opcode, int opaque) {
                                rqueue.add(new TapAck(conn, node, opcode,
                                        opaque, this));
                            }

                            public void complete() {
                                latch.countDown();
                            }
                        });
                ts.addOp((TapOperation) op);
                return op;
            }
        });
        synchronized (omap) {
            omap.put(ts, conn);
        }
        return ts;
    }

    private void tapDeregister(final String id, final net.spy.memcached.TapConnectionProvider conn) {
        conn.broadcastOp(new BroadcastOpFactory() {

            public Operation newOp(final MemcachedNode n,
                    final CountDownLatch latch) {

                Operation op = conn.getOpFactory().tapDeregister(id,
                        new OperationCallback() {

                            @Override
                            public void receivedStatus(OperationStatus status) {
                                getLogger().debug("Deregister TAP client %s returned %d from node %s", id, status, n);
                            }

                            @Override
                            public void complete() {
                                latch.countDown();
                            }

                        });
                return op;
            }
        });
    }

    private void tapAck(TapConnectionProvider conn, MemcachedNode node,
            TapOpcode opcode, int opaque, OperationCallback cb) {
        final Operation op = conn.getOpFactory().tapAck(opcode, opaque, cb);
        conn.addTapAckOp(node, op);
    }

    public TapStream tap(final TapConnect connect) throws IOException, ConfigurationException {

        // build a new TapStream
        final TapStream ts = new TapStream();

        return tapExisting(ts, connect);
    }

    public TapStream tapExisting(final TapStream ts, final TapConnect connect) throws IOException, ConfigurationException {
        // build a new TapConnection
        final TapConnectionProvider conn = new TapConnectionProvider(this, baseList,
                bucketName, pwd);

        // build BroadcastOpFactory
        BroadcastOpFactory opFactory = new BroadcastOpFactory() {

            public Operation newOp(final MemcachedNode n,
                    final CountDownLatch latch) {

                TapOperation.Callback callback = new TapOperation.Callback() {
                    public void receivedStatus(OperationStatus status) {
                    }

                    public void gotData(ResponseMessage tapMessage) {
                        rqueue.add(tapMessage);
                        messagesRead++;
                    }

                    public void gotAck(MemcachedNode node,
                            TapOpcode opcode, int opaque) {
                        rqueue.add(new TapAck(conn, node, opcode,
                                opaque, this));
                    }

                    public void complete() {
                        latch.countDown();
                    }
                };

                Operation op;
                if(connect.getMode() == TapMode.DUMP) {
                    op = conn.getOpFactory().tapDump(connect.getId(), callback);
                } else {
                    if(connect.getCheckpointManager() == null) {
                        op = conn.getOpFactory().tapBackfill(connect.getId(), connect.getDate(), callback);
                    } else {
                        getLogger().debug("Building TAP connect message for %s", n);
                        // find out which vbucket masters are on this node
                        List<Integer> vbuckets = conn.getVBucketsForNode(n);
                        getLogger().debug("Master vbuckets on this node are: %s", vbuckets);

                        // build a map of closed checkpoints for these vbuckets
                        Map<Short, Long> closedCheckpointMap = new HashMap<Short, Long>();
                        for (Integer vbucket : vbuckets) {
                            Long checkpoint = connect.getCheckpointManager().getLastClosedCheckpoint(vbucket);
                            // NOTE: converting to short here, really should have been short everywhere
                            closedCheckpointMap.put(vbucket.shortValue(), checkpoint != null ? checkpoint: 0);
                        }
                        getLogger().debug("Closed checkpoint map for this node is: %s", closedCheckpointMap);

                        op = conn.getOpFactory().tapBackfill(connect.getCurrentConnectId(), closedCheckpointMap, callback);
                    }
                }

                ts.addOp((TapOperation) op);
                return op;
            }
        };
        conn.broadcastOp(opFactory);
        synchronized (omap) {
            omap.put(ts, conn);
        }
        synchronized (cmap) {
            cmap.put(ts, connect);
        }
        return ts;
    }

    /**
     * Shuts down all tap streams that are currently running.
     */
    public void shutdown() {
        synchronized (omap) {
            for (Map.Entry<TapStream, net.spy.memcached.TapConnectionProvider> me : omap
                    .entrySet()) {
                me.getValue().shutdown();
            }
        }
    }

    /**
     * The number of messages read by all of the tap streams created with this
     * client. This will include a count of all tap response types.
     *
     * @return The number of messages read
     */
    public long getMessagesRead() {
        return messagesRead;
    }

    @Override
    public void reconfigure(Bucket bucket) {
        this.reconfiguring = true;

    }
}
