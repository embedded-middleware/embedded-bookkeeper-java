package io.github.embedded.bookkeeper.core;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZKConnectionWatcher implements Watcher {
    private final CountDownLatch clientConnectLatch = new CountDownLatch(1);

    private final int sessionTimeout;

    public ZKConnectionWatcher(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected) {
            clientConnectLatch.countDown();
        }
    }

    public void waitForConnection() throws IOException {
        try {
            if (!clientConnectLatch.await(sessionTimeout, TimeUnit.MILLISECONDS)) {
                throw new IOException("Couldn't connect to zookeeper server");
            }
        } catch (InterruptedException e) {
            throw new IOException("Interrupted when connecting to zookeeper server", e);
        }
    }
}
