package io.github.embedded.bookkeeper.core;

import io.github.embedded.zookeeper.core.EmbeddedZkServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.assertj.core.util.Files;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class EmbeddedBookkeeperServer {

    private final File bkDir;

    private final int bkPort;

    private final int bkWebPort;

    private EmbeddedZkServer embeddedZkServer;

    private LifecycleComponentStack bookie;

    public EmbeddedBookkeeperServer() {
        this(new EmbeddedBookkeeperConfig());
    }

    public EmbeddedBookkeeperServer(EmbeddedBookkeeperConfig embeddedBookkeeperConfig) {
        try {
            this.bkDir = Files.newTemporaryFolder();
            this.bkDir.deleteOnExit();
            if (embeddedBookkeeperConfig.getBkPort() == 0) {
                this.bkPort = SocketUtil.getFreePort();
            } else {
                this.bkPort = embeddedBookkeeperConfig.getBkPort();
            }
            if (embeddedBookkeeperConfig.getBkWebPort() == 0) {
                this.bkWebPort = SocketUtil.getFreePort();
            } else {
                this.bkWebPort = embeddedBookkeeperConfig.getBkWebPort();
            }
        } catch (Throwable e) {
            log.error("exception is ", e);
            throw new IllegalStateException("start bookkeeper standalone failed");
        }
    }

    public void start() throws Exception {
        embeddedZkServer = new EmbeddedZkServer();
        embeddedZkServer.start();
        int zkPort = embeddedZkServer.getZkPort();
        initZooKeeperData();
        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.setBookiePort(bkPort);
        // metadata
        String zkServers = "127.0.0.1:" + zkPort;
        String metadataServiceUriStr = "zk://" + zkServers + "/ledgers";
        serverConf.setProperty("metadataServiceUri", metadataServiceUriStr);
        serverConf.setProperty("ensemblePlacementPolicy",
                "org.apache.bookkeeper.bookie.LocalBookieEnsemblePlacementPolicy");
        serverConf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        serverConf.setAllowEphemeralPorts(true);
        serverConf.setAllowLoopback(true);
        // storage
        serverConf.setJournalDirName(bkDir.getPath());
        serverConf.setJournalRemovePagesFromCache(false);
        serverConf.setLedgerDirNames(new String[] { bkDir.getPath() });
        // http
        serverConf.setHttpServerEnabled(true);
        serverConf.setHttpServerPort(bkWebPort);
        serverConf.setProperty("httpServerClass", "org.apache.bookkeeper.http.vertx.VertxHttpServer");
        BookieConfiguration conf = new BookieConfiguration(serverConf);
        bookie = Main.buildBookieServer(conf);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        bookie.addLifecycleListener(new WaitStartLifecycleListener(countDownLatch));
        ComponentStarter.startComponent(bookie);
        countDownLatch.await();
    }

    private void initZooKeeperData() throws Exception {
        ZKConnectionWatcher zkConnectionWatcher = new ZKConnectionWatcher(30_000);
        ZooKeeper zkc = new ZooKeeper(String.format("localhost:%d", embeddedZkServer.getZkPort()),
                30_000, zkConnectionWatcher);
        zkConnectionWatcher.waitForConnection();
        if (zkc.exists("/ledgers", false) == null) {
            zkc.create("/ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zkc.exists("/ledgers/available", false) == null) {
            zkc.create("/ledgers/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zkc.exists("/ledgers/available/readonly", false) == null) {
            zkc.create("/ledgers/available/readonly", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (zkc.exists("/bookies", false) == null) {
            zkc.create("/bookies", "{}".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
        zkc.close();
    }

    public int getBkPort() {
        return bkPort;
    }

    public int getBkWebPort() {
        return bkWebPort;
    }

    public void close() throws Exception {
        if (bookie != null) {
            bookie.stop();
        }
        if (embeddedZkServer != null) {
            embeddedZkServer.close();
        }
    }
}
