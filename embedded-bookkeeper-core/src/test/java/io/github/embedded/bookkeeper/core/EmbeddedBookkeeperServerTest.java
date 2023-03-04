package io.github.embedded.bookkeeper.core;

import org.junit.jupiter.api.Test;

public class EmbeddedBookkeeperServerTest {

    @Test
    public void testBookkeeperServerBoot() throws Exception {
        EmbeddedBookkeeperServer server = new EmbeddedBookkeeperServer();
        server.start();
        server.close();
    }
}
