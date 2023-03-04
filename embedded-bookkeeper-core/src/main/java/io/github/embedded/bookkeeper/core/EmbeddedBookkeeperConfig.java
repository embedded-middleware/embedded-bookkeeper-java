package io.github.embedded.bookkeeper.core;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class EmbeddedBookkeeperConfig {
    private int bkPort;

    private int bkWebPort;

    public EmbeddedBookkeeperConfig() {
    }

    public EmbeddedBookkeeperConfig bkPort(int bkPort) {
        this.bkPort = bkPort;
        return this;
    }

    public EmbeddedBookkeeperConfig bkWebPort(int bkWebPort) {
        this.bkWebPort = bkWebPort;
        return this;
    }
}
