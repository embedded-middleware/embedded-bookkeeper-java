package io.github.embedded.bookkeeper.core;

import org.apache.bookkeeper.common.component.LifecycleListener;

import java.util.concurrent.CountDownLatch;

public class WaitStartLifecycleListener implements LifecycleListener {
    private final CountDownLatch countDownLatch;

    public WaitStartLifecycleListener(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void beforeStart() {
    }

    @Override
    public void afterStart() {
        countDownLatch.countDown();
    }

    @Override
    public void beforeStop() {
    }

    @Override
    public void afterStop() {
    }

    @Override
    public void beforeClose() {
    }

    @Override
    public void afterClose() {
    }
}
