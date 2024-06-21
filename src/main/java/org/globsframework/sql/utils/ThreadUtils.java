package org.globsframework.sql.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadUtils {
    static final Logger LOGGER = LoggerFactory.getLogger(ThreadUtils.class);

    public interface IsComplete {
        boolean complete();
    }

    public static boolean waitComplete(Object thisObject, IsComplete isComplete, int timeInSecond) {
        synchronized (thisObject) {
            long waitUntil = System.currentTimeMillis() + timeInSecond * 1000;
            while (!isComplete.complete()) {
                long stillToWait = waitUntil - System.currentTimeMillis();
                if (stillToWait <= 0) {
                    LOGGER.info("timeout");
                    return false;
                }
                try {
                    thisObject.wait(stillToWait);
                } catch (InterruptedException e) {
                    LOGGER.info("Interrupted wait", e);
                    return false;
                }
            }
        }
        return true;
    }


    public static Limiter createLimiter(int maxConnection) {
        return new DefaultLimiter(maxConnection);
    }

    private static class DefaultLimiter implements Limiter {
        private final int maxConnection;
        int count = 0;

        public DefaultLimiter(int maxConnection) {
            this.maxConnection = maxConnection;
        }

        public void notifyDown() {
            synchronized (this) {
                count -= 1;
                if (count < maxConnection / 2) {
                    this.notifyAll();
                }
            }
        }

        public void limitParallelConnection() {
            synchronized (this) {
                count += 1;
                while (count > maxConnection) {
                    try {
                        long l = System.currentTimeMillis();
                        wait(60000);
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Wait " + (System.currentTimeMillis() - l) + " ms");
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException("While waiting for db", e);
                    }
                }
            }
        }

        public void waitAllDone() {
            synchronized (this) {
                while (count != 0) {
                    try {
                        long l = System.currentTimeMillis();
                        wait(60000);
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Wait " + (System.currentTimeMillis() - l) + " ms");
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException("While waiting for db", e);
                    }
                }
            }
        }
    }

    public interface Limiter {
        Limiter NULL = new Limiter() {
            public void notifyDown() {
            }

            public void limitParallelConnection() {
            }

            public void waitAllDone() {
            }
        };
        void notifyDown();

        void limitParallelConnection();

        void waitAllDone();
    }
}
