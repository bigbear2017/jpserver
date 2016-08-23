package com.skywalker.jpserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Put data to cache. When the server starts, it will put data to blocking list.
 * Then we will poll data from list and put data to cache. By using a list, we
 * can update one by one.
 *
 * @author caonn@mediav.com
 * @version 16/8/23.
 */
public class DataConsumeHandler implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataConsumeHandler.class);
  private DataKeeperHandler keeperHandler;
  private boolean shutdown = false;

  public DataConsumeHandler(DataKeeperHandler keeperHandler) {
    this.keeperHandler = keeperHandler;
  }

  @Override
  public void run() {
    LOGGER.info("");
    while(!shutdown) {
      keeperHandler.putToCache();
    }
    LOGGER.info("Finished");
  }

  public void shutdown() {
    LOGGER.info("Shutdown consumer to cache.");
    shutdown = true;
  }
}
