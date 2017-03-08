package com.skywalker.jpserver;

import com.skywalker.zkmgr.ZKInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author caonn@mediav.com
 * @version 16/8/23.
 */
public class DataSyncHandler implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataSyncHandler.class);
  private DataKeeperHandler keeperHandler;
  private boolean shutdown = false;
  private boolean localMode = true;
  private ZKInfo zkInfo = null;

  public DataSyncHandler( DataKeeperHandler keeperHandler, boolean localMode, ZKInfo zkInfo ) {
    this.keeperHandler = keeperHandler;
    this.localMode =  localMode;
    this.zkInfo = zkInfo;
  }

  @Override
  public void run() {
    LOGGER.info("DataSync starts...");
    while( !shutdown ) {
      keeperHandler.updateCache(localMode, zkInfo);
    }
    LOGGER.info("DataSync stops...");
  }

  public void shutdown() {
    shutdown = true;
  }
}
