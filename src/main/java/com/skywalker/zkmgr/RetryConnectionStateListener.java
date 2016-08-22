package com.skywalker.zkmgr;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * If connection lost, retry to connect to the zookeeper server.
 *
 * @author caonn@mediav.com
 * @version 16/8/18.
 */
public class RetryConnectionStateListener implements ConnectionStateListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(RetryConnectionStateListener.class);
  private String zkNode;
  private String namePrefix;
  private NodeInfo nodeInfo;

  public RetryConnectionStateListener(String zkNode, NodeInfo nodeInfo, String namePrefix) {
    this.zkNode = zkNode;
    this.namePrefix = namePrefix;
    this.nodeInfo = nodeInfo;
  }

  @Override
  public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
    LOGGER.debug("State Changed : {}", connectionState);
    if( connectionState == ConnectionState.LOST ) {
      while(true)
      try {
        if (curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut() ) {
          if( null == ZKManager.getRegisteredZkNode(curatorFramework, zkNode, nodeInfo, namePrefix) ) {
            synchronized ( this ) {
              ZKManager.createZkNode(curatorFramework, zkNode, nodeInfo, namePrefix);
            }
          }
          break;
        }
      } catch (Exception e) {
        LOGGER.info("Exception Throwed in LOST STATE: {}", e);
      }
    } else if( connectionState == ConnectionState.SUSPENDED) {
      LOGGER.info("State Changed to SUSPENDED.");
    } else if( connectionState == ConnectionState.RECONNECTED) {
      LOGGER.info("State Changed to RECONNECTED");
    }
  }
}
