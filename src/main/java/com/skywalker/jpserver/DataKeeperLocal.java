package com.skywalker.jpserver;

import com.skywalker.zkmgr.NodeInfo;
import com.skywalker.zkmgr.ZKInfo;
import com.skywalker.zkmgr.ZKManager;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author caonn@mediav.com
 * @version 16/8/18.
 */
public class DataKeeperLocal implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataKeeperLocal.class);

  private final DataKeeperHandler handler;
  private final TNonblockingServer server;
  private final ZKInfo zkInfo;
  private final NodeInfo nodeInfo;
  private String zkNode = null;


  public DataKeeperLocal(Properties properties) throws Exception {
    zkInfo = new ZKInfo(properties);
    nodeInfo = NodeInfo.getLocalNodeInfo(properties);
    handler = new DataKeeperHandler();
    TProcessor processor = new DataKeeperService.Processor<>(handler);
    TNonblockingServerSocket socket = new TNonblockingServerSocket(nodeInfo.localPort);
    server = new THsHaServer( new THsHaServer.Args(socket).processor(processor) );

    try {
      zkNode = ZKManager.register(zkInfo, nodeInfo);
      if ( zkNode != null ) {
        LOGGER.info("Register to zookeeper successfully!");
      }
    } catch (Exception e) {
      throw new InterruptedException("Can not register to zookeeper!");
    }
  }

  @Override
  public void run() {
    LOGGER.info("Starting server!");
    try {
      if (!server.isServing()) {
        server.serve();
      }
    } catch (Exception e) {
      LOGGER.error("Can not start server : {}", e);
    }
    LOGGER.info("Done !");
  }


  public void shutdown() {
    try {
      boolean state = ZKManager.unregister(zkInfo, zkNode);
      LOGGER.info("UnRegister Node: {}, closing state : {}, ", zkNode, state);
      ZKManager.closeClient(zkInfo);
    } catch (Exception e ) {
      LOGGER.error("UnRegister Node: {} failed, exception: {}", zkNode, e);
    }

    if( server.isServing()) {
      server.stop();
    }

    LOGGER.info("Shutdown server successfully.");
  }
}
