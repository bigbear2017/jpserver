package com.skywalker.jpserver;

import com.skywalker.zkmgr.NodeInfo;
import com.skywalker.zkmgr.ZKInfo;
import com.skywalker.zkmgr.ZKManager;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 *
 * @author caonn@mediav.com
 * @version 16/8/18.
 */
public class DataKeeperLocal implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataKeeperLocal.class);

  private final DataKeeperHandler dataHandler;
  private final TNonblockingServer server;

  private final DataConsumeHandler consumeHandler;
  private final Thread consumeThread;

  private final DataSyncHandler syncHandler;
  private final Thread syncThread;

  private final ZKInfo zkInfo;
  private final NodeInfo nodeInfo;
  private String zkNode = null;
  private boolean localMode = true;

  public static void main(String [] args) throws Exception {
    ServerOptions options = new ServerOptions();
    CmdLineParser parser = new CmdLineParser(new ServerOptions());
    parser.parseArgument(args);

    Properties properties = new Properties();
    properties.load(new FileInputStream(new File(options.getPropFile())));

    DataKeeperLocal dataKeeperLocal = new DataKeeperLocal(properties);
    dataKeeperLocal.run();

  }

  public DataKeeperLocal(Properties properties) throws Exception {
    zkInfo = new ZKInfo(properties);
    nodeInfo = NodeInfo.getLocalNodeInfo(properties);
    dataHandler = new DataKeeperHandler(1000, 1000, 100, 2000);
    localMode = Boolean.parseBoolean(properties.getProperty(Constants.LOCAL_MODE));
    TProcessor processor = new DataKeeperService.Processor<>(dataHandler);
    TNonblockingServerSocket socket = new TNonblockingServerSocket(nodeInfo.localPort);
    server = new THsHaServer( new THsHaServer.Args(socket).processor(processor) );

    consumeHandler = new DataConsumeHandler(dataHandler);
    consumeThread = new Thread(consumeHandler);

    syncHandler = new DataSyncHandler(dataHandler, localMode, zkInfo);
    syncThread = new Thread(syncHandler);

    if( !localMode ) {
      try {
        zkNode = ZKManager.register(zkInfo, nodeInfo);
        if (zkNode != null) {
          LOGGER.info("Register to zookeeper successfully!");
        }
      } catch (Exception e) {
        throw new InterruptedException("Can not register to zookeeper!");
      }
    }
  }

  @Override
  public void run() {
    LOGGER.info("Starting server!");
    try {
      if (!server.isServing()) {
        server.serve();
      }
      consumeThread.start();
      consumeThread.setDaemon(true);
      syncThread.start();
      syncThread.setDaemon(true);
    } catch (Exception e) {
      LOGGER.error("Can not start server : {}", e);
    }
    LOGGER.info("Done !");
  }


  public void shutdown() {
    if(!localMode) {
      try {
        boolean state = ZKManager.unregister(zkInfo, zkNode);
        LOGGER.info("UnRegister Node: {}, closing state : {}, ", zkNode, state);
        ZKManager.closeClient(zkInfo);
      } catch (Exception e) {
        LOGGER.error("UnRegister Node: {} failed, exception: {}", zkNode, e);
      }
    }

    if( server.isServing()) {
      server.stop();
    }

    consumeHandler.shutdown();
    syncHandler.shutdown();

    LOGGER.info("Shutdown server successfully.");
  }
}
