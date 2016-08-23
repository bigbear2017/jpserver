package com.skywalker.zkmgr;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * ZooKeeper Manager to register and unRegister nodes.
 *
 * @author caonn@mediav.com
 * @version 16/8/18.
 */
public class ZKManager {
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static Map<String, CuratorFramework> clientMap = Maps.newHashMap();
  private static Map<String, ConnectionStateListener> listenerMap = Maps.newHashMap();

  public static CuratorFramework initClient (ZKInfo zkInfo) throws InterruptedException {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework client = CuratorFrameworkFactory.newClient( zkInfo.connString, retryPolicy);
    clientMap.put(zkInfo.connString, client);
    client.start();
    client.getZookeeperClient().blockUntilConnectedOrTimedOut();
    return client;
  }

  public static String register( ZKInfo zkInfo, NodeInfo nodeInfo ) throws Exception {
    CuratorFramework client = findClient(zkInfo);
    if( client == null ) {
      client = initClient(zkInfo);
    }
    String prefix = "node-";
    String zkNode =  createZkNode(client, zkInfo.zkRegPath, nodeInfo, prefix);
    ConnectionStateListener listener = new RetryConnectionStateListener(zkInfo.zkRegPath, nodeInfo, prefix);
    client.getConnectionStateListenable().addListener(listener);
    listenerMap.put(zkNode, listener);
    return zkNode;
  }

  public static CuratorFramework findClient( ZKInfo zkInfo ) {
    if( clientMap.containsKey(zkInfo.connString) ) {
      return clientMap.get(zkInfo.connString);
    }
    return null;
  }

  public static String createZkNode(CuratorFramework client, String zkRegPath, NodeInfo nodeInfo, String prefix)
    throws Exception {
    String nodePath = zkRegPath + "/" + prefix;
    return client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(nodePath,
      nodeInfo.toString().getBytes(UTF8));
  }

  public static String getRegisteredZkNode(CuratorFramework client, String zkRegPath,
                                           NodeInfo nodeInfo, String prefix) throws Exception {
    if( null == client ) {
      return null;
    }
    if( !client.getZookeeperClient().isConnected() ) {
      client.start();
      client.getZookeeperClient().blockUntilConnectedOrTimedOut();
    }

    List<String> nodes = client.getChildren().forPath(zkRegPath);
    for(String node : nodes) {
      String nodeData = new String( client.getData().forPath(zkRegPath + "/" + node) );
      NodeInfo nInfo = NodeInfo.parseNodeInfo(nodeData);
      if( node.startsWith(prefix) && nInfo.localAddress.equals(nodeInfo.localAddress)
          && nInfo.localPort == nodeInfo.localPort ) {
        return zkRegPath + "/" + node;
      }
    }
    return null;
  }

  public static synchronized boolean unregister(ZKInfo zkInfo, String zNode) throws Exception {
    CuratorFramework client = findClient(zkInfo);
    if( null == client) {
      return false;
    }
    if( ! client.getZookeeperClient().isConnected() ) {
      client.start();
      client.getZookeeperClient().blockUntilConnectedOrTimedOut();
    }

    if( null != client.checkExists().forPath(zNode) ) {
      ConnectionStateListener listener = listenerMap.get(zNode);
      if( null != listener ) {
        client.getConnectionStateListenable().removeListener(listener);
        listenerMap.remove(zNode);
      }
      client.delete().forPath(zNode);
      return true;
    }
    return false;
  }

  public static synchronized void closeClient( ZKInfo zkInfo ) throws Exception {
    CuratorFramework client = clientMap.get(zkInfo.connString);
    if( null != client ) {
      client.close();
    }
  }

  public static List<String> getNodes(ZKInfo zkInfo) throws Exception {
    CuratorFramework client = clientMap.get(zkInfo.connString);
    if( null == client ) {
      return null;
    }
    if( !client.getZookeeperClient().isConnected() ) {
      client.start();
      client.blockUntilConnected();
    }
    List<String> nodes = client.getChildren().forPath(zkInfo.zkRegPath);
    List<String> res = Lists.newArrayList();
    for( String node : nodes ) {
      String nodeData = new String( client.getData().forPath(zkInfo.zkRegPath + "/" + node) );
      res.add(nodeData);
    }
    return res;
  }

  public static NodeInfo getPrimaryServer(ZKInfo zkInfo) throws Exception {
    List<String> nodes = getNodes(zkInfo);
    if( null == nodes ) {
      return null;
    }
    Collections.sort(nodes);
    return NodeInfo.parseNodeInfo(nodes.get(0));
  }
}
