package com.skywalker.zkmgr;

import com.skywalker.jpserver.Constants;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * A simple class, which can be used to store information about address and port.
 * Node info example : "123.23.34.12:7514". It can be registered to zookeeper. Then
 * other clients will know how to connect to it.
 *
 * @author caonn@mediav.com
 * @version 16/8/18.
 */
public class NodeInfo {
  public final String localAddress;
  public final int localPort;

  public NodeInfo(String localAddress, int localPort) throws SocketException {
    this.localAddress = localAddress;
    this.localPort = localPort;
  }

  public static NodeInfo parseNodeInfo( String nodeInfoStr ) throws SocketException {
    String [] vars = nodeInfoStr.split(":");
    String address = vars[0];
    int port = Integer.parseInt(vars[1]);
    return new NodeInfo(address, port);
  }

  public static NodeInfo getLocalNodeInfo (Properties properties) throws SocketException {
    String localAddress = getLocalIP();
    int localPort = Integer.parseInt(properties.getProperty(Constants.LOCAL_PORT));
    return new NodeInfo(localAddress, localPort);
  }

  @Override
  public String toString() {
    return localAddress + ":" + localPort;
  }

  public static String getLocalIP () throws SocketException {
    Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
    for (; n.hasMoreElements();)
    {
      NetworkInterface e = n.nextElement();

      Enumeration<InetAddress> a = e.getInetAddresses();
      for (; a.hasMoreElements();)
      {
        InetAddress addr = a.nextElement();
        if( !addr.isLoopbackAddress() && !addr.isSiteLocalAddress() && addr.getHostAddress().contains(":")) {
          return addr.getHostAddress();
        }
      }
    }
    return null;
  }
}
