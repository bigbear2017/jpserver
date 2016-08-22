package com.skywalker.zkmgr;

import java.util.Properties;

import static com.skywalker.jpserver.Constants.*;

/** A class to contains all fields of zk.
 *
 * @author caonn@mediav.com
 * @version 16/8/18.
 */
public class ZKInfo {
  public final String connString;
  public final String zkRegPath;

  public ZKInfo(String connString, String zkRegPath) {
    this.connString = connString;
    this.zkRegPath = zkRegPath;
  }

  public ZKInfo(Properties properties) {
    this.connString = properties.getProperty(CONN_STRING);
    this.zkRegPath = properties.getProperty(ZK_REG_PATH);
  }

}
