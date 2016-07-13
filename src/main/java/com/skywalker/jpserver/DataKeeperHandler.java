package com.skywalker.jpserver;

import com.google.common.collect.Lists;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.procedure.TIntLongProcedure;

import java.util.List;

/**
 * @author caonn@mediav.com
 * @version 16/7/13.
 */
public class DataKeeperHandler implements DataKeeperService.Iface {
  TIntLongMap dataMap = new TIntLongHashMap();

  public boolean push(List<Point> dataList) throws org.apache.thrift.TException {
    return false;
  }

  public List<Point> pull() throws org.apache.thrift.TException {
    final List<Point> points = Lists.newArrayList();
    dataMap.forEachEntry(new TIntLongProcedure() {
      public boolean execute(int i, long l) {
        points.add(new Point().setIndex(i).setValue(l));
        return true;
      }
    });
    return points;
  }
}
