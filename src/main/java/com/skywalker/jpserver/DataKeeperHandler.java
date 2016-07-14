package com.skywalker.jpserver;

import com.google.common.collect.Lists;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntLongHashMap;

import java.util.List;

import static com.skywalker.jpserver.Constants.*;
/**
 * This handler will handle data internally. All push requests will be cached first.
 * When the cache has been synchronized, the cache will be added to data and cleared.
 * @author caonn@mediav.com
 * @version 16/7/13.
 */
public class DataKeeperHandler implements DataKeeperService.Iface {

  TIntLongMap dataMap = new TIntLongHashMap(INITIAL_CAPACITY, LOAD_FACTOR, NO_ENTRY_KEY, NO_ENTRY_VALUE);
  TIntLongMap cache = new TIntLongHashMap(INITIAL_CAPACITY, LOAD_FACTOR, NO_ENTRY_KEY, NO_ENTRY_VALUE);

  public boolean push(List<Point> dataList) throws org.apache.thrift.TException {
    dataList.stream().forEach( p -> {
      long value = cache.get(p.getIndex());
      value += p.getValue();
      cache.put(p.getIndex(), value);
    });
    return false;
  }

  public List<Point> pull() throws org.apache.thrift.TException {
    final List<Point> points = Lists.newArrayList();
    dataMap.forEachEntry(( i, l ) -> points.add(new Point().setIndex(i).setValue(l)));
    return points;
  }
}
