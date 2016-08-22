package com.skywalker.jpserver;

import com.google.common.collect.Lists;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;

import java.util.List;

import static com.skywalker.jpserver.Constants.*;
/**
 * This handler will handle data internally. All push requests will be cached first.
 * When the cache has been synchronized, the cache will be added to data and cleared.
 * @author caonn@mediav.com
 * @version 16/7/13.
 */
public class DataKeeperHandler implements DataKeeperService.Iface {

  TLongIntMap dataMap = new TLongIntHashMap(INITIAL_CAPACITY, LOAD_FACTOR, NO_ENTRY_KEY, NO_ENTRY_VALUE);
  TLongIntMap cache = new TLongIntHashMap(INITIAL_CAPACITY, LOAD_FACTOR, NO_ENTRY_KEY, NO_ENTRY_VALUE);
  private long updateCounter = 0;
  private long updateTime = System.currentTimeMillis();

  public boolean push(List<Point> dataList) throws org.apache.thrift.TException {
    dataList.stream().forEach( p -> {
      int value = cache.get(p.getIndex());
      value += p.getValue();
      cache.put(p.getIndex(), value);
    });
    updateCounter ++;
    return true;
  }

  public List<Point> pull() throws org.apache.thrift.TException {
    final List<Point> points = Lists.newArrayList();
    dataMap.forEachEntry((i, l) -> points.add(new Point().setIndex(i).setValue(l)));
    return points;
  }

  /**
   * Send all data to the primary server to synchronize all data.
   */
  public void pushCache() {
    updateTime = System.currentTimeMillis();
    updateCounter = 0;
  }

  public long getUpdateCounter() {
    return updateCounter;
  }

  public long getUpdateTime() {
    return updateTime;
  }
}
