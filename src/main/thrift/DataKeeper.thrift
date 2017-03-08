namespace java com.skywalker.jpserver

struct Point {
    1: optional i64 index; // the index of point to update
    2: optional i32 value; // the value of point to update
}

service DataKeeperService {
    bool push( 1: list<Point> dataList ); // this op support sparse vector
    list<Point> pull (); // this op return the sparse vector
}