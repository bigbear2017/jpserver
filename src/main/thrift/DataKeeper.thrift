namespace java com.skywalker.jpserver

struct Point {
    1: optional i32 index; // the index of point to update
    2: optional i64 value; // the value of point to update
}

service DataKeeperService {
    bool push( 1: list<Point> dataList ); // this op support sparse vector
    list<Point> pull (); // this op return the sparse vector
}