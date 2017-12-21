# hashvector
Redis module. Support int64 vector and float64 vector in hashmap

Added six new api for hashmap:
* HSETIV      key field int64 [int64...]
* HINCRBYIV   key field int64 [int64...]
* HGETIV      key field
* HSETFV      key field float64 [float64...]
* HINCRBYFV   key field float64 [float64...]
* HGETFV      key field

The int64 vector and float64 vector is not compatible, so you should not use `***iv` and `***fv` api at the same item.

## example
Compile this module, just run `make`. You can get the hashvector.so.
```
# redis-cli
127.0.0.1:6379> module load /path/to/hashvector.so
OK
127.0.0.1:6379> hincrbyiv grade andy 10 10 1
OK
127.0.0.1:6379> hgetiv grade andy
1) (integer) 50
2) (integer) 85
3) (integer) 81
127.0.0.1:6379>
127.0.0.1:6379> hdel grade andy
(integer) 1
127.0.0.1:6379> hincrbyiv grade andy 10 23
OK
127.0.0.1:6379> hgetiv grade andy
1) (integer) 10
2) (integer) 23
127.0.0.1:6379> hincrbyiv grade andy 11 -10 14
OK
127.0.0.1:6379> hgetiv grade andy
1) (integer) 21
2) (integer) 13
3) (integer) 14
127.0.0.1:6379> hsetiv grade andy 9 10
OK
127.0.0.1:6379> hgetiv grade andy
1) (integer) 9
2) (integer) 10

# You should not use `***iv` and `***fv` api at the same item.
127.0.0.1:6379> hgetfv grade andy
1) "4.4465908125712189e-323"
2) "4.9406564584124654e-323"
# It is not right
```
