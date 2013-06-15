
json2avro: json2avro.c avrolib/lib/libavro.so
	cc -o json2avro json2avro.c avrolib/lib/libavro.a -I avrolib/include -I avro-c/jansson/src -lz -llzma -lsnappy

avrolib/lib/libavro.so:
	cd avro-c && mkdir -p build && cd build && cmake .. -DCMAKE_INSTALL_PREFIX=../../avrolib -DCMAKE_BUILD_TYPE=Debug && make && make install

clean:
	rm -rf avrolib
	rm -rf avro-c/build



