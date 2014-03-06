json2avro: json2avro.c avrolib/lib/libavro.so
	cc -o json2avro json2avro.c avrolib/lib/libavro.a -I avrolib/include -I avro-c/jansson/src -lz -llzma -lsnappy

avrolib/lib/libavro.so:
	mkdir -p avro-c/build
	cd avro-c/build && cmake .. -DCMAKE_INSTALL_PREFIX=../../avrolib -DCMAKE_BUILD_TYPE=Debug
	cd avro-c/build && make
	test -f avro-c/build/avro-c.pc && mv avro-c/build/avro-c.pc avro-c/build/src/ || true
	cd avro-c/build && make install

clean:
	rm -rf avrolib
	rm -rf avro-c/build



