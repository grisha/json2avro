
A utility for converting JSON files to Avro. It is written entirely in
C and is quite fast. Supports Snappy, Deflate (zlib) and LZMA
compression codecs, as well as custom Avro block size.

The purpose is to be useful in converting messy legacy JSON in which
some elements might be missing or of wrong type, which is not
currently possible with the standard avro-tools fromjson option.

Since in a conversion from JSON *schema resolution* is technically not
applicable (because JSON is not Avro), json2avro mimics schema
resolution behavior by attemptin to use the defaults specified in the
schema if the corresponding JSON element is missing as well as
attempting to resolve unions by trying each type until one succeeds.

It uses the Jansson JSON parser and Avro-C for Avro encoding. Both
tools are written in C and are extremely fast.

The Jansson parser is used with the JSON_DISABLE_EOF_CHECK, which
means that the input does not have to be an object per-line, but is
free-format. So long as the input represents a sequence of JSON
objects (an object is enclosed in [] or {}), json2avro should be able
to parse it. Note that Jansson does not allow null characters (\u0000)
as part of JSON strings, not even in embedded form. To work around
this, json2avro will replace all nulls in strings (escaped or
not) with a character '0'. (Yes, this is a total hack).

If json2avro encounters an error, it skips to the nearest end-of-line
and starts parsing afresh. (This behavior can be turned off with the
-x option).

## Usage

```sh
$ ./json2avro -h
Usage: ./json2avro [options] [input_file.json] <output_file.avro>

Where options are:
 -s schema (required) Avro schema to use for conversion.
 -S file   (required) JSON file to read the avro schema from.
 -c algo   (optional) Set output compression algorithm: null, snappy, deflate, lzma
                      Default: no compression
 -b bytes  (optional) Set output block size in bytes. Default: 16384
 -d        (optional) Turn on debug mode.
 -j        (optional) Dump unexpected JSON objects as strings.
 -x        (optional) Abort on JSON parsing errors. Default: skip invalid json.
 -z bytes  (optional) Maximum JSON string size. Default: no limit.
 -m        (optional) Linux only, enable periodic memory stats information output.
 -h                   Show this help and exit.

If infile.json is not specified, STDIN is assumed. outfile.avro of '-' means STDOUT.
```

## Example

If we have the following JSON called `input.json`:

```javascript

{"a_null":null, "a_bool":true, "an_int":12345, "a_long":9876543210,
"a_float":1.234567, "a_double":12345678.1234567, "a_string":"foo bar",
"random_bytes":"\nV@H#3\u001ad\u001a\u0006G\u0006K\u0007",
"a_fixed":"abcd", "an_int_array":[123, 456, -32, 0, 12],
"a_float_map":{"foo":2.345, "bar":-3.456}}    {"a_null":null,
"a_bool":false, "an_int":54321, "a_long":9876543212,
"a_float":7.654321, "a_double":8.76543217654321E7,
"a_string":"foo bar",
"random_bytes":"\u0006K\u0007\nV@H#3\u001ad\u001a\u0006",
"a_fixed":"dcba", "an_int_array":[321, 654, -23, 0, 21],
"a_float_map":{"foo":5.324, "bar":-6.543}, "null_default":"blah"}

```

It can be converted to Avro with the following command:

```
./json2avro input.json output.avro -s \
'{"type":"record","name":"testrec","fields":[
 {"name":"a_null","type":"null"},
 {"name":"a_bool","type":"boolean"},
 {"name":"an_int","type":"int"},
 {"name":"a_long","type":"long"},
 {"name":"a_float","type":"float"},
 {"name":"a_double","type":"double"},
 {"name":"a_string","type":"string"},
 {"name":"random_bytes","type":"bytes"},
 {"name":"a_fixed","type":{"type":"fixed","size":3,"name":"four"}},
 {"name":"an_int_array","type":{"type":"array","items":"int"}},
 {"name":"a_float_map","type":{"type":"map","values":"float"}},
 {"name":"null_default","type":["null","string"],"default":"null"}]}'
```

You can verify that it worked correctly by using avro-tools (distributed with Avro Java), for example:

```
java -jar ~/src/avro/java/avro-tools-1.7.4.jar tojson output.avro
{"a_null":null,"a_bool":true,"an_int":12345,"a_long":9876543210,"a_float":1.234567,"a_double":1.23456781234567E7,"a_string":"foo bar","random_bytes":"\nV@H#3\u001Ad\u001A\u0006G\u0006K\u0007","a_fixed":"\u0000\u0000\u0000","an_int_array":[123,456,-32,0,12],"a_float_map":{"bar":-3.456,"foo":2.345},"null_default":{"string":"null"}}
{"a_null":null,"a_bool":false,"an_int":54321,"a_long":9876543212,"a_float":7.654321,"a_double":8.76543217654321E7,"a_string":"foo bar","random_bytes":"\u0006K\u0007\nV@H#3\u001Ad\u001A\u0006","a_fixed":"\u0000\u0000\u0000","an_int_array":[321,654,-23,0,21],"a_float_map":{"bar":-6.543,"foo":5.324},"null_default":{"string":"blah"}}
```

The -j options tells json2avro to dump remaining JSON as a string
where the Avro schema expects a string but JSON contains other
types. This is useful when you have objects of arbitrary schema and
you would like to store them as strings. For example, given the
following JSON:

```
{"foo":"some value", "bar":{"some":["more", 3, {"complex":"json"}], "which":"we don't care to parse"}}
```

You can convert it to Avro as such:

```sh
./json2avro input2.json output2.avro -j -s \
'{"type":"record", "name":"strjson", "fields":[
 {"name":"foo", "type":"string"},
 {"name":"bar", "type":"string"}]}'
```

This will save the value of "bar" as a JSON-encoded string:

```
java -jar ~/src/avro/java/avro-tools-1.7.4.jar tojson output2.avro
{"foo":"some value","bar":"{\"some\":[\"more\",3,{\"complex\":\"json\"}],\"which\":\"we don't care to parse\"}"}
```
