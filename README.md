Thrift
======

A codec based Thrift library for Torch. Supports very fast deserialization of
arbitrary Thrift binary data to Lua native types. Also includes serialization
of Lua native types back into Thrift binary based on a provided schema.

Reading
-------

Thrift binary data is self descriptive. If you just want to quickly
read it and convert it to Lua native types then no schema is required
when creating the codec.

```lua
local thrift = require 'libthrift'
local codec = thrift.codec()
local binary = io.open('thrift_data.bin', 'r'):read('*all')
local result = codec:read(binary)
print(result)
```

Writing
-------

Writing Thrift binary requires a schema as there is no 1:1 mapping
of Lua and Thrift types. We support all Thrift types and they can
be nested indefinitely.

```lua
local thrift = require 'libthrift'
local codec = thrift.codec({
   ttype = "struct",
   fields = {
      [1] = "i32",
      [2] = "bool",
      [3] = { ttype = "list", value = "double" },
   }
})
local binary = codec:write({
   42,
   true,
   { 3.14, 13.13, 543.21 },
})
io.open('thrift_data.bin', 'w'):write(binary)
```

Codec
-----

The schema table passed into a codec during creation has a simple
format. We support the following Thrift types.

   - void
   - bool
   - byte
   - double
   - i16
   - i32
   - i64
   - string
   - struct
   - map
   - set
   - list

A more complicated schema can be found below.

```lua
local descA = {
   ttype = "struct",
   fields = {
      [1] = "i32",
      [2] = "bool",
      [3] = { ttype = "list", value = "double" },
   }
}
local desc = {
   ttype = "struct",
   fields = {
      [1] = { ttype = "map", key = "i32", value = "i32" },
      [2] = { ttype = "map", key = "i64", value = { ttype = "set", value = "string" } },
      [3] = descA,
      [4] = { ttype = "list", value = descA },
      [5] = { ttype = "set", value = descA },
      [7] = { ttype = "map", key = "i16", value = descA },
   }
}
```

It corresponds to this Thrift file.

```thrift
struct A {
   1: i32 x
   2: bool y
   3: list<double> z
}

struct B {
   1: map<i32, i32> a
   2: map<i64, set<string>> b
   3: A c
   4: list<A> d
   5: set<A> e
   7: map<i16, A> f
}
```

Lua and 64 bit integers
-----------------------

Lua 5.1 and earlier uses doubles as its internal number format.
That means we can not represent the full range of i64 values
natively in the Lua VM. The default behavior is to throw an error
when reading or writing any value that would be out of range
for either Thrift or for Lua. That works for most cases,
however if you need the full range of i64 you can tell the
codec to turn i64 values into strings and vice versa on write.

```lua
local codec = thrift.codec({ i64string = true })
```
