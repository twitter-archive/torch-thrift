local thrift = require 'libthrift'
local codec = thrift.codec({
   ttype = "struct",
   fields = {
      [1] = { ttype = "i32", name = "an_int" },
      [2] = { ttype = "bool", name = "someBoolean" },
      [3] = { ttype = "list", value = "double", name = "vector" },
   }
})
local binary = codec:write({
   an_int = 42,
   someBoolean = true,
   vector = { 3.14, 13.13, 543.21 },
})
local f = io.open('thrift_data.bin', 'w')
f:write(binary)
f:close()
print('Wrote '..string.len(binary)..' bytes to "thrift_data.bin".')
