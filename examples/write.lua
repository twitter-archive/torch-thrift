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
local f = io.open('thrift_data.bin', 'w')
f:write(binary)
f:close()
print('Wrote '..string.len(binary)..' bytes to "thrift_data.bin".')
