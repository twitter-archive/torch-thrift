local thrift = require 'libthrift'
local codec = thrift.codec()
local f = io.open('thrift_data.bin', 'r')
if not f then
   error('Please run "th write.lua" first to generate binary data.')
end
local binary = f:read('*all')
f:close()
print('Read '..string.len(binary)..' bytes from "thrift_data.bin".')
local result = codec:read(binary)
print(result)
