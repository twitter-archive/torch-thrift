local test = require 'regress'
local thrift = require 'libthrift'

local function mustMatchA(actual, expected)
   assert(actual[1] == expected[1])
   assert(actual[2] == expected[2])
   for i,a in ipairs(expected[3]) do
      assert(actual[3][i] == a)
   end
end

-- See testSomeThrift below.
local function verifyDeserializationForSomeThrift(deser)
   -- 1: a = Map(22 -> 33),
   assert(deser[1] ~= nil)
   assert(deser[1][22] == 33)

   -- 2: b = Map(99L -> Set("aaaa")),
   assert(deser[2] ~= nil)
   assert(deser[2][99] ~= nil)
   assert(deser[2][99][1] == "aaaa")

   -- 3: c = anA
   -- val anA = A(x=0, y=false, z=List(8,7,6))
   mustMatchA(deser[3], {[1] = 0, [2] = false, [3] = {8, 7, 6}})

   -- 4: d = listA
   -- val listA = List(
   --   A(x=1, y=true, z=List(2.2, 3.3, 9.9)),
   --   A(x=2, y=false, z=List(5.5, 4.4, 7.7))
   -- )
   assert(#deser[4] == 2)
   mustMatchA(deser[4][1], {[1] = 1, [2] = true, [3] = {2.2, 3.3, 9.9}})
   mustMatchA(deser[4][2], {[1] = 2, [2] = false, [3] = {5.5, 4.4, 7.7}})

   -- 5: e = setA
   -- val setA = Set(
   --    A(x=11, y=true, z=List(2.2, 3.3, 9.9)),
   --    A(x=22, y=false, z=List(5.5, 4.4, 7.7)),
   --    A(x=33, y=true, z=List(6.6, 1.1, 4.4))
   --  )
   assert(#deser[5] == 3)
   mustMatchA(deser[5][1], {[1] = 11, [2] = true, [3] = {2.2, 3.3, 9.9}})
   mustMatchA(deser[5][2], {[1] = 22, [2] = false, [3] = {5.5, 4.4, 7.7}})
   mustMatchA(deser[5][3], {[1] = 33, [2] = true, [3] = {6.6, 1.1, 4.4}})

   -- 7: f = mapA
   -- val mapA = Map(
   --   99.toShort -> A(x=44, y=true, z=List(1)),
   --   88.toShort -> A(x=55, y=false, z=List(2))
   -- )
   mustMatchA(deser[7][99], {[1] = 44, [2] = true, [3] = {[1] = 1}})
   mustMatchA(deser[7][88], {[1] = 55, [2] = false, [3] = {[1] = 2}})
end

-- See testDataRecord below.
local function verifyDeserializationForDataRecord(deser)
   assert(#deser == 6)

   assert(deser[1] ~= nil)
   assert(deser[1][1] == 33)
   assert(deser[1][2] == 66)

   assert(deser[2] ~= nil)
   assert(deser[2][22] == 222.0)
   assert(deser[2][33] == 333.0)

   assert(deser[3] ~= nil)
   assert(deser[3][44] == 444)
   assert(deser[3][55] == 444)
   assert(deser[3][66] == 666)

   assert(deser[4] ~= nil)
   assert(deser[4][111] == "fff")
   assert(deser[4][222] == "ggg")
   assert(deser[4][333] == "hhh")

   assert(deser[5] ~= nil)
   assert(deser[5][77] ~= nil)
   assert(deser[5][77][1] == "bbbb")
   assert(deser[5][77][2] == "aaa")
   assert(deser[5][77][3] == "yyyyy")
   assert(deser[5][88] ~= nil)
   assert(deser[5][88][1] == "zzzzzzzz")
   assert(deser[5][88][2] == "cccccc")
   assert(deser[5][88][3] == "ddddddd")

   assert(deser[6] ~= nil)
   assert(deser[6][99] ~= nil)
   assert(deser[6][99]["eee"] == 99.99)
   assert(deser[6][99]["fff"] == 98.98)
   assert(deser[6][98] ~= nil)
   assert(deser[6][98]["ggg"] == 199.99)
   assert(deser[6][98]["hhh"] == 198.98)
end

local function fromBytes(data)
   local ser = ""
   for _,c in ipairs(data) do
      ser = ser .. string.char(c)
   end
   return ser
end

local function pass(c, x, d)
   local y = c:read(d and fromBytes(d) or c:write(x))
   if torch.isTensor(x) then
      assert(torch.all(torch.eq(x, y)), 'expected\n'..tostring(x)..'\nbut got\n'..tostring(y))
   else
      assert(x == y, 'expected '..x..' but got '..y)
   end
end

local function fail(c, x, d)
   local ok, y = pcall(function() return c:read(d and fromBytes(d) or c:write(x)) end)
   assert(ok == false, 'expected '..x..' to fail but got '..tostring(y))
end

test {
   testSomeThrift = function()
      --[[
      Table "data" below contains an array of bytes, and represents a test Thrift struct with
      the following schema:

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

      serialized with the BinaryScalaCodec. The struct has been initialized using the following
      Scala code:

      val anA = A(x=0, y=false, z=List(8,7,6))
      val listA = List(
      A(x=1, y=true, z=List(2.2, 3.3, 9.9)),
      A(x=2, y=false, z=List(5.5, 4.4, 7.7))
      )
      val setA = Set(
      A(x=11, y=true, z=List(2.2, 3.3, 9.9)),
      A(x=22, y=false, z=List(5.5, 4.4, 7.7)),
      A(x=33, y=true, z=List(6.6, 1.1, 4.4) )
      )
      val mapA = Map(
      99.toShort -> A(x=44, y=true, z=List(1)),
      88.toShort -> A(x=55, y=false, z=List(2))
      )
      val myB = B(
      a = Map(22 -> 33),
      b = Map(99L -> Set("aaaa")),
      c = anA,
      d = listA,
      e = setA,
      f = mapA
      )
      --]]

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
      local codec = thrift.codec(desc)

      local data = {
         13, 0, 1, 8, 8, 0, 0, 0, 1, 0, 0, 0, 22, 0, 0, 0, 33, 13, 0, 2, 10, 14, 0, 0, 0, 1, 0, 0, 0, 0,
         0, 0, 0, 99, 11, 0, 0, 0, 1, 0, 0, 0, 4, 97, 97, 97, 97, 12, 0, 3, 8, 0, 1, 0, 0, 0, 0, 2, 0, 2,
         0, 15, 0, 3, 4, 0, 0, 0, 3, 64, 32, 0, 0, 0, 0, 0, 0, 64, 28, 0, 0, 0, 0, 0, 0, 64, 24, 0, 0, 0,
         0, 0, 0, 0, 15, 0, 4, 12, 0, 0, 0, 2, 8, 0, 1, 0, 0, 0, 1, 2, 0, 2, 1, 15, 0, 3, 4, 0, 0, 0, 3,
         64, 1, 153, 153, 153, 153, 153, 154, 64, 10, 102, 102, 102, 102, 102, 102, 64, 35, 204, 204, 204,
         204, 204, 205, 0, 8, 0, 1, 0, 0, 0, 2, 2, 0, 2, 0, 15, 0, 3, 4, 0, 0, 0, 3, 64, 22, 0, 0, 0, 0, 0,
         0, 64, 17, 153, 153, 153, 153, 153, 154, 64, 30, 204, 204, 204, 204, 204, 205, 0, 14, 0, 5, 12, 0,
         0, 0, 3, 8, 0, 1, 0, 0, 0, 11, 2, 0, 2, 1, 15, 0, 3, 4, 0, 0, 0, 3, 64, 1, 153, 153, 153, 153, 153,
         154, 64, 10, 102, 102, 102, 102, 102, 102, 64, 35, 204, 204, 204, 204, 204, 205, 0, 8, 0, 1, 0, 0,
         0, 22, 2, 0, 2, 0, 15, 0, 3, 4, 0, 0, 0, 3, 64, 22, 0, 0, 0, 0, 0, 0, 64, 17, 153, 153, 153, 153,
         153, 154, 64, 30, 204, 204, 204, 204, 204, 205, 0, 8, 0, 1, 0, 0, 0, 33, 2, 0, 2, 1, 15, 0, 3, 4,
         0, 0, 0, 3, 64, 26, 102, 102, 102, 102, 102, 102, 63, 241, 153, 153, 153, 153, 153, 154, 64, 17,
         153, 153, 153, 153, 153, 154, 0, 13, 0, 7, 6, 12, 0, 0, 0, 2, 0, 99, 8, 0, 1, 0, 0, 0, 44, 2, 0,
         2, 1, 15, 0, 3, 4, 0, 0, 0, 1, 63, 240, 0, 0, 0, 0, 0, 0, 0, 0, 88, 8, 0, 1, 0, 0, 0, 55, 2, 0,
         2, 0, 15, 0, 3, 4, 0, 0, 0, 1, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0
      }

      local ser = ""
      for _,c in ipairs(data) do ser = ser .. string.char(c) end

      local deser = codec:read(ser)

      verifyDeserializationForSomeThrift(deser);

      --[[
      Round-trip to/from serializer and verify content again.
      --]]
      local ser2 = codec:write(deser)
      local deser2 = codec:read(ser2)
      verifyDeserializationForSomeThrift(deser2);
   end,

   testDataRecord = function()
      --[[
      Table "data" below contains an array of bytes, and represents a com.twitter.ml.api.DataRecord,
      a Thrift struct with the following schema:
      struct DataRecord {
      1: optional set<i64> binaryFeatures;                       // stores BINARY features
      2: optional map<i64, double> continuousFeatures;         // stores CONTINUOUS features
      3: optional map<i64, i64> discreteFeatures;              // stores DISCRETE features
      4: optional map<i64, string> stringFeatures;             // stores STRING features
      5: optional map<i64, set<string>> sparseBinaryFeatures;  // stores sparse BINARY features
      6: optional map<i64, map<string, double>> sparseContinuousFeatures; // sparse CONTINUOUS features
      }

      The DataRecord has been initialized using the following Scala code:
      import java.util.{HashMap => JHashMap, HashSet => JHashSet, Map => JMap, Set => JSet}
      import java.lang.{Long => JLong, Double => JDouble, String => JString}
      // [... blah blah ...]
      val dr = new DataRecord()
      // Binary features.
      dr.addToBinaryFeatures(33L)
      dr.addToBinaryFeatures(66L)
      // Continuous features.
      dr.setContinuousFeatures(new JHashMap[JLong, JDouble])
      dr.getContinuousFeatures.put(22L, 222.0)
      dr.getContinuousFeatures.put(33L, 333.0)
      // Discrete features.
      dr.setDiscreteFeatures(new JHashMap[JLong, JLong])
      dr.getDiscreteFeatures.put(44L, 444L)
      dr.getDiscreteFeatures.put(55L, 444L)
      dr.getDiscreteFeatures.put(66L, 666L)
      // Sparse binary features.
      dr.setSparseBinaryFeatures(new JHashMap[JLong, JSet[JString] ])
      dr.getSparseBinaryFeatures.put(77L, new JHashSet[JString])
      dr.getSparseBinaryFeatures.get(77L).add("aaa")
      dr.getSparseBinaryFeatures.get(77L).add("bbbb")
      dr.getSparseBinaryFeatures.get(77L).add("yyyyy")
      dr.getSparseBinaryFeatures.put(88L, new JHashSet[JString])
      dr.getSparseBinaryFeatures.get(88L).add("cccccc")
      dr.getSparseBinaryFeatures.get(88L).add("ddddddd")
      dr.getSparseBinaryFeatures.get(88L).add("zzzzzzzz")
      // Sparse continuous features.
      dr.setSparseContinuousFeatures(new JHashMap[JLong, JMap[JString, JDouble] ])
      dr.getSparseContinuousFeatures.put(99L, new JHashMap[JString, JDouble])
      dr.getSparseContinuousFeatures.get(99L).put("eee", 99.99)
      dr.getSparseContinuousFeatures.get(99L).put("fff", 98.98)
      dr.getSparseContinuousFeatures.put(98L, new JHashMap[JString, JDouble])
      dr.getSparseContinuousFeatures.get(98L).put("ggg", 199.99)
      dr.getSparseContinuousFeatures.get(98L).put("hhh", 198.98)
      // String features.
      dr.setStringFeatures(new JHashMap[JLong, JString])
      dr.getStringFeatures.put(111L, "fff")
      dr.getStringFeatures.put(222L, "ggg")
      dr.getStringFeatures.put(333L, "hhh")

      and serialized with the BinaryThriftCodec.
      --]]

      local desc = {
         ttype = "struct",
         fields = {
            [1] = { ttype = "set", value = "i64" },
            [2] = { ttype = "map", key = "i64", value = "double" },
            [3] = { ttype = "map", key = "i64", value = "i64" },
            [4] = { ttype = "map", key = "i64", value = "string" },
            [5] = { ttype = "map", key = "i64", value = { ttype = "set", value = "string" } },
            [6] = { ttype = "map", key = "i64", value = { ttype = "map", key = "string", value = "double" } },
         }
      }
      local codec = thrift.codec(desc)

      local data = {
         14, 0, 1, 10, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 33, 0, 0, 0, 0, 0, 0, 0, 66, 13, 0, 2, 10, 4,
         0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 33, 64, 116, 208, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 22,
         64, 107, 192, 0, 0, 0, 0, 0, 13, 0, 3, 10, 10, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 55, 0, 0,
         0, 0, 0, 0, 1, 188, 0, 0, 0, 0, 0, 0, 0, 66, 0, 0, 0, 0, 0, 0, 2, 154, 0, 0, 0, 0, 0, 0, 0,
         44, 0, 0, 0, 0, 0, 0, 1, 188, 13, 0, 4, 10, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 222, 0, 0,
         0, 3, 103, 103, 103, 0, 0, 0, 0, 0, 0, 0, 111, 0, 0, 0, 3, 102, 102, 102, 0, 0, 0, 0, 0, 0,
         1, 77, 0, 0, 0, 3, 104, 104, 104, 13, 0, 5, 10, 14, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 77, 11,
         0, 0, 0, 3, 0, 0, 0, 4, 98, 98, 98, 98, 0, 0, 0, 3, 97, 97, 97, 0, 0, 0, 5, 121, 121, 121,
         121, 121, 0, 0, 0, 0, 0, 0, 0, 88, 11, 0, 0, 0, 3, 0, 0, 0, 8, 122, 122, 122, 122, 122, 122,
         122, 122, 0, 0, 0, 6, 99, 99, 99, 99, 99, 99, 0, 0, 0, 7, 100, 100, 100, 100, 100, 100, 100,
         13, 0, 6, 10, 13, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 98, 11, 4, 0, 0, 0, 2, 0, 0, 0, 3, 104,
         104, 104, 64, 104, 223, 92, 40, 245, 194, 143, 0, 0, 0, 3, 103, 103, 103, 64, 104, 255, 174,
         20, 122, 225, 72, 0, 0, 0, 0, 0, 0, 0, 99, 11, 4, 0, 0, 0, 2, 0, 0, 0, 3, 102, 102, 102, 64,
         88, 190, 184, 81, 235, 133, 31, 0, 0, 0, 3, 101, 101, 101, 64, 88, 255, 92, 40, 245, 194, 143, 0
      }

      local ser = ""
      for _,c in ipairs(data) do ser = ser .. string.char(c) end
      local deser = codec:read(ser)

      --[[
      Verify content.
      --]]
      verifyDeserializationForDataRecord(deser)

      --[[
      Round-trip to/from serializer and verify content again.
      --]]
      local ser2 = codec:write(deser)
      local deser2 = codec:read(ser2)
      verifyDeserializationForDataRecord(deser2);
   end,

   testIntegerLimits = function()
      -- byte is 100% representable as a double
      pass(thrift.codec("byte"), 0)
      pass(thrift.codec("byte"), 255)
      fail(thrift.codec("byte"), -1)
      fail(thrift.codec("byte"), 256)
      fail(thrift.codec("byte"), 13.42)

      -- i16 is 100% representable as a double
      pass(thrift.codec("i16"), 32767)
      fail(thrift.codec("i16"), 32768)
      pass(thrift.codec("i16"), -32768)
      fail(thrift.codec("i16"), -32769)
      fail(thrift.codec("i16"), 13.42)

      -- i32 is 100% representable as a double
      pass(thrift.codec("i32"), 2147483647)
      fail(thrift.codec("i32"), 2147483648)
      pass(thrift.codec("i32"), -2147483648)
      fail(thrift.codec("i32"), -2147483649)
      fail(thrift.codec("i32"), 13.42)

      -- i64 up to 53 bits is ok
      pass(thrift.codec("i64"),  9007199254740991)
      pass(thrift.codec("i64"),  9007199254740991,    {0x00,0x1F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF})
      -- but 54 bits will lose precision
      fail(thrift.codec("i64"), '0x003FFFFFFFFFFFFF', {0x00,0x3F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF})
      -- and max positive i64 will never work
      fail(thrift.codec("i64"), '0x7FFFFFFFFFFFFFFF', {0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF})
      -- i64 down to negative 53 bits is ok
      pass(thrift.codec("i64"),  -9007199254740991)
      pass(thrift.codec("i64"),  -9007199254740991,   {0xFF,0xE0,0x00,0x00,0x00,0x00,0x00,0x01})
      -- but negative 54 bits will lose precision
      fail(thrift.codec("i64"), 'FFC0000000000001',   {0xFF,0xC0,0x00,0x00,0x00,0x00,0x00,0x01})
      fail(thrift.codec("i64"), 13.42)
   end,

   testI64Strings = function()
      local c = thrift.codec({ ttype = "i64", i64string = true })
      -- i64 up to (2^63)-1 is ok
      pass(c, "9223372036854775807")
      -- and down to -(2^63) is ok
      pass(c, "-9223372036854775808")
      -- same from binary (2^63)-1
      pass(c, "9223372036854775807", {0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF})
      -- and from binary -(2^63)
      pass(c, "-9223372036854775808", {0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x00})
      -- a bunch of corners should fail
      fail(c, "99a")
      fail(c, "a99")
      fail(c, "-xuehf")
      fail(c, "3.1452")
      fail(c, "3.1452e13")
      fail(c, "")
      fail(c, "9223372036854775808")
      fail(c, "-9223372036854775809")
   end,

   testI64StringsSimpleReader = function()
      -- test a simple reader case
      local c0 = thrift.codec({ ttype = "struct", i64string = true, fields = { "i64" } })
      local c1 = thrift.codec({ i64string = true })
      local s = c1:read(c0:write({ "9223372036854775807" }))
      assert(s[1] == "9223372036854775807", 'expected a struct with a single field of 9223372036854775807')
   end,

   testNamedFields = function()
      local codec = thrift.codec({
         ttype = "struct",
         fields = {
            [1] = { ttype = "i32", name = "some_int" },
            [2] = "string",
            [3] = { ttype = "string", name = "named_string" },
            [4] = { ttype = "set", name = "a_set", value = {
                     ttype = "struct",
                     fields = {
                        [1] = { ttype = "i32", name = "inner" },
                     },
                  }
               },
            [5] = { ttype = "map", name = "a_map",
                     key = "string",
                     value = {
                        ttype = "struct",
                        fields = {
                           [1] = { ttype = "double", name = "y" },
                        },
                     }
               },
         },
      })
      local a_map = { }
      a_map["x"] = { y = 3.14 }
      local data = {
         some_int = 42,
         [2] = "hello",
         named_string = "whatup",
         a_set = { { inner = 13 }, { inner = 27 } },
         a_map = a_map,
      }
      local result = codec:read(codec:write(data))
      assert(result.some_int == data.some_int)
      assert(result[2] == data[2])
      assert(result.named_string == data.named_string)
      assert(result.a_set[1].inner == data.a_set[1].inner)
      assert(result.a_set[2].inner == data.a_set[2].inner)
      assert(result.a_map["x"].y == data.a_map["x"].y)
   end,

   testI64Tensors = function()
      local c = thrift.codec({ ttype = "i64", i64tensor = true })
      pass(c, torch.LongTensor({12345}))
      pass(c, torch.LongTensor({0}))
      pass(c, torch.LongTensor({-42}))
   end,

   testTensors = function()
      local outers = { "list", "set" }
      local inners = { byte = "Byte", double = "Double", i16 = "Short", i32 = "Int", i64 = "Long" }
      for _,outer in ipairs(outers) do
         for inner,tt in pairs(inners) do
            local t = torch.Tensor(math.random(1, 100)):type("torch."..tt.."Tensor")
            for i = 1,t:size(1) do
               t[i] = math.random(0, 1000) / 100
            end
            local c = thrift.codec({ ttype = outer, value = inner, tensors = true })
            pass(c, t)
         end
      end
   end,

   testReadWriteTensors = function()
      local codec = thrift.codec({ ttype = 'struct', fields = { 'i32', 'string' } })
      local bytes = codec:writeTensor({ 13, 'hello' })
      assert(bytes:size(1) == 20, bytes:size(1))
      local result = codec:readTensor(bytes)
      assert(result[1] == 13)
      assert(result[2] == 'hello')
   end,

   testUnions = function()
      local Tweet = {
         name = "tweet",
         ttype = "struct",
         fields = {
            [1] = { ttype = "string", name = "text" },
            [2] = { ttype = "list", value = "string", name = "penguinTokens" },
         },
      }
      local Image = {
         name = "image",
         ttype = "struct",
         fields = {
            [1] = 'string'
         },
      }
      local Video = {
         name = "video",
         ttype = "struct",
         fields = {
            [1] = 'string'
         },
      }
      local Request = {
         ttype = "struct",
         fields = {
            [1] = Tweet,
            [2] = Image,
            [3] = Video
         }
      }
      local thrift = require 'libthrift'
      local codec = thrift.codec(Request)
      codec:write({ })
      codec:write({ image = { 'jpeg' } })
      codec:write({ video = { 'mp4' }, image = { 'jpeg' } })
      codec:write({ video = { 'mp4' } })
      codec:write({ tweet = { text = 'tweet', penguinTokens = { "a", "b" } } })
   end,
}
