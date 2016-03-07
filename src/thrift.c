#include <TH/TH.h>
#include "luaT.h"
#include "endianutils.h"
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>

#define TTYPE_STOP   (0)
#define TTYPE_VOID   (1)
#define TTYPE_BOOL   (2)
#define TTYPE_BYTE   (3)
#define TTYPE_DOUBLE (4)
#define TTYPE_I16    (6)
#define TTYPE_I32    (8)
#define TTYPE_I64    (10)
#define TTYPE_STRING (11)
#define TTYPE_STRUCT (12)
#define TTYPE_MAP    (13)
#define TTYPE_SET    (14)
#define TTYPE_LIST   (15)
#define TTYPE_ENUM   (16)

static int _lua_error(lua_State *L, int ret, const char* file, int line) {
   int pos_ret = ret < 0 ? -ret : ret;
   return luaL_error(L, "Thrift Error: (%s, %d): (%d, %s)\n", file, line, pos_ret, strerror(pos_ret));
}

static int _lua_error_str(lua_State *L, const char *str, const char* file, int line) {
   return luaL_error(L, "Thrift Error: (%s, %d): (%s)\n", file, line, str);
}

#define LUA_HANDLE_ERROR(L, ret) _lua_error(L, ret, __FILE__, __LINE__)
#define LUA_HANDLE_ERROR_STR(L, str) _lua_error_str(L, str, __FILE__, __LINE__)

static int thrift_ttype(lua_State *L, const char *sz) {
   if (strcmp(sz, "void") == 0) return TTYPE_VOID;
   else if (strcmp(sz, "bool") == 0) return TTYPE_BOOL;
   else if (strcmp(sz, "byte") == 0) return TTYPE_BYTE;
   else if (strcmp(sz, "double") == 0) return TTYPE_DOUBLE;
   else if (strcmp(sz, "i16") == 0) return TTYPE_I16;
   else if (strcmp(sz, "i32") == 0) return TTYPE_I32;
   else if (strcmp(sz, "i64") == 0) return TTYPE_I64;
   else if (strcmp(sz, "string") == 0) return TTYPE_STRING;
   else if (strcmp(sz, "struct") == 0) return TTYPE_STRUCT;
   else if (strcmp(sz, "map") == 0) return TTYPE_MAP;
   else if (strcmp(sz, "set") == 0) return TTYPE_SET;
   else if (strcmp(sz, "list") == 0) return TTYPE_LIST;
   else return LUA_HANDLE_ERROR(L, EINVAL);
}

typedef struct buffer_t {
   uint8_t *data;
   size_t cb;
   size_t max_cb;
} buffer_t;

#define MAX(a,b) (((a)>(b))?(a):(b))

#define WRITE(L, src, srccb, b) \
   while ((b)->cb + (srccb) > (b)->max_cb) { \
      (b)->max_cb = MAX((b)->max_cb * 2, 256); \
      (b)->data = (uint8_t *)realloc((b)->data, (b)->max_cb); \
   } \
   memcpy((b)->data + (b)->cb, (src), (srccb)); \
   (b)->cb += (srccb);

#define READ(L, dst, dstcb, b) \
   if ((b)->max_cb - (b)->cb < (dstcb)) return LUA_HANDLE_ERROR(L, ENOMEM); \
   memcpy((dst), (b)->data + (b)->cb, (dstcb)); \
   (b)->cb += (dstcb);

#define READN(L, dstcb, b) \
   if ((b)->max_cb - (b)->cb < (dstcb)) return LUA_HANDLE_ERROR(L, ENOMEM); \
   (b)->cb += (dstcb);

#define I64_AS_NUMBER            (0)
#define I64_AS_STRING            (1)
#define I64_AS_TENSOR            (2)
#define I64_AS_MASK              (3)
#define LIST_AND_SET_AS_TENSOR   (4)

typedef struct desc_t {
   struct desc_t *key_ttype;
   struct desc_t *value_ttype;
   struct desc_t *fields;
   uint16_t num_fields;
   uint16_t field_id;
   uint8_t ttype;
   int flags;
   const char *field_name;
} desc_t;

static int _compare(const void *a, const void *b) {
   return (int)((desc_t *)a)->field_id - (int)((desc_t *)b)->field_id;
}

static int thrift_desc_rcsv(lua_State *L, int index, desc_t *desc) {
   if (lua_type(L, index) == LUA_TSTRING) {
      desc->ttype = thrift_ttype(L, lua_tostring(L, index));
      return 0;
   } else if (lua_type(L, index) == LUA_TTABLE) {
      lua_pushstring(L, "i64string");
      lua_gettable(L, index);
      if (lua_toboolean(L, lua_gettop(L))) {
         desc->flags |= I64_AS_STRING;
      }
      lua_pop(L, 1);
      lua_pushstring(L, "i64tensor");
      lua_gettable(L, index);
      if (lua_toboolean(L, lua_gettop(L))) {
         desc->flags |= I64_AS_TENSOR;
      }
      lua_pop(L, 1);
      lua_pushstring(L, "tensors");
      lua_gettable(L, index);
      if (lua_toboolean(L, lua_gettop(L))) {
         desc->flags |= LIST_AND_SET_AS_TENSOR;
      }
      lua_pop(L, 1);
      lua_pushstring(L, "ttype");
      lua_gettable(L, index);
      if (lua_type(L, lua_gettop(L)) == LUA_TNIL) {
         desc->ttype = TTYPE_STRUCT;
         lua_pop(L, 1);
         return 0;
      }
      lua_pushstring(L, "name");
      lua_gettable(L, index);
      const char *name = lua_tostring(L, lua_gettop(L));
      if (name) {
         desc->field_name = strdup(name);
      }
      lua_pop(L, 1);
      desc->ttype = thrift_ttype(L, lua_tostring(L, lua_gettop(L)));
      lua_pop(L, 1);
      switch (desc->ttype) {
         case TTYPE_STRUCT:
            lua_pushstring(L, "fields");
            lua_gettable(L, index);
            int fields = lua_gettop(L);
            lua_pushnil(L);
            desc->num_fields = 0;
            while (lua_next(L, fields) != 0) {
               desc->fields = (desc_t *)realloc(desc->fields, (desc->num_fields + 1) * sizeof(desc_t));
               memset(&desc->fields[desc->num_fields], 0, sizeof(desc_t));
               desc->fields[desc->num_fields].field_id = lua_tointeger(L, fields + 1);
               thrift_desc_rcsv(L, fields + 2, &desc->fields[desc->num_fields]);
               lua_pop(L, 1);
               desc->num_fields++;
            }
            qsort(desc->fields, desc->num_fields, sizeof(desc_t), _compare);
            lua_pop(L, 1);
            return 0;
         case TTYPE_MAP:
            lua_pushstring(L, "key");
            lua_gettable(L, index);
            desc->key_ttype = calloc(1, sizeof(desc_t));
            thrift_desc_rcsv(L, lua_gettop(L), desc->key_ttype);
            lua_pop(L, 1);
            lua_pushstring(L, "value");
            lua_gettable(L, index);
            desc->value_ttype = calloc(1, sizeof(desc_t));
            thrift_desc_rcsv(L, lua_gettop(L), desc->value_ttype);
            lua_pop(L, 1);
            return 0;
         case TTYPE_SET:
         case TTYPE_LIST:
            lua_pushstring(L, "value");
            lua_gettable(L, index);
            desc->value_ttype = calloc(1, sizeof(desc_t));
            thrift_desc_rcsv(L, lua_gettop(L), desc->value_ttype);
            lua_pop(L, 1);
            return 0;
      }
      return 0;
   }
   return LUA_HANDLE_ERROR_STR(L, "expected a string or a table");
}

static int thrift_desc(lua_State *L) {
   desc_t *desc = (desc_t *)lua_newuserdata(L, sizeof(desc_t));
   memset(desc, 0, sizeof(desc_t));
   if (lua_gettop(L) > 1) {
      thrift_desc_rcsv(L, 1, desc);
   } else {
      desc->ttype = TTYPE_STRUCT;
   }
   luaL_getmetatable(L, "thrift.codec");
   lua_setmetatable(L, -2);
   return 1;
}

static void thrift_destroy_desc_rcsv(desc_t *desc) {
   if (desc->key_ttype) {
      thrift_destroy_desc_rcsv(desc->key_ttype);
      free(desc->key_ttype);
   }
   if (desc->value_ttype) {
      thrift_destroy_desc_rcsv(desc->value_ttype);
      free(desc->value_ttype);
   }
   for (uint16_t i = 0; i < desc->num_fields; i++) {
      thrift_destroy_desc_rcsv(&desc->fields[i]);
   }
   free(desc->fields);
   free((void *)desc->field_name);
}

static int thrift_gc(lua_State *L) {
   desc_t *desc = (desc_t *)lua_touserdata(L, 1);
   thrift_destroy_desc_rcsv(desc);
   return 0;
}

static int thrift_read_rcsv(lua_State *L, uint8_t ttype, buffer_t *in, int flags, desc_t *desc, void *out) {
   switch (ttype) {
      case TTYPE_STOP:
         return 0;
      case TTYPE_VOID:
         return 0;
      case TTYPE_BOOL: {
         uint8_t i8;
         READ(L, &i8, sizeof(i8), in)
         lua_pushboolean(L, i8 != 0);
         return 1;
      }
      case TTYPE_BYTE: {
         uint8_t i8;
         READ(L, &i8, sizeof(i8), in)
         if (out) {
            memcpy(out, &i8, sizeof(i8));
            return 0;
         } else {
            lua_pushinteger(L, i8);
            return 1;
         }
      }
      case TTYPE_DOUBLE: {
         int64_t i64;
         READ(L, &i64, sizeof(i64), in)
         i64 = betoh64(i64);
         if (out) {
            memcpy(out, &i64, sizeof(i64));
            return 0;
         } else {
            double d;
            memcpy(&d, &i64, sizeof(i64));
            lua_pushnumber(L, d);
            return 1;
         }
      }
      case TTYPE_I16: {
         int16_t i16;
         READ(L, &i16, sizeof(i16), in)
         i16 = betoh16(i16);
         if (out) {
            memcpy(out, &i16, sizeof(i16));
            return 0;
         } else {
            double d = i16;
            lua_pushnumber(L, d);
            return 1;
         }
      }
      case TTYPE_I32:
      case TTYPE_ENUM: {
         int32_t i32;
         READ(L, &i32, sizeof(i32), in)
         i32 = betoh32(i32);
         if (out) {
            memcpy(out, &i32, sizeof(i32));
            return 0;
         } else {
            double d = i32;
            lua_pushnumber(L, d);
            return 1;
         }
      }
      case TTYPE_I64: {
         int64_t i64;
         READ(L, &i64, sizeof(i64), in)
         i64 = betoh64(i64);
         if (out) {
            memcpy(out, &i64, sizeof(i64));
            return 0;
         }
         switch (flags & I64_AS_MASK) {
            case I64_AS_NUMBER: {
               double d = i64;
               if ((int64_t)d != i64) {
                  return LUA_HANDLE_ERROR_STR(L, "i64 value out of range");
               }
               lua_pushnumber(L, d);
               return 1;
            }
            case I64_AS_STRING: {
               char sz[256];
               snprintf(sz, 256, "%" PRId64 "", i64);
               lua_pushstring(L, sz);
               return 1;
            }
            case I64_AS_TENSOR: {
               THLongStorage *values = THLongStorage_newWithSize(1);
               values->data[0] = i64;
               THLongStorage *size = THLongStorage_newWithSize(1);
               size->data[0] = values->size;
               THLongTensor *tensor = THLongTensor_newWithStorage(values, 0, size, NULL);
               THLongStorage_free(size);
               luaT_pushudata(L, tensor, "torch.LongTensor");
               return 1;
            }
            default:
               return LUA_HANDLE_ERROR_STR(L, "unknown flags value");
         }
      }
      case TTYPE_STRING: {
         int32_t i32;
         READ(L, &i32, sizeof(i32), in)
         i32 = betoh32(i32);
         const char *str = (const char *)(in->data + in->cb);
         READN(L, (uint32_t)i32, in)
         lua_pushlstring(L, str, i32);
         return 1;
      }
      case TTYPE_STRUCT: {
         lua_newtable(L);
         uint8_t vt;
         READ(L, &vt, sizeof(vt), in)
         while (vt != TTYPE_STOP) {
            uint16_t fid;
            READ(L, &fid, sizeof(fid), in)
            fid = betoh16(fid);
            desc_t *field_desc = NULL;
            if (desc) {
               for (uint16_t i = 0; i < desc->num_fields; i++) {
                  if (desc->fields[i].field_id == fid) {
                     field_desc = &desc->fields[i];
                     break;
                  }
               }
               if (field_desc == NULL && desc->num_fields > 0) {
                  return LUA_HANDLE_ERROR_STR(L, "field id value out of range for struct");
               }
            }
            if (field_desc && field_desc->field_name) {
               lua_pushstring(L, field_desc->field_name);
            } else {
               lua_pushinteger(L, fid);
            }
            thrift_read_rcsv(L, vt, in, flags, field_desc, NULL);
            lua_settable(L, -3);
            READ(L, &vt, sizeof(vt), in)
         }
         return 1;
      }
      case TTYPE_MAP: {
         lua_newtable(L);
         uint8_t kt;
         READ(L, &kt, sizeof(kt), in)
         uint8_t vt;
         READ(L, &vt, sizeof(vt), in)
         int32_t i32;
         READ(L, &i32, sizeof(i32), in)
         i32 = betoh32(i32);
         while (i32 > 0) {
            thrift_read_rcsv(L, kt, in, flags, desc ? desc->key_ttype : NULL, NULL);
            thrift_read_rcsv(L, vt, in, flags, desc ? desc->value_ttype : NULL, NULL);
            lua_settable(L, -3);
            i32--;
         }
         return 1;
      }
      case TTYPE_SET:
      case TTYPE_LIST: {
         uint8_t vt;
         READ(L, &vt, sizeof(vt), in)
         int32_t i32;
         READ(L, &i32, sizeof(i32), in)
         i32 = betoh32(i32);
         if (flags & LIST_AND_SET_AS_TENSOR) {
            switch (vt) {
               case TTYPE_BYTE: {
                  THByteStorage *values = THByteStorage_newWithSize(i32);
                  for (int32_t i = 0; i < i32; i++) {
                     thrift_read_rcsv(L, vt, in, flags, desc ? desc->value_ttype : NULL, &values->data[i]);
                  }
                  THLongStorage *size = THLongStorage_newWithSize(1);
                  size->data[0] = values->size;
                  luaT_pushudata(L, THByteTensor_newWithStorage(values, 0, size, NULL), "torch.ByteTensor");
                  THLongStorage_free(size);
                  return 1;
               }
               case TTYPE_DOUBLE: {
                  THDoubleStorage *values = THDoubleStorage_newWithSize(i32);
                  for (int32_t i = 0; i < i32; i++) {
                     thrift_read_rcsv(L, vt, in, flags, desc ? desc->value_ttype : NULL, &values->data[i]);
                  }
                  THLongStorage *size = THLongStorage_newWithSize(1);
                  size->data[0] = values->size;
                  luaT_pushudata(L, THDoubleTensor_newWithStorage(values, 0, size, NULL), "torch.DoubleTensor");
                  THLongStorage_free(size);
                  return 1;
               }
               case TTYPE_I16: {
                  THShortStorage *values = THShortStorage_newWithSize(i32);
                  for (int32_t i = 0; i < i32; i++) {
                     thrift_read_rcsv(L, vt, in, flags, desc ? desc->value_ttype : NULL, &values->data[i]);
                  }
                  THLongStorage *size = THLongStorage_newWithSize(1);
                  size->data[0] = values->size;
                  luaT_pushudata(L, THShortTensor_newWithStorage(values, 0, size, NULL), "torch.ShortTensor");
                  THLongStorage_free(size);
                  return 1;
               }
               case TTYPE_I32: {
                  THIntStorage *values = THIntStorage_newWithSize(i32);
                  for (int32_t i = 0; i < i32; i++) {
                     thrift_read_rcsv(L, vt, in, flags, desc ? desc->value_ttype : NULL, &values->data[i]);
                  }
                  THLongStorage *size = THLongStorage_newWithSize(1);
                  size->data[0] = values->size;
                  luaT_pushudata(L, THIntTensor_newWithStorage(values, 0, size, NULL), "torch.IntTensor");
                  THLongStorage_free(size);
                  return 1;
               }
               case TTYPE_I64: {
                  THLongStorage *values = THLongStorage_newWithSize(i32);
                  for (int32_t i = 0; i < i32; i++) {
                     thrift_read_rcsv(L, vt, in, flags, desc ? desc->value_ttype : NULL, &values->data[i]);
                  }
                  THLongStorage *size = THLongStorage_newWithSize(1);
                  size->data[0] = values->size;
                  luaT_pushudata(L, THLongTensor_newWithStorage(values, 0, size, NULL), "torch.LongTensor");
                  THLongStorage_free(size);
                  return 1;
               }
            }
         }
         lua_newtable(L);
         for (int32_t i = 1; i <= i32; i++) {
            lua_pushinteger(L, i);
            thrift_read_rcsv(L, vt, in, flags, desc ? desc->value_ttype : NULL, NULL);
            lua_settable(L, -3);
         }
         return 1;
      }
      default:
         return LUA_HANDLE_ERROR(L, EINVAL);
   }
}

static int thrift_read(lua_State *L) {
   desc_t *desc = (desc_t *)lua_touserdata(L, 1);
   buffer_t in;
   in.data = (uint8_t *)lua_tolstring(L, 2, &in.max_cb);
   in.cb = 0;
   return thrift_read_rcsv(L, desc->ttype, &in, desc->flags, desc, NULL);
}

static int thrift_read_tensor(lua_State *L) {
   desc_t *desc = (desc_t *)lua_touserdata(L, 1);
   THByteTensor *tensor = luaT_toudata(L, 2, "torch.ByteTensor");
   buffer_t in;
   in.data = (uint8_t *)(tensor->storage->data + tensor->storageOffset);
   in.max_cb = tensor->size[0];
   in.cb = 0;
   return thrift_read_rcsv(L, desc->ttype, &in, desc->flags, desc, NULL);
}

static int thrift_write_rcsv(lua_State *L, int index, desc_t *desc, buffer_t *out, int flags, void *in) {
   switch (desc->ttype) {
      case TTYPE_BOOL: {
         uint8_t i8;
         i8 = lua_toboolean(L, index);
         WRITE(L, &i8, sizeof(i8), out)
         return 0;
      }
      case TTYPE_BYTE: {
         uint8_t i8;
         if (in) {
            memcpy(&i8, in, sizeof(i8));
         } else {
            double d = lua_tonumber(L, index);
            i8 = d;
            if ((double)i8 != d) return LUA_HANDLE_ERROR_STR(L, "byte value out of range");
         }
         WRITE(L, &i8, sizeof(i8), out)
         return 0;
      }
      case TTYPE_DOUBLE: {
         int64_t i64;
         if (in) {
            memcpy(&i64, in, sizeof(i64));
         } else {
            double d = lua_tonumber(L, index);
            memcpy(&i64, &d, sizeof(i64));
         }
         i64 = htobe64(i64);
         WRITE(L, &i64, sizeof(i64), out)
         return 0;
      }
      case TTYPE_I16: {
         int16_t i16;
         if (in) {
            memcpy(&i16, in, sizeof(i16));
         } else {
            double d = lua_tonumber(L, index);
            i16 = d;
            if ((double)i16 != d) return LUA_HANDLE_ERROR_STR(L, "i16 value out of range");
         }
         i16 = htobe16(i16);
         WRITE(L, &i16, sizeof(i16), out)
         return 0;
      }
      case TTYPE_I32:
      case TTYPE_ENUM: {
         int32_t i32;
         if (in) {
            memcpy(&i32, in, sizeof(i32));
         } else {
            double d = lua_tonumber(L, index);
            i32 = d;
            if ((double)i32 != d) return LUA_HANDLE_ERROR_STR(L, "i32 value out of range");
         }
         i32 = htobe32(i32);
         WRITE(L, &i32, sizeof(i32), out)
         return 0;
      }
      case TTYPE_I64: {
         int64_t i64;
         if (in) {
            memcpy(&i64, in, sizeof(i64));
         } else {
            switch (flags & I64_AS_MASK) {
               case I64_AS_NUMBER: {
                  double d = lua_tonumber(L, index);
                  i64 = d;
                  if ((double)i64 != d) return LUA_HANDLE_ERROR_STR(L, "i64 value out of range");
                  break;
               }
               case I64_AS_STRING: {
                  size_t len;
                  const char *str = lua_tolstring(L, index, &len);
                  if (str == NULL || len == 0) return LUA_HANDLE_ERROR_STR(L, "i64 can not convert from empty string");
                  char *str_end = (char *)str + len;
                  errno = 0;  // reset errno, strtoll doesn't have a proper return code to indicate true error
                  i64 = strtoll(str, &str_end, 10);
                  if (i64 == 0 && errno == EINVAL) return LUA_HANDLE_ERROR(L, errno);
                  if ((i64 == LLONG_MIN || i64 == LLONG_MAX) && errno == ERANGE) return LUA_HANDLE_ERROR(L, errno);
                  if (str_end != ((char *)str + len)) return LUA_HANDLE_ERROR_STR(L, "i64 did not consume the entire string");
                  break;
               }
               case I64_AS_TENSOR: {
                  THLongTensor *values = luaT_toudata(L, index, "torch.LongTensor");
                  i64 = values->storage->data[values->storageOffset];
                  break;
               }
               default:
                  return LUA_HANDLE_ERROR_STR(L, "unknown flags value");
            }
         }
         i64 = htobe64(i64);
         WRITE(L, &i64, sizeof(i64), out)
         return 0;
      }
      case TTYPE_STRING: {
         size_t len;
         const char *str = lua_tolstring(L, index, &len);
         int32_t i32 = htobe32(len);
         WRITE(L, &i32, sizeof(i32), out)
         WRITE(L, str, len, out)
         return 0;
      }
      case TTYPE_STRUCT: {
         for (int16_t j = 0; j < desc->num_fields; j++) {
            if (desc->fields[j].field_name) {
               lua_pushstring(L, desc->fields[j].field_name);
            } else {
               lua_pushinteger(L, desc->fields[j].field_id);
            }
            lua_rawget(L, index);
            if (lua_type(L, -1) != LUA_TNIL) {
               WRITE(L, &desc->fields[j].ttype, sizeof(uint8_t), out)
               int16_t i16 = htobe16(desc->fields[j].field_id);
               WRITE(L, &i16, sizeof(i16), out)
               thrift_write_rcsv(L, lua_gettop(L), &desc->fields[j], out, flags, NULL);
            }
            lua_pop(L, 1);
         }
         uint8_t i8 = TTYPE_STOP;
         WRITE(L, &i8, sizeof(i8), out)
         return 0;
      }
      case TTYPE_MAP: {
         WRITE(L, &desc->key_ttype->ttype, sizeof(uint8_t), out)
         WRITE(L, &desc->value_ttype->ttype, sizeof(uint8_t), out)
         int32_t i32 = 0;
         lua_pushnil(L);
         while (lua_next(L, index) != 0) {
            i32++;
            lua_pop(L, 1);
         }
         i32 = htobe32(i32);
         WRITE(L, &i32, sizeof(i32), out)
         int top = lua_gettop(L);
         lua_pushnil(L);
         while (lua_next(L, index) != 0) {
            thrift_write_rcsv(L, top + 1, desc->key_ttype, out, flags, NULL);
            thrift_write_rcsv(L, top + 2, desc->value_ttype, out, flags, NULL);
            lua_pop(L, 1);
         }
         return 0;
      }
      case TTYPE_SET:
      case TTYPE_LIST: {
         WRITE(L, &desc->value_ttype->ttype, sizeof(uint8_t), out)
         if (flags & LIST_AND_SET_AS_TENSOR) {
            switch (desc->value_ttype->ttype) {
               case TTYPE_BYTE: {
                  THByteTensor *values = luaT_toudata(L, index, "torch.ByteTensor");
                  if (values->nDimension != 1) return LUA_HANDLE_ERROR_STR(L, "expected a 1 dimensional tensor");
                  int32_t len = values->size[0];
                  int32_t i32 = htobe32(len);
                  WRITE(L, &i32, sizeof(i32), out)
                  for (int32_t i = 0; i < len; i++) {
                     thrift_write_rcsv(L, -1, desc->value_ttype, out, flags, values->storage->data + values->storageOffset + i);
                  }
                  return 0;
               }
               case TTYPE_DOUBLE: {
                  THDoubleTensor *values = luaT_toudata(L, index, "torch.DoubleTensor");
                  if (values->nDimension != 1) return LUA_HANDLE_ERROR_STR(L, "expected a 1 dimensional tensor");
                  int32_t len = values->size[0];
                  int32_t i32 = htobe32(len);
                  WRITE(L, &i32, sizeof(i32), out)
                  for (int32_t i = 0; i < len; i++) {
                     thrift_write_rcsv(L, -1, desc->value_ttype, out, flags, values->storage->data + values->storageOffset + i);
                  }
                  return 0;
               }
               case TTYPE_I16: {
                  THShortTensor *values = luaT_toudata(L, index, "torch.ShortTensor");
                  if (values->nDimension != 1) return LUA_HANDLE_ERROR_STR(L, "expected a 1 dimensional tensor");
                  int32_t len = values->size[0];
                  int32_t i32 = htobe32(len);
                  WRITE(L, &i32, sizeof(i32), out)
                  for (int32_t i = 0; i < len; i++) {
                     thrift_write_rcsv(L, -1, desc->value_ttype, out, flags, values->storage->data + values->storageOffset + i);
                  }
                  return 0;
               }
               case TTYPE_I32: {
                  THIntTensor *values = luaT_toudata(L, index, "torch.IntTensor");
                  if (values->nDimension != 1) return LUA_HANDLE_ERROR_STR(L, "expected a 1 dimensional tensor");
                  int32_t len = values->size[0];
                  int32_t i32 = htobe32(len);
                  WRITE(L, &i32, sizeof(i32), out)
                  for (int32_t i = 0; i < len; i++) {
                     thrift_write_rcsv(L, -1, desc->value_ttype, out, flags, values->storage->data + values->storageOffset + i);
                  }
                  return 0;
               }
               case TTYPE_I64: {
                  THLongTensor *values = luaT_toudata(L, index, "torch.LongTensor");
                  if (values->nDimension != 1) return LUA_HANDLE_ERROR_STR(L, "expected a 1 dimensional tensor");
                  int32_t len = values->size[0];
                  int32_t i32 = htobe32(len);
                  WRITE(L, &i32, sizeof(i32), out)
                  for (int32_t i = 0; i < len; i++) {
                     thrift_write_rcsv(L, -1, desc->value_ttype, out, flags, values->storage->data + values->storageOffset + i);
                  }
                  return 0;
               }
            }
         }
         size_t len = lua_objlen(L, index);
         int32_t i32 = htobe32(len);
         WRITE(L, &i32, sizeof(i32), out)
         int top = lua_gettop(L);
         for (int32_t i = 1; i <= (int32_t)len; i++) {
            lua_rawgeti(L, index, i);
            thrift_write_rcsv(L, top + 1, desc->value_ttype, out, flags, NULL);
            lua_pop(L, 1);
         }
         return 0;
      }
   }
   return LUA_HANDLE_ERROR(L, EINVAL);
}

static int thrift_write(lua_State *L) {
   desc_t *desc = (desc_t *)lua_touserdata(L, 1);
   buffer_t out;
   memset(&out, 0, sizeof(buffer_t));
   thrift_write_rcsv(L, 2, desc, &out, desc->flags, NULL);
   lua_pushlstring(L, (const char *)out.data, out.cb);
   free(out.data);
   return 1;
}

static int thrift_write_tensor(lua_State *L) {
   desc_t *desc = (desc_t *)lua_touserdata(L, 1);
   buffer_t out;
   memset(&out, 0, sizeof(buffer_t));
   thrift_write_rcsv(L, 2, desc, &out, desc->flags, NULL);
   THByteStorage* storage = THByteStorage_newWithSize(out.cb);
   memcpy(storage->data, out.data, out.cb);
   free(out.data);
   THLongStorage *size = THLongStorage_newWithSize(1);
   size->data[0] = storage->size;
   THByteTensor *tensor = THByteTensor_newWithStorage(storage, 0, size, NULL);
   THLongStorage_free(size);
   luaT_pushudata(L, tensor, "torch.ByteTensor");
   return 1;
}

static const luaL_Reg thrift_routines[] = {
   {"codec", thrift_desc},
   {NULL, NULL}
};

static const luaL_Reg thrift_codec_routines[] = {
   {"read", thrift_read},
   {"readTensor", thrift_read_tensor},
   {"write", thrift_write},
   {"writeTensor", thrift_write_tensor},
   {"__gc", thrift_gc},
   {NULL, NULL}
};

DLL_EXPORT int luaopen_libthrift(lua_State *L) {
   luaL_newmetatable(L, "thrift.codec");
   lua_pushstring(L, "__index");
   lua_pushvalue(L, -2);
   lua_settable(L, -3);
   luaT_setfuncs(L, thrift_codec_routines, 0);
   lua_newtable(L);
   luaT_setfuncs(L, thrift_routines, 0);
   return 1;
}
