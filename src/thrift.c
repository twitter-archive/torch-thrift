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

typedef struct desc_t {
   struct desc_t *key_ttype;
   struct desc_t *value_ttype;
   struct desc_t *fields;
   uint16_t num_fields;
   uint16_t field_id;
   uint8_t ttype;
   int i64_string;
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
      desc->i64_string = lua_toboolean(L, lua_gettop(L));
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

static int thrift_read_rcsv(lua_State *L, uint8_t ttype, buffer_t *in, int i64_string, desc_t *desc) {
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
         lua_pushinteger(L, i8);
         return 1;
      }
      case TTYPE_DOUBLE: {
         int64_t i64;
         READ(L, &i64, sizeof(i64), in)
         i64 = betoh64(i64);
         double d;
         memcpy(&d, &i64, sizeof(i64));
         lua_pushnumber(L, d);
         return 1;
      }
      case TTYPE_I16: {
         int16_t i16;
         READ(L, &i16, sizeof(i16), in)
         i16 = betoh16(i16);
         double d = i16;
         lua_pushnumber(L, d);
         return 1;
      }
      case TTYPE_I32:
      case TTYPE_ENUM: {
         int32_t i32;
         READ(L, &i32, sizeof(i32), in)
         i32 = betoh32(i32);
         double d = i32;
         lua_pushnumber(L, d);
         return 1;
      }
      case TTYPE_I64: {
         int64_t i64;
         READ(L, &i64, sizeof(i64), in)
         i64 = betoh64(i64);
         if (i64_string) {
            char sz[256];
            snprintf(sz, 256, "%" PRId64 "", i64);
            lua_pushstring(L, sz);
         } else {
            double d = i64;
            if ((int64_t)d != i64) {
               return LUA_HANDLE_ERROR_STR(L, "i64 value out of range");
            }
            lua_pushnumber(L, d);
         }
         return 1;
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
            thrift_read_rcsv(L, vt, in, i64_string, field_desc);
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
            thrift_read_rcsv(L, kt, in, i64_string, desc ? desc->key_ttype : NULL);
            thrift_read_rcsv(L, vt, in, i64_string, desc ? desc->value_ttype : NULL);
            lua_settable(L, -3);
            i32--;
         }
         return 1;
      }
      case TTYPE_SET:
      case TTYPE_LIST: {
         lua_newtable(L);
         uint8_t vt;
         READ(L, &vt, sizeof(vt), in)
         int32_t i32;
         READ(L, &i32, sizeof(i32), in)
         i32 = betoh32(i32);
         for (int32_t i = 1; i <= i32; i++) {
            lua_pushinteger(L, i);
            thrift_read_rcsv(L, vt, in, i64_string, desc ? desc->value_ttype : NULL);
            lua_settable(L, -3);
         }
         return 1;
      }
      default:
         return LUA_HANDLE_ERROR(L, EINVAL);
   }
}

static int thrift_read(lua_State *L) {
   buffer_t in;
   desc_t *desc;

   desc = (desc_t *)lua_touserdata(L, 1);
   in.data = (uint8_t *)lua_tolstring(L, 2, &in.max_cb);
   in.cb = 0;
   return thrift_read_rcsv(L, desc->ttype, &in, desc->i64_string, desc);
}

static int thrift_write_rcsv(lua_State *L, int index, desc_t *desc, buffer_t *out, int i64_string) {
   switch (desc->ttype) {
      case TTYPE_BOOL: {
         uint8_t i8;
         i8 = lua_toboolean(L, index);
         WRITE(L, &i8, sizeof(i8), out)
         return 0;
      }
      case TTYPE_BYTE: {
         double d = lua_tonumber(L, index);
         uint8_t i8 = d;
         if ((double)i8 != d) return LUA_HANDLE_ERROR_STR(L, "byte value out of range during");
         WRITE(L, &i8, sizeof(i8), out)
         return 0;
      }
      case TTYPE_DOUBLE: {
         double d = lua_tonumber(L, index);
         int64_t i64;
         memcpy(&i64, &d, sizeof(i64));
         i64 = htobe64(i64);
         WRITE(L, &i64, sizeof(i64), out)
         return 0;
      }
      case TTYPE_I16: {
         double d = lua_tonumber(L, index);
         int16_t i16 = d;
         if ((double)i16 != d) return LUA_HANDLE_ERROR_STR(L, "i16 value out of range");
         i16 = htobe16(i16);
         WRITE(L, &i16, sizeof(i16), out)
         return 0;
      }
      case TTYPE_I32:
      case TTYPE_ENUM: {
         double d = lua_tonumber(L, index);
         int32_t i32 = d;
         if ((double)i32 != d) return LUA_HANDLE_ERROR_STR(L, "i32 value out of range");
         i32 = htobe32(i32);
         WRITE(L, &i32, sizeof(i32), out)
         return 0;
      }
      case TTYPE_I64: {
         int64_t i64;
         if (i64_string) {
            size_t len;
            const char *str = lua_tolstring(L, index, &len);
            if (str == NULL || len == 0) return LUA_HANDLE_ERROR_STR(L, "i64 can not convert from empty string");
            char *str_end = (char *)str + len;
            i64 = strtoll(str, &str_end, 10);
            if (i64 == 0 && errno == EINVAL) return LUA_HANDLE_ERROR(L, errno);
            if ((i64 == LLONG_MIN || i64 == LLONG_MAX) && errno == ERANGE) return LUA_HANDLE_ERROR(L, errno);
            if (str_end != ((char *)str + len)) return LUA_HANDLE_ERROR_STR(L, "i64 did not consume the entire string");
         } else {
            double d = lua_tonumber(L, index);
            i64 = d;
            if ((double)i64 != d) return LUA_HANDLE_ERROR_STR(L, "i64 value out of range");
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
            WRITE(L, &desc->fields[j].ttype, sizeof(uint8_t), out)
            int16_t i16 = htobe16(desc->fields[j].field_id);
            WRITE(L, &i16, sizeof(i16), out)
            if (desc->fields[j].field_name) {
               lua_pushstring(L, desc->fields[j].field_name);
            } else {
               lua_pushinteger(L, desc->fields[j].field_id);
            }
            lua_rawget(L, index);
            thrift_write_rcsv(L, lua_gettop(L), &desc->fields[j], out, i64_string);
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
            thrift_write_rcsv(L, top + 1, desc->key_ttype, out, i64_string);
            thrift_write_rcsv(L, top + 2, desc->value_ttype, out, i64_string);
            lua_pop(L, 1);
         }
         return 0;
      }
      case TTYPE_SET:
      case TTYPE_LIST: {
         WRITE(L, &desc->value_ttype->ttype, sizeof(uint8_t), out)
         size_t len = lua_objlen(L, index);
         int32_t i32 = htobe32(len);
         WRITE(L, &i32, sizeof(i32), out)
         int top = lua_gettop(L);
         for (int32_t i = 1; i <= (int32_t)len; i++) {
            lua_rawgeti(L, index, i);
            thrift_write_rcsv(L, top + 1, desc->value_ttype, out, i64_string);
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
   thrift_write_rcsv(L, 2, desc, &out, desc->i64_string);
   lua_pushlstring(L, (const char *)out.data, out.cb);
   free(out.data);
   return 1;
}

static const luaL_reg thrift_routines[] = {
   {"codec", thrift_desc},
   {NULL, NULL}
};

static const luaL_reg thrift_codec_routines[] = {
   {"read", thrift_read},
   {"write", thrift_write},
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
