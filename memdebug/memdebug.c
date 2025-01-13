#define _GNU_SOURCE 1
#include <dlfcn.h>
#include <malloc.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MIN(x, y) ((x) < (y) ? (x) : (y))

#define CONTEXT_MAX 32

static __thread int g_mem_context;

static const char* context_names[] = {
    "Default",
    "Database",
    "MemTable",
    "MemTableSnapshot",
    "MemTableGc",
    "BackgroundFlush",
    "BackgroundCompaction",
    "TableCache",
    "TableBlock",
    "TransactionManager",
    "Wal",
    "RedoNode",
    "TransactionPrivate",
};

static uint64_t g_ctx_alloc_size[CONTEXT_MAX];
static uint64_t g_ctx_free_size[CONTEXT_MAX];

static inline void *__malloc_impl(size_t size) {
  static void *(*real_malloc)(size_t) = NULL;
  void *ptr;
  size_t alloc_size;
  size_t usable_size;
  void *end;

  if (!real_malloc) {
    real_malloc = dlsym(RTLD_NEXT, "malloc");
  }

  alloc_size = size + 8;

  ptr = real_malloc(alloc_size);
  usable_size = malloc_usable_size(ptr);
  end = ptr + usable_size;

  *(uint32_t *)(end - 8) = (uint32_t)g_mem_context;
  *(uint32_t *)(end - 4) = (uint32_t)size;

  __atomic_fetch_add(&g_ctx_alloc_size[g_mem_context], size, __ATOMIC_RELAXED);

  return ptr;
}

void *malloc(size_t size) { return __malloc_impl(size); }

void *calloc(size_t nelem, size_t elsize) {
  void *p;

  p = __malloc_impl(nelem * elsize);
  if (p == 0)
    return (p);

  bzero(p, nelem * elsize);
  return (p);
}

void *realloc(void *ptr, size_t size) {
  void *new;

  new = __malloc_impl(size);
  if (!new) {
    goto error;
  }

  if (ptr) {
    memcpy(new, ptr, MIN(malloc_usable_size(ptr), size));

    free(ptr);
  }

  return new;
error:
  return NULL;
}

int posix_memalign(void **memptr, size_t alignment, size_t size) {
  static int (*real_memalign)(void **, size_t, size_t) = NULL;
  void *end;
  int ret;

  if (!real_memalign) {
    real_memalign = dlsym(RTLD_NEXT, "posix_memalign");
  }

  ret = real_memalign(memptr, alignment, size + 8);
  if (ret != 0)
    return ret;

  end = *memptr + malloc_usable_size(*memptr);
  *(uint32_t *)(end - 8) = (uint32_t)g_mem_context;
  *(uint32_t *)(end - 4) = (uint32_t)size;

  __atomic_fetch_add(&g_ctx_alloc_size[g_mem_context], size, __ATOMIC_RELAXED);

  return ret;
}

void free(void *ptr) {
  static void (*real_free)(void *) = NULL;
  int ctx;
  size_t size;
  void *end;

  if (!real_free) {
    real_free = dlsym(RTLD_NEXT, "free");
  }

  if (ptr != NULL) {
    end = ptr + malloc_usable_size(ptr);
    ctx = *(uint32_t *)(end - 8);
    size = *(uint32_t *)(end - 4);

    if (ctx >= 0 && ctx < CONTEXT_MAX)
      __atomic_fetch_add(&g_ctx_free_size[ctx], size, __ATOMIC_RELAXED);
    else
      fprintf(stderr, "Bad context %d\n", ctx);
  }

  real_free(ptr);
}

int mdb_get_context(void) { return g_mem_context; }

void mdb_enter_context(int context) { g_mem_context = context; }

void mdb_report_usage(void) {
  for (int i = 0; i < sizeof(context_names)/sizeof(context_names[0]); i++) {
    fprintf(stderr, "Context [%20s]: %6lu MB (+%6lu MB, -%6lu MB)\n",
            context_names[i], (g_ctx_alloc_size[i] - g_ctx_free_size[i]) >> 20, g_ctx_alloc_size[i] >> 20, g_ctx_free_size[i] >> 20);
  }
}
