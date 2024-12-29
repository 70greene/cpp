#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>

 // gcc -fPIC -shared -o syscall.so syscall.c
 // /etc/init.d/mysql restart
 // LD_PRELOAD=/home/mathilda/git/zvfs/syscall.so /etc/init.d/mysql restart
 // LD_PRELOAD=/home/mathilda/git/spdk/build/lib/libspdk_zvfs.so /etc/init.d/mysql restart

#define DEBUG_ENABLE 1

#if DEBUG_ENABLE
#define dblog(fmt, ...) printf(fmt, ##__VA_ARGS__)
#else
#define dblog(fmt, ...)
#endif

typedef int (*open_t)(const char *pathname, int flags);
open_t open_f = NULL;

typedef int (*read_t)(int fd, void *buf, size_t conut);
read_t read_f = NULL;

typedef ssize_t (*write_t)(int fd, const void *buf, size_t count);
write_t write_f = NULL;

typedef int (*close_t)(int fd);
close_t close_f = NULL;

int open(const char *pathname, int flags) {
    if (!open_f) {
        open_f = dlsym(RTLD_NEXT, "open");
    }
    dblog("open %s ...\n", pathname);
    return open_f(pathname, flags);
}

ssize_t read(int fd, void *buf, size_t count) {
    if (!read_f) {
        read_f = dlsym(RTLD_NEXT, "read");
    }
    dblog("read ...\n");
    return read_f(fd, buf, count);
}

ssize_t write(int fd, const void *buf, size_t count) {
    if (!write_f) {
        write_f = dlsym(RTLD_NEXT, "write");
    }
    dblog("write ...\n");
    return write_f(fd, buf, count);
}

int close(int fd) {
    if (!close_f) {
        close_f  = dlsym(RTLD_NEXT, "close");
    }
    dblog("close ...\n");
    return close_f(fd);
}