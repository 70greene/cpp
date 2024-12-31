#include <dlfcn.h>
#include <spdk/event.h>
#include <spdk/blob.h>
#include <spdk/bdev.h>
#include <spdk/env.h>
#include <spdk/blob_bdev.h>

#define FILENAME_LENGTH 128

typedef struct zvfs_file_s {
    char filename[FILENAME_LENGTH];
    uint8_t *write_buffer;
    uint8_t *read_buffer;
    struct spdk_blob *blob;

    struct zvfs_filesystem_s *fs;
} zvfs_file_t;

typedef struct zvfs_filesystem_s {
    struct spdk_bs_dev *bsdev;
    struct spdk_blob_store *blobstore;
    struct spdk_io_channel *channel;
    uint64_t io_unit_size;
    struct spdk_thread *thread;
    bool finished;
} zvfs_filesystem_t;

zvfs_filesystem_t *fs_instance = NULL;

static void zvfs_bdev_event_call(enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *event_ctx) {
    SPDK_NOTICELOG("%s --> enter\n", __func__);
}

static const int POLLER_MAX_TIME = 100000;

static bool poller(struct spdk_thread *thread, spdk_msg_fn start_fn, void *ctx, bool *finished) {
    spdk_thread_send_msg(thread, start_fn, ctx);
    int poller_count = 0;

    do {
        spdk_thread_poll(thread, 0, 0);
        poller_count++;
    } while (!(*finished) && poller_count < POLLER_MAX_TIME);

    if (!(*finished) && poller_count >= POLLER_MAX_TIME) {
        return false;
    }
    return true;
}

static void zvfs_file_close(zvfs_file_t *file) {
    if (file->read_buffer) {
        spdk_free(file->read_buffer);
        file->read_buffer = NULL;
    }
    if (file->write_buffer) {
        spdk_free(file->write_buffer);
        file->write_buffer = NULL;
    }
}

static void zvfs_bs_unload_complete(void *arg, int bserrno) {
    zvfs_filesystem_t *fs = (zvfs_filesystem_t *)arg;
    fs->finished = true;
    spdk_app_stop(1);
}

static void zvfs_bs_unload(void *arg) {
    zvfs_filesystem_t *fs = (zvfs_filesystem_t *)arg;
    if (fs->blobstore) {
        if (fs->channel) {
            spdk_bs_free_io_channel(fs->channel);
        }
        // FIXME: should clean blob
        spdk_bs_unload(fs->blobstore, zvfs_bs_unload_complete, fs);
    }
}

static void zvfs_filesystem_unregister(zvfs_filesystem_t *fs) {
    fs->finished = false;
    poller(fs->thread, zvfs_bs_unload, fs, &fs->finished);
}

static void zvfs_blob_read_complete(void *arg, int bserrno) {
    zvfs_file_t *file = (zvfs_file_t *)arg;
    zvfs_filesystem_t *fs = file->fs;
    SPDK_NOTICELOG("size: %ld, buffer: %s\n", fs->io_unit_size, file->read_buffer);
    fs->finished = true;
}

static void zvfs_do_read(void *arg) {
    zvfs_file_t *file = (zvfs_file_t *)arg;
    zvfs_filesystem_t *fs = file->fs;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

    memset(file->read_buffer, '\0', fs->io_unit_size);
    spdk_blob_io_read(file->blob, fs->channel, file->read_buffer, 0, 1, zvfs_blob_read_complete, file);
}


static void zvfs_file_read(zvfs_file_t *file) {
    zvfs_filesystem_t *fs = file->fs;
    fs->finished = false;
    poller(fs->thread, zvfs_do_read, file, &fs->finished);
}

static void zvfs_blob_write_complete(void *arg, int bserrno) {
    zvfs_file_t *file = (zvfs_file_t *)arg;
    zvfs_filesystem_t *fs = file->fs;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

    fs->finished = true;
}

static void zvfs_do_write(void *arg) {
    zvfs_file_t *file = (zvfs_file_t *)arg;
    zvfs_filesystem_t *fs = file->fs;

    // memset(file->write_buffer, '\0', fs->io_unit_size);
    // memset(file->write_buffer, 'A', fs->io_unit_size - 1);

    spdk_blob_io_write(file->blob, fs->channel, file->write_buffer, 0, 1, zvfs_blob_write_complete, file);
}

static void zvfs_file_write(zvfs_file_t *file) {
    zvfs_filesystem_t *fs = file->fs;
    fs->finished = false;
    poller(fs->thread, zvfs_do_write, file, &fs->finished);
}

static void zvfs_blob_sync_complete(void *arg, int bserrno) {
    zvfs_file_t *file = (zvfs_file_t *)arg;
    zvfs_filesystem_t *fs = file->fs;

    fs->finished = true;
    SPDK_NOTICELOG("%s --> %lu enter\n", __func__, fs->io_unit_size);
}

static void zvfs_blob_resize_complete(void *arg, int bserrno) {
    zvfs_file_t *file = (zvfs_file_t *)arg;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

    spdk_blob_sync_md(file->blob, zvfs_blob_sync_complete, file);
}

static void zvfs_blob_open_complete(void *arg, struct spdk_blob *blob, int bserrno) {
    zvfs_file_t *file = (zvfs_file_t *)arg;
    zvfs_filesystem_t *fs = file->fs;
    file->blob = blob;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

    uint64_t freed = spdk_bs_free_cluster_count(fs->blobstore);
    file->write_buffer = spdk_malloc(fs->io_unit_size, 0x1000, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    if (file->write_buffer == NULL) {
        return;
    }

    file->read_buffer = spdk_malloc(fs->io_unit_size, 0x1000, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    if (file->read_buffer == NULL) {
        spdk_free(file->write_buffer);
        return;
    }

    spdk_blob_resize(blob, freed, zvfs_blob_resize_complete, file);
}

static void zvfs_bs_create_complete(void *arg, spdk_blob_id blobid, int bserrno) {
    zvfs_file_t *file = (zvfs_file_t *)arg;
    zvfs_filesystem_t *fs = file->fs;
    
    SPDK_NOTICELOG("%s --> enter\n", __func__);
    spdk_bs_open_blob(fs->blobstore, blobid, zvfs_blob_open_complete, file);
}

static void zvfs_do_create(void *arg) {
    zvfs_file_t *file = (zvfs_file_t *)arg;
    zvfs_filesystem_t *fs = file->fs;
    spdk_bs_create_blob(fs->blobstore, zvfs_bs_create_complete, file);
}

static void zvfs_file_create(zvfs_file_t *file) {
    zvfs_filesystem_t *fs = file->fs;
    fs->finished = false;

    poller(fs->thread, zvfs_do_create, file, &fs->finished);
}

static void zvfs_bs_init_complete(void *arg, struct spdk_blob_store *bs, int bserrno) {
    zvfs_filesystem_t *fs = (zvfs_filesystem_t*)arg;
    fs->blobstore = bs;
    fs->io_unit_size = spdk_bs_get_io_unit_size(bs);
    SPDK_NOTICELOG("%s --> enter: %lu\n", __func__, fs->io_unit_size);

    struct spdk_io_channel *channel = spdk_bs_alloc_io_channel(fs->blobstore);
    if (channel == NULL) {
        zvfs_bs_unload(fs);
        return;
    }
    fs->channel = channel;
    fs->finished = true;
}

static void zvfs_entry(void *arg) {
    zvfs_filesystem_t *fs = (zvfs_filesystem_t*)arg;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

    const char *bdev_name = "Malloc0";
    int rc = spdk_bdev_create_bs_dev_ext(bdev_name, zvfs_bdev_event_call, NULL, &fs->bsdev);
    if (rc != 0) {
        spdk_app_stop(-1);
        return;
    }
    spdk_bs_init(fs->bsdev, NULL, zvfs_bs_init_complete, fs);
}

static void json_app_load_done(int rc, void *ctx) {
    bool *done = ctx;
    *done = true;
}

void
json_config_prepare_ctx(spdk_subsystem_init_fn cb_fn, void *cb_arg, bool stop_on_error, void *json,
			ssize_t json_size, bool initalize_subsystems);

const char* json_str = "{\n"
"  \"subsystems\": [\n"
"    {\n"
"      \"subsystem\": \"bdev\",\n"
"      \"config\": [\n"
"        {\n"
"          \"method\": \"bdev_malloc_create\",\n"
"          \"params\": {\n"
"            \"name\": \"Malloc0\",\n"
"            \"num_blocks\": 32768,\n"
"            \"block_size\": 512\n"
"          }\n"
"        }\n"
"      ]\n"
"    }\n"
"  ]\n"
"}";

static void zvfs_json_load_fn(void *arg) {
    // json_data = spdk_posix_file_load_from_name(json_file, &json_data_size);
    size_t json_data_size = strlen(json_str) + 1;  // 281
    void *json_data = malloc(strlen(json_str) + 1);
    strcpy((char*)json_data, json_str);
    // printf("JSON data: %s\n", (char*)json_data);
    json_config_prepare_ctx(json_app_load_done, arg, true, json_data, json_data_size, true);
}

#define MAX_FD_COUNT 1024
#define DEFAULT_FD_NUM 3

zvfs_file_t *files[MAX_FD_COUNT] = {0};
static unsigned fd_table[MAX_FD_COUNT / 8] = {0};

static int zvfs_get_fd(void) {
    int fd = DEFAULT_FD_NUM;
    for (; fd < MAX_FD_COUNT; fd++) {
        if ((fd_table[fd / 8] & (0x1 << (fd % 8))) == 0) {
            fd_table[fd / 8] = (0x1 << (fd % 8));
            return fd;
        }
    }
    return -1;
}

static void zvfs_set_fd(int fd) {
    if (fd >= MAX_FD_COUNT) {
        return;
    }
    fd_table[fd / 8] &= ~(0x1 << (fd % 8));
}

static int zvfs_filesystem_setup(void) {
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    if (spdk_env_init(&opts) != 0) {
        return -1;
    }

    spdk_log_set_print_level(SPDK_LOG_NOTICE);
    spdk_log_set_level(SPDK_LOG_NOTICE);
    spdk_log_open(NULL);

    zvfs_filesystem_t *fs = calloc(1, sizeof(zvfs_filesystem_t));
    if (!fs) {
        return 0;
    }
    fs_instance = fs;

    spdk_thread_lib_init(NULL, 0);
    fs->thread = spdk_thread_create("global", NULL);
    spdk_set_thread(fs->thread);

    bool done = false;
    poller(fs->thread, zvfs_json_load_fn, &done, &done);

    fs->finished = false;
    poller(fs->thread, zvfs_entry, fs, &fs->finished);

    return 0;
}

static int zvfs_create(const char *pathname, int flags) {
    if (!fs_instance) {
        zvfs_filesystem_setup();
    }

    int fd = zvfs_get_fd();
    zvfs_file_t *file = calloc(1, sizeof(zvfs_file_t));
    if (!file) {
        return -1;
    }

    strcpy(file->filename, pathname);
    files[fd] = file;
    file->fs = fs_instance;
    zvfs_file_create(file);
    return fd;
}

static ssize_t zvfs_write(int fd, const void *buf, size_t count) {
    zvfs_file_t *file = files[fd];
    if (!file) {
        return -1;
    }

    memcpy(file->write_buffer, buf, count);
    zvfs_file_write(file);
    return 0;
}

static ssize_t zvfs_read(int fd, void *buf, size_t count) {
    zvfs_file_t *file = files[fd];
    if (!file) {
        return -1;
    }

    zvfs_file_read(file);
    memcpy(buf, file->read_buffer, count);
    return 0;
}

static int zvfs_close(int fd) {
    zvfs_file_t *file = files[fd];
    if (!file) {
        return 0;
    }

    zvfs_filesystem_unregister(file->fs);

    zvfs_file_close(file);
    zvfs_set_fd(fd);

    free(file);
    files[fd] = NULL;

    return 0;
}

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

int open(const char *pathname, int flags, ...) {
    if (!open_f) {
        open_f = dlsym(RTLD_NEXT, "open");
    }
    dblog("open %s ...\n", pathname);
    return zvfs_create(pathname, flags);
}

ssize_t read(int fd, void *buf, size_t count) {
    if (!read_f) {
        read_f = dlsym(RTLD_NEXT, "read");
    }
    dblog("read ...\n");
    return zvfs_read(fd, buf, count);
}

ssize_t write(int fd, const void *buf, size_t count) {
    if (!write_f) {
        write_f = dlsym(RTLD_NEXT, "write");
    }
    dblog("write ...\n");
    return zvfs_write(fd, buf, count);
}

int close(int fd) {
    if (!close_f) {
        close_f  = dlsym(RTLD_NEXT, "close");
    }
    dblog("close ...\n");
    return zvfs_close(fd);
}

int main(int argc, char *argv[]) {
    printf("hello spdk\n");

    int fd = open("a.txt", O_RDWR | O_CREAT);
    char *wbuffer = "450 mathilda";

    write(fd, wbuffer, strlen(wbuffer));

    char rbuffer[1024] = {0};
    read(fd, rbuffer, 1024);
    printf("rbuffer: %s\n", rbuffer);

    close(fd);
    return 0;
}

/*
root@nvme:/home/mathilda/git/cpp/zvfs# ./zvfs
hello spdk
open a.txt ...
read ...
close ...
EAL: Unable to read from VFIO noiommu file 0 (Success)
read ...
close ...
EAL: FATAL: Cannot use IOVA as 'PA' since physical addresses are not available
EAL: Cannot use IOVA as 'PA' since physical addresses are not available
[2024-12-30 17:13:52.361324] init.c: 733:spdk_env_init: *ERROR*: Failed to initialize DPDK
Segmentation fault (core dumped)
*/
