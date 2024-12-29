#include <spdk/event.h>
#include <spdk/blob.h>
#include <spdk/bdev.h>
#include <spdk/env.h>
#include <spdk/blob_bdev.h>

typedef struct zvfs_context_s {
    struct spdk_bs_dev *bsdev;
    struct spdk_blob_store *blobstore;
    struct spdk_blob *blob;
    struct spdk_io_channel *channel;

    uint8_t *write_buffer;
    uint8_t *read_buffer;
    uint64_t io_unit_size;

    bool finished;
} zvfs_context_t;

struct spdk_thread *global_thread = NULL;

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

static void zvfs_bs_unload_complete(void *arg, int bserrno) {
    spdk_app_stop(1);
}

static void zvfs_bs_unload(zvfs_context_t *ctx) {
    if (ctx->blobstore) {
        if (ctx->channel) {
            spdk_bs_free_io_channel(ctx->channel);
        }
        if (ctx->read_buffer) {
            spdk_free(ctx->read_buffer);
            ctx->read_buffer = NULL;
        }
        if (ctx->write_buffer) {
            spdk_free(ctx->write_buffer);
            ctx->write_buffer = NULL;
        }
        spdk_bs_unload(ctx->blobstore, zvfs_bs_unload_complete, ctx);
    }
}

static void zvfs_blob_read_complete(void *arg, int bserrno) {
    zvfs_context_t *ctx = (zvfs_context_t*)arg;
    SPDK_NOTICELOG("size: %ld, buffer: %s\n", ctx->io_unit_size, ctx->read_buffer);
    ctx->finished = true;
}

static void zvfs_do_read(void *arg) {
    zvfs_context_t *ctx = (zvfs_context_t*)arg;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

    ctx->read_buffer = spdk_malloc(ctx->io_unit_size, 0x1000, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    if (ctx->read_buffer == NULL) {
        zvfs_bs_unload(ctx);
        return;
    }
    memset(ctx->read_buffer, '\0', ctx->io_unit_size);
    spdk_blob_io_read(ctx->blob, ctx->channel, ctx->read_buffer, 0, 1, zvfs_blob_read_complete, ctx);
}


static void zvfs_file_read(zvfs_context_t *ctx) {
    ctx->finished = false;
    poller(global_thread, zvfs_do_read, ctx, &ctx->finished);
}

static void zvfs_blob_write_complete(void *arg, int bserrno) {
    zvfs_context_t *ctx = (zvfs_context_t*)arg;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

    ctx->finished = true;
}

static void zvfs_do_write(void *arg) {
    zvfs_context_t *ctx = (zvfs_context_t*)arg;

    ctx->write_buffer = spdk_malloc(ctx->io_unit_size, 0x1000, NULL, SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    if (ctx->write_buffer == NULL) {
        zvfs_bs_unload(ctx);
        return;
    }
    memset(ctx->write_buffer, '\0', ctx->io_unit_size);
    memset(ctx->write_buffer, 'A', ctx->io_unit_size - 1);

    struct spdk_io_channel *channel = spdk_bs_alloc_io_channel(ctx->blobstore);
    if (channel == NULL) {
        zvfs_bs_unload(ctx);
        return;
    }

    ctx->channel = channel;
    spdk_blob_io_write(ctx->blob, ctx->channel, ctx->write_buffer, 0, 1, zvfs_blob_write_complete, ctx);
}

static void zvfs_file_write(zvfs_context_t *ctx) {
    ctx->finished = false;
    poller(global_thread, zvfs_do_write, ctx, &ctx->finished);
}


static void zvfs_blob_sync_complete(void *arg, int bserrno) {
    zvfs_context_t *ctx = (zvfs_context_t*)arg;

    ctx->finished = true;
    SPDK_NOTICELOG("%s --> %lu enter\n", __func__, ctx->io_unit_size);
}

static void zvfs_blob_resize_complete(void *arg, int bserrno) {
    zvfs_context_t *ctx = (zvfs_context_t*)arg;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

    spdk_blob_sync_md(ctx->blob, zvfs_blob_sync_complete, ctx);
}

static void zvfs_blob_open_complete(void *arg, struct spdk_blob *blob, int bserrno) {
    zvfs_context_t *ctx = (zvfs_context_t*)arg;
    ctx->blob = blob;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

    uint64_t freed = spdk_bs_free_cluster_count(ctx->blobstore);
    spdk_blob_resize(blob, freed, zvfs_blob_resize_complete, ctx);
}

static void zvfs_bs_create_complete(void *arg, spdk_blob_id blobid, int bserrno) {
    zvfs_context_t *ctx = (zvfs_context_t*)arg;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

    spdk_bs_open_blob(ctx->blobstore, blobid, zvfs_blob_open_complete, ctx);
}

static void zvfs_bs_init_complete(void *arg, struct spdk_blob_store *bs, int bserrno) {
    zvfs_context_t *ctx = (zvfs_context_t*)arg;
    ctx->blobstore = bs;
    ctx->io_unit_size = spdk_bs_get_io_unit_size(bs);
    SPDK_NOTICELOG("%s --> enter: %lu\n", __func__, ctx->io_unit_size);
    spdk_bs_create_blob(bs, zvfs_bs_create_complete, ctx);
}

static void zvfs_entry(void *arg) {
    zvfs_context_t *ctx = (zvfs_context_t*)arg;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

    const char *bdev_name = "Malloc0";
    int rc = spdk_bdev_create_bs_dev_ext(bdev_name, zvfs_bdev_event_call, NULL, &ctx->bsdev);
    if (rc != 0) {
        spdk_app_stop(-1);
        return;
    }
    spdk_bs_init(ctx->bsdev, NULL, zvfs_bs_init_complete, ctx);
}

static const char *json_file = "/home/mathilda/git/zvfs/hello_blob.json";

static void json_app_load_done(int rc, void *ctx) {
    bool *done = ctx;
    *done = true;
}

static void zvfs_json_load_fn(void *arg) {
    spdk_subsystem_init_from_json_config(json_file, SPDK_DEFAULT_RPC_ADDR, json_app_load_done, arg, true);
}

int main(int argc, char *argv[]) {
    printf("hello spdk\n");

    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    if (spdk_env_init(&opts) != 0) {
        return -1;
    }
       
    spdk_log_set_print_level(SPDK_LOG_NOTICE);
    spdk_log_set_level(SPDK_LOG_NOTICE);
    spdk_log_open(NULL);

    spdk_thread_lib_init(NULL, 0);
    global_thread = spdk_thread_create("global", NULL);
    spdk_set_thread(global_thread);

    bool done = false;
    poller(global_thread, zvfs_json_load_fn, &done, &done);

    zvfs_context_t *ctx = calloc(1, sizeof(zvfs_context_t));
    if (ctx == NULL) {
        return -1;
    }
    memset(ctx, 0, sizeof(zvfs_context_t));

    ctx->finished = false;
    poller(global_thread, zvfs_entry, ctx, &ctx->finished);

    SPDK_NOTICELOG("--> ctx->io_unit_size: %ld\n", ctx->io_unit_size);
    
    zvfs_file_write(ctx);
    zvfs_file_read(ctx);

    return 0;
}

/*
mathilda@montclaire:~/git/cpp/zvfs$ sudo ./zvfs
[sudo] password for mathilda: 
hello spdk
[2024-12-28 16:58:14.688265] json_config.c: 659:spdk_subsystem_init_from_json_config: *WARNING*: spdk_subsystem_init_from_json_config: deprecated feature spdk_subsystem_init_from_json_config is deprecated to be removed in v24.09
[2024-12-28 16:58:14.800163] zvfs.c: 163:zvfs_entry: *NOTICE*: zvfs_entry --> enter
[2024-12-28 16:58:14.800955] zvfs.c: 157:zvfs_bs_init_complete: *NOTICE*: zvfs_bs_init_complete --> enter: 512
[2024-12-28 16:58:14.801014] zvfs.c: 148:zvfs_bs_create_complete: *NOTICE*: zvfs_bs_create_complete --> enter
[2024-12-28 16:58:14.801024] zvfs.c: 140:zvfs_blob_open_complete: *NOTICE*: zvfs_blob_open_complete --> enter
[2024-12-28 16:58:14.801031] zvfs.c: 132:zvfs_blob_resize_complete: *NOTICE*: zvfs_blob_resize_complete --> enter
[2024-12-28 16:58:14.801038] zvfs.c: 127:zvfs_blob_sync_complete: *NOTICE*: zvfs_blob_sync_complete --> 512 enter
[2024-12-28 16:58:14.801043] zvfs.c: 214:main: *NOTICE*: --> ctx->io_unit_size: 512
[2024-12-28 16:58:14.801049] zvfs.c:  91:zvfs_blob_write_complete: *NOTICE*: zvfs_blob_write_complete --> enter
[2024-12-28 16:58:14.801053] zvfs.c:  72:zvfs_do_read: *NOTICE*: zvfs_do_read --> enter
[2024-12-28 16:58:14.801058] zvfs.c:  66:zvfs_blob_read_complete: *NOTICE*: size: 512, buffer: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
*/