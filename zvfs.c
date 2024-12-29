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
} zvfs_context_t;

struct spdk_thread *global_thread = NULL;

static void zvfs_bdev_event_call(enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *event_ctx) {
    SPDK_NOTICELOG("%s --> enter\n", __func__);
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
}

static void zvfs_blob_write_complete(void *arg, int bserrno) {
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

static void zvfs_blob_sync_complete(void *arg, int bserrno) {
    zvfs_context_t *ctx = (zvfs_context_t*)arg;
    SPDK_NOTICELOG("%s --> enter\n", __func__);

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

int main(int argc, char *argv[]) {
    printf("hello spdk\n");

    struct spdk_app_opts opts = {};
    spdk_app_opts_init(&opts, sizeof(struct spdk_app_opts));
    opts.name = "zvfs";
    opts.json_config_file = argv[1];

    zvfs_context_t *ctx = calloc(1, sizeof(zvfs_context_t));
    if (ctx == NULL) {
        return -1;
    }
    memset(ctx, 0, sizeof(zvfs_context_t));

    int res = spdk_app_start(&opts, zvfs_entry, ctx);
    if (res) {
        SPDK_NOTICELOG("ERROR!\n");
    } else {
        SPDK_NOTICELOG("SUCCESS!\n");
    }
    return 0;
}

/*
mathilda@montclaire:~/git/cpp/zvfs$ sudo ./zvfs hello_blob.json 
[sudo] password for mathilda: 
hello spdk
[2024-12-28 16:29:37.937247] Starting SPDK v25.01-pre git sha1 e01cb43b8 / DPDK 24.03.0 initialization...
[2024-12-28 16:29:37.937326] [ DPDK EAL parameters: zvfs --no-shconf -c 0x1 --huge-unlink --no-telemetry --log-level=lib.eal:6 --log-level=lib.cryptodev:5 --log-level=lib.power:5 --log-level=user1:6 --iova-mode=pa --base-virtaddr=0x200000000000 --match-allocations --file-prefix=spdk_pid784586 ]
[2024-12-28 16:29:38.063111] app.c: 919:spdk_app_start: *NOTICE*: Total cores available: 1
[2024-12-28 16:29:38.088073] reactor.c: 995:reactor_run: *NOTICE*: Reactor started on core 0
[2024-12-28 16:29:38.186407] zvfs.c: 118:zvfs_entry: *NOTICE*: zvfs_entry --> enter
[2024-12-28 16:29:38.187529] zvfs.c: 112:zvfs_bs_init_complete: *NOTICE*: zvfs_bs_init_complete --> enter: 512
[2024-12-28 16:29:38.187816] zvfs.c: 103:zvfs_bs_create_complete: *NOTICE*: zvfs_bs_create_complete --> enter
[2024-12-28 16:29:38.187863] zvfs.c:  95:zvfs_blob_open_complete: *NOTICE*: zvfs_blob_open_complete --> enter
[2024-12-28 16:29:38.187875] zvfs.c:  87:zvfs_blob_resize_complete: *NOTICE*: zvfs_blob_resize_complete --> enter
[2024-12-28 16:29:38.187885] zvfs.c:  65:zvfs_blob_sync_complete: *NOTICE*: zvfs_blob_sync_complete --> enter
[2024-12-28 16:29:38.187893] zvfs.c:  52:zvfs_blob_write_complete: *NOTICE*: zvfs_blob_write_complete --> enter
[2024-12-28 16:29:38.187901] zvfs.c:  47:zvfs_blob_read_complete: *NOTICE*: size: 512, buffer: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
Killed
*/