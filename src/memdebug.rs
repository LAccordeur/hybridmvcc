cfg_if::cfg_if! {
    if #[cfg(feature = "memdebug")] {

        #[link(name = "memdebug", kind = "dylib")]
        extern "C" {
            fn mdb_enter_context(ctx: i32);
            fn mdb_get_context() -> i32;
            fn mdb_report_usage();
        }

        fn get_context() -> i32 {
            unsafe {
                mdb_get_context()
            }
        }

        fn enter_context(ctx: i32) {
            unsafe {
                mdb_enter_context(ctx);
            }
        }

        pub fn report_usage() {
            unsafe {
                mdb_report_usage();
            }
        }

    } else {
        fn enter_context(_ctx: i32) {}

        fn get_context() -> i32 {
            0
        }

        pub fn report_usage() {}
    }
}

pub enum MemContext {
    Default,
    Database,
    MemTable,
    MemTableSnapshot,
    MemTableGc,
    BackgroundFlush,
    BackgroundCompaction,
    TableCache,
    TableBlock,
    TransactionManager,
    Wal,
    RedoNode,
    TransactionPrivate,
}

pub struct MemContextGuard(i32);

impl MemContextGuard {
    pub fn new(ctx: MemContext) -> Self {
        let old_ctx = get_context();

        enter_context(ctx as i32);
        Self(old_ctx)
    }

    pub fn with_context<F, R>(ctx: MemContext, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let old_ctx = get_context();
        enter_context(ctx as i32);
        let ret = f();
        enter_context(old_ctx as i32);
        ret
    }
}

impl Drop for MemContextGuard {
    fn drop(&mut self) {
        enter_context(self.0);
    }
}
