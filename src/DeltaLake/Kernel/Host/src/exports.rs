use upstream_ffi as ffi;

mod c_symbols {
    use upstream_ffi::error::ExternResult;
    use upstream_ffi::handle::Handle;
    use upstream_ffi::transaction::ExclusiveTransaction;
    use upstream_ffi::{KernelStringSlice, OptionalValue, SharedExternEngine, SharedSnapshot};

    #[allow(
        improper_ctypes,
        reason = "Upstream Handle is repr(transparent) over NonNull; its opaque descriptor is never passed by value."
    )]
    extern "C" {
        pub fn with_transaction_id(
            txn: Handle<ExclusiveTransaction>,
            app_id: KernelStringSlice,
            version: i64,
            engine: Handle<SharedExternEngine>,
        ) -> ExternResult<Handle<ExclusiveTransaction>>;

        pub fn get_app_id_version(
            snapshot: Handle<SharedSnapshot>,
            app_id: KernelStringSlice,
            engine: Handle<SharedExternEngine>,
        ) -> ExternResult<OptionalValue<i64>>;
    }
}

/// An immutable code address; the host never dereferences it as data.
#[repr(transparent)]
pub struct RetainedExport(*const ());

/// The immutable table contains only function addresses, not shared mutable data.
unsafe impl Sync for RetainedExport {}

macro_rules! retain_exports {
    ($($function:path),+ $(,)?) => {
        /// Linker-visible roots for all 51 active managed imports, including callbacks.
        #[used]
        #[no_mangle]
        pub static DELTA_DOTNET_KERNEL_EXPORT_ROOTS: [RetainedExport; 51] = [
            $(RetainedExport($function as *const ())),+
        ];
    };
}

retain_exports!(
    ffi::free_bool_slice,
    ffi::get_engine_builder,
    ffi::set_builder_option,
    ffi::set_builder_with_multithreaded_executor,
    ffi::builder_build,
    ffi::free_engine,
    ffi::get_snapshot_builder,
    ffi::get_snapshot_builder_from,
    ffi::snapshot_builder_set_version,
    ffi::snapshot_builder_build,
    ffi::free_snapshot,
    ffi::checkpoint_snapshot,
    ffi::version,
    ffi::free_schema,
    ffi::snapshot_table_root,
    ffi::get_partition_column_count,
    ffi::get_partition_columns,
    ffi::string_slice_next,
    ffi::free_string_slice_data,
    ffi::engine_data::get_raw_arrow_data,
    ffi::engine_data::free_arrow_ffi_data,
    ffi::engine_data::get_engine_data,
    ffi::engine_funcs::read_parquet_file,
    ffi::engine_funcs::read_result_next,
    ffi::engine_funcs::free_read_result_iter,
    ffi::scan::scan,
    ffi::scan::free_scan,
    ffi::scan::scan_logical_schema,
    ffi::scan::scan_physical_schema,
    ffi::scan::scan_metadata_iter_init,
    ffi::scan::scan_metadata_next,
    ffi::scan::free_scan_metadata_iter,
    ffi::scan::visit_scan_metadata,
    ffi::scan::selection_vector_from_dv,
    ffi::scan::get_from_string_map,
    ffi::table_changes::table_changes_from_version,
    ffi::table_changes::table_changes_between_versions,
    ffi::table_changes::table_changes_scan,
    ffi::table_changes::free_table_changes_scan,
    ffi::table_changes::table_changes_scan_execute,
    ffi::table_changes::scan_table_changes_next,
    ffi::table_changes::free_scan_table_changes_iter,
    ffi::transaction::transaction,
    ffi::transaction::free_transaction,
    ffi::transaction::set_data_change,
    ffi::transaction::add_files,
    ffi::transaction::commit,
    ffi::transaction::committed_transaction_version,
    ffi::transaction::free_committed_transaction,
    c_symbols::with_transaction_id,
    c_symbols::get_app_id_version,
);
