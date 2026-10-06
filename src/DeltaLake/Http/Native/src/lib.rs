//! Internal, same-image HTTP header acquisition. No Rust allocation crosses the C ABI.
//! Hosts must root static callbacks for process lifetime and retain providers in stores.

mod broker;
mod headers;
mod protocol;
mod transport;

pub use broker::{acquire, complete, register, unregister, Provider};
pub use protocol::{ByteSlice, HeaderCallbacks, HeaderError, ABI_VERSION};
pub use transport::{StockConnector, StockHeaderConnector, StorageEndpoint};
