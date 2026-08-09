//! TiangZ DBProxy 的公共门面包。
//! Public facade for the standalone TiangZ DBProxy workspace.
//!
//! 真实服务按 core、protocol、client、server 与 storage 分层；根包只提供公共SDK入口，
//! 不在这里添加游戏业务或第二套持久化语义。
//! The service is split into core, protocol, client, server, and storage layers. This facade
//! only exposes public SDK packages and must not grow game-specific persistence rules.

pub use tiangz_dbproxy_client as client;
pub use tiangz_dbproxy_core as core;
pub use tiangz_dbproxy_protocol as protocol;
pub use tiangz_dbproxy_storage as storage;
