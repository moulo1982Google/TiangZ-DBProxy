//! TiangZ DBProxy 的公共门面包。
//! Public facade for the standalone TiangZ DBProxy workspace.
//!
//! 真实服务仍按 `dbproxy-core` 与 `dbproxy-storage` 分层演进；根包只提供统一入口，
//! 不在这里添加游戏业务或第二套持久化语义。
//! The service continues to evolve in `dbproxy-core` and `dbproxy-storage`; this facade
//! only provides one package entry point and must not grow game-specific persistence rules.

pub use tiangz_dbproxy_core as core;
pub use tiangz_dbproxy_storage as storage;
