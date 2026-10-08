//! 运维显式运行的只读容量命令，不监听 HTTP，不建立后台采样循环。
//! Explicit one-shot read-only capacity CLI, without HTTP or background scheduling.

use std::{env, process::ExitCode, time::Duration};
use tiangz_dbproxy_storage::capacity::{CapacityOptions, sample_capacity};

const USAGE: &str = "dbproxy_capacity --postgres-url-env NAME [--schema public] [--timeout-ms 10000] [--include-server-age]";

/// 严格拒绝未知/重复参数；连接串仅从指定环境变量读取。
/// Rejects unknown/duplicate arguments; reads credentials only from a named environment variable.
fn parse_args(args: &[String]) -> Result<(String, CapacityOptions), &'static str> {
    let mut name = None;
    let mut schema_seen = false;
    let mut timeout_seen = false;
    let mut options = CapacityOptions::default();
    let mut cursor = 0;
    while cursor < args.len() {
        let flag = args[cursor].as_str();
        if flag == "--include-server-age" && !options.include_server_age {
            options.include_server_age = true;
            cursor += 1;
            continue;
        }
        let value = args.get(cursor + 1).ok_or(USAGE)?;
        match flag {
            "--postgres-url-env"
                if name.is_none() && !value.is_empty() && !value.contains(['=', '\0']) =>
            {
                name = Some(value.clone())
            }
            "--schema"
                if !schema_seen
                    && !value.is_empty()
                    && value.len() <= 63
                    && !value.contains('\0') =>
            {
                options.schema = value.clone();
                schema_seen = true;
            }
            "--timeout-ms" if !timeout_seen => {
                let millis = value
                    .parse::<u64>()
                    .ok()
                    .filter(|n| (1..=60_000).contains(n))
                    .ok_or(USAGE)?;
                options.timeout = Duration::from_millis(millis);
                timeout_seen = true;
            }
            _ => return Err(USAGE),
        }
        cursor += 2;
    }
    Ok((name.ok_or(USAGE)?, options))
}

/// 错误走 Display 而非带原始连接信息的 Debug；失败时不输出半份 JSON。
/// Prints sanitized errors, and never emits a partial JSON snapshot on failure.
#[tokio::main]
async fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.as_slice() == ["--help"] {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    match run(&args).await {
        Ok(report) => {
            println!("{report}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

/// 解析连接串的原始错误也不能将凭据写进日志。
/// Raw connection-string parse errors are never included in logs.
async fn run(args: &[String]) -> Result<String, String> {
    let (name, options) = parse_args(args).map_err(str::to_owned)?;
    let url =
        env::var(name).map_err(|_| "PostgreSQL environment variable is missing or invalid")?;
    let config = url
        .parse()
        .map_err(|_| "invalid PostgreSQL connection configuration")?;
    let snapshot = sample_capacity(&config, &options)
        .await
        .map_err(|e| e.to_string())?;
    serde_json::to_string_pretty(&snapshot).map_err(|_| "capacity JSON serialization failed".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_require_explicit_credentials_and_bounded_options() {
        let parse =
            |args: &[&str]| parse_args(&args.iter().map(|s| (*s).into()).collect::<Vec<_>>());
        for args in [
            &[][..],
            &["--postgres-url", "secret"],
            &["--postgres-url-env", "PG", "--timeout-ms", "0"],
            &["--postgres-url-env", "PG", "--timeout-ms", "60001"],
            &[
                "--postgres-url-env",
                "PG",
                "--schema",
                "public",
                "--schema",
                "other",
            ],
            &[
                "--postgres-url-env",
                "PG",
                "--include-server-age",
                "--include-server-age",
            ],
        ] {
            let error = parse(args).unwrap_err();
            assert_eq!(error, USAGE);
            assert!(!error.contains("secret"));
        }
        let (name, options) = parse(&[
            "--postgres-url-env",
            "MY_PG",
            "--schema",
            "quoted\"schema",
            "--include-server-age",
            "--timeout-ms",
            "2345",
        ])
        .unwrap();
        assert_eq!(name, "MY_PG");
        assert_eq!(options.schema, "quoted\"schema");
        assert!(options.include_server_age);
        assert_eq!(options.timeout, Duration::from_millis(2345));
    }
}
