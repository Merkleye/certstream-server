use std::env;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// User-Agent sent on every outbound HTTP request unless `ct_log.user_agent`
/// overrides it. Built from the package version at compile time so the CT log
/// fetch client and the TLS-pinned Apple catalog client can never drift apart.
pub const DEFAULT_USER_AGENT: &str = concat!("certstream-server-rust/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub validate_config: bool,
    pub dry_run: bool,
    pub export_metrics: bool,
    pub show_version: bool,
    pub show_help: bool,
}

impl CliArgs {
    pub fn parse() -> Self {
        Self::parse_from(env::args())
    }

    /// Parses from an arbitrary argument sequence (`args()[0]` is the
    /// program name, same convention as `env::args()`, though every flag
    /// check below simply scans for the flag anywhere in it). Split out
    /// from `parse()` so tests can supply args directly instead of the
    /// real process's.
    fn parse_from(args: impl IntoIterator<Item = String>) -> Self {
        let args: Vec<String> = args.into_iter().collect();

        Self {
            validate_config: args.iter().any(|a| a == "--validate-config"),
            dry_run: args.iter().any(|a| a == "--dry-run"),
            export_metrics: args.iter().any(|a| a == "--export-metrics"),
            show_version: args.iter().any(|a| a == "--version" || a == "-V"),
            show_help: args.iter().any(|a| a == "--help" || a == "-h"),
        }
    }

    pub fn print_help() {
        println!("certstream-server-rust {}", VERSION);
        println!();
        println!("High-performance Certificate Transparency log streaming server");
        println!();
        println!("USAGE:");
        println!("    certstream-server-rust [OPTIONS]");
        println!();
        println!("OPTIONS:");
        println!("    --validate-config    Validate configuration and exit");
        println!("    --dry-run            Start server without connecting to CT logs");
        println!("    --export-metrics     Export current metrics and exit (output is empty on cold start)");
        println!("    -V, --version        Print version information");
        println!("    -h, --help           Print help information");
        println!();
        println!("ENVIRONMENT VARIABLES:");
        println!("    CERTSTREAM_CONFIG              Path to config file");
        println!("    CERTSTREAM_HOST                Server host (default: 0.0.0.0)");
        println!("    CERTSTREAM_PORT                Server port (default: 8080)");
        println!("    CERTSTREAM_LOG_LEVEL           Log level (default: info)");
        println!("    CERTSTREAM_BUFFER_SIZE         Broadcast buffer size (default: 1000)");
        println!("    CERTSTREAM_USER_AGENT          Override the outbound HTTP User-Agent");
        println!();
        println!("For more information, see: https://github.com/reloading01/certstream-server-rust");
    }

    pub fn print_version() {
        println!("certstream-server-rust {}", VERSION);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(flags: &[&str]) -> Vec<String> {
        std::iter::once("certstream-server-rust".to_string())
            .chain(flags.iter().map(|s| s.to_string()))
            .collect()
    }

    #[test]
    fn no_flags_are_all_false() {
        let parsed = CliArgs::parse_from(args(&[]));
        assert!(!parsed.validate_config);
        assert!(!parsed.dry_run);
        assert!(!parsed.export_metrics);
        assert!(!parsed.show_version);
        assert!(!parsed.show_help);
    }

    #[test]
    fn validate_config_flag() {
        assert!(CliArgs::parse_from(args(&["--validate-config"])).validate_config);
    }

    #[test]
    fn dry_run_flag() {
        assert!(CliArgs::parse_from(args(&["--dry-run"])).dry_run);
    }

    #[test]
    fn export_metrics_flag() {
        assert!(CliArgs::parse_from(args(&["--export-metrics"])).export_metrics);
    }

    #[test]
    fn version_flag_long_and_short() {
        assert!(CliArgs::parse_from(args(&["--version"])).show_version);
        assert!(CliArgs::parse_from(args(&["-V"])).show_version);
    }

    #[test]
    fn help_flag_long_and_short() {
        assert!(CliArgs::parse_from(args(&["--help"])).show_help);
        assert!(CliArgs::parse_from(args(&["-h"])).show_help);
    }

    #[test]
    fn multiple_flags_combine() {
        let parsed = CliArgs::parse_from(args(&["--dry-run", "--validate-config"]));
        assert!(parsed.dry_run);
        assert!(parsed.validate_config);
        assert!(!parsed.export_metrics);
    }

    #[test]
    fn unknown_args_are_ignored() {
        let parsed = CliArgs::parse_from(args(&["--not-a-real-flag", "positional"]));
        assert!(!parsed.validate_config);
        assert!(!parsed.dry_run);
        assert!(!parsed.export_metrics);
        assert!(!parsed.show_version);
        assert!(!parsed.show_help);
    }

    #[test]
    fn parse_reads_the_real_process_args() {
        // Doesn't control the test binary's own argv, so this only checks
        // parse() actually delegates to parse_from() rather than
        // duplicating its logic (e.g. it shouldn't panic, and its result
        // should be independently reproducible from env::args() directly).
        let parsed = CliArgs::parse();
        let expected = CliArgs::parse_from(env::args());
        assert_eq!(parsed.validate_config, expected.validate_config);
        assert_eq!(parsed.dry_run, expected.dry_run);
        assert_eq!(parsed.export_metrics, expected.export_metrics);
        assert_eq!(parsed.show_version, expected.show_version);
        assert_eq!(parsed.show_help, expected.show_help);
    }

    #[test]
    fn version_constant_matches_cargo_package_version() {
        assert_eq!(VERSION, env!("CARGO_PKG_VERSION"));
        assert!(DEFAULT_USER_AGENT.starts_with("certstream-server-rust/"));
        assert!(DEFAULT_USER_AGENT.ends_with(VERSION));
    }

    #[test]
    fn print_help_does_not_panic() {
        CliArgs::print_help();
    }

    #[test]
    fn print_version_does_not_panic() {
        CliArgs::print_version();
    }
}
