use crate::{
    path::Path,
    pool::Pooled,
    protocol::resolver::{Auth, Referral},
    utils,
};
use anyhow::Result;
use log::debug;
use serde_json::from_str;
use std::{
    convert::AsRef,
    convert::Into,
    env,
    fs::read_to_string,
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
};

/// The on disk format, encoded as JSON
mod file {
    use crate::chars::Chars;
    use std::net::SocketAddr;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) enum Auth {
        Anonymous,
        Krb5(String),
        Local(String),
        Tls(String),
    }

    impl Into<crate::protocol::resolver::Auth> for Auth {
        fn into(self) -> crate::protocol::resolver::Auth {
            use crate::protocol::resolver::Auth as A;
            match self {
                Self::Anonymous => A::Anonymous,
                Self::Krb5(spn) => A::Krb5 { spn: Chars::from(spn) },
                Self::Local(path) => A::Local { path: Chars::from(path) },
                Self::Tls(name) => A::Tls { name: Chars::from(name) },
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct Config {
        pub(super) base: String,
        pub(super) addrs: Vec<(SocketAddr, Auth)>,
        #[serde(default)]
        pub(super) tls_ca_certs: Option<String>,
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base: Path,
    pub addrs: Vec<(SocketAddr, Auth)>,
    pub tls_ca_certs: Option<String>,
}

impl Config {
    pub fn parse(s: &str) -> Result<Config> {
        let cfg: file::Config = from_str(s)?;
        if cfg.addrs.is_empty() {
            bail!("you must specify at least one address");
        }
        for (addr, auth) in &cfg.addrs {
            use file::Auth as FAuth;
            utils::check_addr::<()>(addr.ip(), &[])?;
            match auth {
                FAuth::Anonymous | FAuth::Krb5(_) => (),
                FAuth::Tls { .. } => if cfg.tls_ca_certs.is_none() {
                    bail!("tls auth requires tls_ca_certs path to be set")
                }
                FAuth::Local(_) => {
                    if !addr.ip().is_loopback() {
                        bail!("local auth is not allowed for remote servers")
                    }
                }
            }
        }
        if !cfg.addrs.iter().all(|(a, _)| a.ip().is_loopback())
            && !cfg.addrs.iter().all(|(a, _)| !a.ip().is_loopback())
        {
            bail!("can't mix loopback addrs with non loopback addrs")
        }
        Ok(Config {
            base: Path::from(cfg.base),
            addrs: cfg.addrs.into_iter().map(|(s, a)| (s, a.into())).collect(),
            tls_ca_certs: cfg.tls_ca_certs,
        })
    }

    /// Load the cluster config from the specified file.
    pub fn load<P: AsRef<FsPath>>(file: P) -> Result<Config> {
        Config::parse(&read_to_string(file)?)
    }

    pub fn to_referral(self) -> Referral {
        Referral { path: self.base, ttl: None, addrs: Pooled::orphan(self.addrs) }
    }

    /// This will try in order,
    ///
    /// * $NETIDX_CFG
    /// * ${dirs::config_dir}/netidx/client.json
    /// * ${dirs::home_dir}/.config/netidx/client.json
    /// * C:\netidx\client.json on windows
    /// * /etc/netidx/client.json on unix
    ///
    /// It will load the first file that exists, if that file fails to
    /// load then Err will be returned.
    pub fn load_default() -> Result<Config> {
        if let Some(cfg) = env::var_os("NETIDX_CFG") {
            let cfg = PathBuf::from(cfg);
            if cfg.is_file() {
                debug!("loading {}", cfg.to_string_lossy());
                return Config::load(cfg);
            }
        }
        if let Some(mut cfg) = dirs::config_dir() {
            cfg.push("netidx");
            cfg.push("client.json");
            if cfg.is_file() {
                debug!("loading {}", cfg.to_string_lossy());
                return Config::load(cfg);
            }
        }
        if let Some(mut home) = dirs::home_dir() {
            home.push(".config");
            home.push("netidx");
            home.push("client.json");
            if home.is_file() {
                debug!("loading {}", home.to_string_lossy());
                return Config::load(home);
            }
        }
        let dir = if cfg!(windows) {
            PathBuf::from("C:\\netidx\\client.json")
        } else {
            PathBuf::from("/etc/netidx/client.json")
        };
        if dir.is_file() {
            debug!("loading {}", dir.to_string_lossy());
            return Config::load(dir);
        }
        bail!("no default config file was found")
    }
}
