use anyhow::{Context, Result};
use futures::{prelude::*, select_biased};
use netidx_archive::recorder::{Config, Recorder};
use std::{
    fs,
    path::{self, PathBuf},
};
use tokio::signal::ctrl_c;

pub(crate) async fn run(config: Option<PathBuf>, example: bool) -> Result<()> {
    if example {
        println!("{}", Config::example());
        Ok(())
    } else {
        let config = config.context("config is required")?;
        let config = Config::load(&config).await.context("failed to read config file")?;
        if !path::Path::exists(&config.archive_directory) {
            fs::create_dir_all(&config.archive_directory)
                .context("creating archive directory")?;
        } else if !path::Path::is_dir(&config.archive_directory) {
            panic!("archive_directory must be a directory")
        }
        let recorder = Recorder::start(config);
        loop {
            select_biased! {
                _ = recorder.wait_shutdown().fuse() => break,
                r = ctrl_c().fuse() => if let Ok(()) = r {
                    recorder.shutdown()
                },
            }
        }
        Ok(())
    }
}
