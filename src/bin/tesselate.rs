use clap::Parser;
use std::process::ExitCode;
use tesselate::{
    cli::{execute, prepare_state_dir, print_json, Cli},
    coordination::application::Application,
    persistence::Database,
};

fn main() -> ExitCode {
    let cli = Cli::parse();
    let state_dir = match prepare_state_dir(&cli.state_dir) {
        Ok(path) => path,
        Err(error) => return report(error),
    };
    let db = match Database::open(state_dir.join("coordinator.sqlite3")) {
        Ok(db) => db,
        Err(error) => return report(error),
    };
    let application = match Application::new(db, state_dir.join("worktrees")) {
        Ok(application) => application,
        Err(error) => return report(error),
    };
    match execute(
        &application,
        &state_dir,
        cli.credential_file.as_deref(),
        cli.command,
    ) {
        Ok(value) => match print_json(&value) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => report(error),
        },
        Err(error) => report(error),
    }
}

fn report(error: impl std::fmt::Display) -> ExitCode {
    eprintln!("tesselate: {error}");
    ExitCode::from(2)
}
