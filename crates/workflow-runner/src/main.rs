use std::{
    io::{self, Read, Write},
    process::ExitCode,
};
use workflow_engine::{RunnerOutput, RunnerRequest, execute_runner_request_with_events};

fn main() -> ExitCode {
    let mut input = String::new();
    if let Err(error) = io::stdin().read_to_string(&mut input) {
        eprintln!("failed to read runner request: {error}");
        return ExitCode::from(2);
    }
    let request: RunnerRequest = match serde_json::from_str(&input) {
        Ok(request) => request,
        Err(error) => {
            eprintln!("invalid runner request: {error}");
            return ExitCode::from(2);
        }
    };
    let stdout = io::stdout();
    let mut output = stdout.lock();
    let response = execute_runner_request_with_events(request, |event_type, payload| {
        write_message(
            &mut output,
            &RunnerOutput::Event {
                event_type: event_type.into(),
                payload,
            },
        );
    });
    write_message(
        &mut output,
        &RunnerOutput::Result {
            response: Box::new(response.clone()),
        },
    );
    if response
        .run
        .as_ref()
        .is_some_and(|run| run.status == workflow_core::RunStatus::Succeeded)
    {
        ExitCode::SUCCESS
    } else if response
        .run
        .as_ref()
        .is_some_and(|run| run.status == workflow_core::RunStatus::Cancelled)
    {
        ExitCode::from(6)
    } else {
        ExitCode::from(5)
    }
}

fn write_message(output: &mut impl Write, message: &RunnerOutput) {
    if serde_json::to_writer(&mut *output, message).is_ok() {
        let _ = output.write_all(b"\n");
        let _ = output.flush();
    }
}
