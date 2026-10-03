use crate::api::schema::{AgentLifecycleAction, AgentLifecycleParams, Method, Request};

pub(super) fn run(args: &[String]) -> std::io::Result<i32> {
    let (target, action) = match args {
        [target, action] if action == "snapshot" => (target, AgentLifecycleAction::Snapshot),
        [target, action, text] if action == "submit" =>
            (target, AgentLifecycleAction::Submit { text: text.clone() }),
        [target, action, generation] if action == "abort" =>
            (target, AgentLifecycleAction::Abort { generation: generation.clone() }),
        _ => {
            eprintln!("usage: herdr agent lifecycle <target> snapshot|submit <text>|abort <generation>");
            return Ok(2);
        }
    };
    let response = super::send_request(&Request {
        id: "cli:agent:lifecycle".into(),
        method: Method::AgentLifecycle(AgentLifecycleParams { target: target.clone(), action }),
    })?;
    super::print_response(&response)
}
