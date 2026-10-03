use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentLifecycleParams {
    pub target: String,
    #[serde(flatten)]
    pub action: AgentLifecycleAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum AgentLifecycleAction {
    Snapshot,
    Submit { text: String },
    Abort { generation: String },
}
