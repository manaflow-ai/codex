use codex_protocol::protocol::TurnEnvironmentSelection;
use codex_utils_absolute_path::AbsolutePathBuf;

/// A named execution environment and working directory selected for a thread or turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentSelection {
    pub environment_id: String,
    pub cwd: AbsolutePathBuf,
}

impl From<TurnEnvironmentSelection> for EnvironmentSelection {
    fn from(selection: TurnEnvironmentSelection) -> Self {
        Self {
            environment_id: selection.environment_id,
            cwd: selection.cwd,
        }
    }
}
