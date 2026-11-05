use crate::TimerJobError;

/// 设置任务名称。
pub fn task_name(name: impl Into<String>) -> TaskOption {
    TaskOption {
        kind: TaskOptionKind::TaskName(name.into()),
    }
}

/// 设置任务在每次启动时立即执行一次。
pub fn run_immediately() -> TaskOption {
    TaskOption {
        kind: TaskOptionKind::RunImmediately,
    }
}

/// 可以通过 [`TimerJobBuilder::option`](crate::TimerJobBuilder::option) 应用的单个任务选项。
pub struct TaskOption {
    kind: TaskOptionKind,
}

#[derive(Clone, Debug)]
enum TaskOptionKind {
    TaskName(String),
    RunImmediately,
}

#[derive(Default)]
pub(crate) struct TaskOptions {
    pub(crate) task_name: Option<String>,
    pub(crate) run_immediately: bool,
}

impl TaskOptions {
    pub(crate) fn apply(&mut self, option: TaskOption) {
        match option.kind {
            TaskOptionKind::TaskName(name) => self.task_name = Some(name),
            TaskOptionKind::RunImmediately => self.run_immediately = true,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), TimerJobError> {
        if self
            .task_name
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(TimerJobError::EmptyTaskName);
        }

        Ok(())
    }
}
