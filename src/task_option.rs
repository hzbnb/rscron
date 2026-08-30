use crate::TaskManagerError;

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

/// 让固定间隔任务从上一次实际开始执行的时间计算下一次运行时间。
///
/// 默认情况下，[`job_duration`](crate::job_duration) 会沿用固定的计划时间轴，
/// 避免任务因偶发的调度延迟而逐次漂移。应用此选项后，下一次运行时间改为
/// “上一次实际开始时间 + interval”。该选项对 cron 任务没有影响。
pub fn interval_from_start() -> TaskOption {
    TaskOption {
        kind: TaskOptionKind::IntervalFromStart,
    }
}

/// 可以通过 [`TaskBuilder::option`](crate::TaskBuilder::option) 应用的单个任务选项。
pub struct TaskOption {
    kind: TaskOptionKind,
}

#[derive(Clone, Debug)]
enum TaskOptionKind {
    TaskName(String),
    RunImmediately,
    IntervalFromStart,
}

#[derive(Default)]
pub(crate) struct TaskOptions {
    pub(crate) task_name: Option<String>,
    pub(crate) run_immediately: bool,
    pub(crate) interval_from_start: bool,
}

impl TaskOptions {
    pub(crate) fn apply(&mut self, option: TaskOption) {
        match option.kind {
            TaskOptionKind::TaskName(name) => self.task_name = Some(name),
            TaskOptionKind::RunImmediately => self.run_immediately = true,
            TaskOptionKind::IntervalFromStart => self.interval_from_start = true,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), TaskManagerError> {
        if self
            .task_name
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(TaskManagerError::EmptyTaskName);
        }

        Ok(())
    }
}
