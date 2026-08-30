use std::{fmt, future::Future, pin::Pin, sync::Arc};

use tokio_util::sync::CancellationToken;

use crate::JobSchedule;

/// 任务管理器生成的唯一任务 ID。
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskId(u64);

impl TaskId {
    pub(super) fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// 可供管理界面展示的任务信息。
#[derive(Clone, Debug)]
pub struct TaskInfo {
    pub id: TaskId,
    pub name: String,
    pub run_immediately: bool,
    pub status: TaskStatus,
}

/// 任务当前的生命周期状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskStatus {
    Stopped,
    Running,
    Stopping,
}

pub(super) type TaskFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(super) type TaskHandler = Arc<dyn Fn() -> TaskFuture + Send + Sync + 'static>;

pub(super) struct TaskExecution {
    pub(super) schedule: JobSchedule,
    pub(super) handler: TaskHandler,
    pub(super) run_immediately: bool,
    pub(super) cancellation: CancellationToken,
    pub(super) finished: CancellationToken,
}

pub(super) struct ManagedTask {
    id: TaskId,
    name: String,
    run_immediately: bool,
    schedule: JobSchedule,
    handler: TaskHandler,
    status: TaskStatus,
    cancellation: Option<CancellationToken>,
    finished: Option<CancellationToken>,
}

impl ManagedTask {
    pub(super) fn new(
        id: TaskId,
        name: String,
        run_immediately: bool,
        schedule: JobSchedule,
        handler: TaskHandler,
    ) -> Self {
        Self {
            id,
            name,
            run_immediately,
            schedule,
            handler,
            status: TaskStatus::Stopped,
            cancellation: None,
            finished: None,
        }
    }

    pub(super) fn info(&self) -> TaskInfo {
        TaskInfo {
            id: self.id,
            name: self.name.clone(),
            run_immediately: self.run_immediately,
            status: self.status,
        }
    }

    pub(super) fn start(&mut self, parent: &CancellationToken) -> Option<TaskExecution> {
        if self.status != TaskStatus::Stopped {
            return None;
        }

        let execution = TaskExecution {
            schedule: self.schedule.clone(),
            handler: Arc::clone(&self.handler),
            run_immediately: self.run_immediately,
            cancellation: parent.child_token(),
            finished: CancellationToken::new(),
        };

        self.status = TaskStatus::Running;
        self.cancellation = Some(execution.cancellation.clone());
        self.finished = Some(execution.finished.clone());

        Some(execution)
    }

    pub(super) fn request_stop(&mut self) -> Option<CancellationToken> {
        match self.status {
            TaskStatus::Running | TaskStatus::Stopping => {
                self.status = TaskStatus::Stopping;
                if let Some(cancellation) = &self.cancellation {
                    cancellation.cancel();
                }
                self.finished.clone()
            }
            TaskStatus::Stopped => None,
        }
    }

    pub(super) fn mark_stopped(&mut self) {
        self.status = TaskStatus::Stopped;
        self.cancellation = None;
        self.finished = None;
    }
}
