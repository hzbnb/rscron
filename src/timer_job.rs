use std::{fmt, future::Future, pin::Pin, sync::Arc};

use tokio_util::sync::CancellationToken;

use crate::{JobSchedule, TimerJobBuilder};

/// 已通过配置校验、尚未注册的定时任务。
///
/// 通过 [`TimerJob::builder`] 配置，并调用 [`TimerJobBuilder::build`] 完成构建。
/// 注册到 [`TimerJobManager`](crate::TimerJobManager) 后才会分配任务 ID；
/// 任务需要显式启动。
#[must_use = "构建后的定时任务需要注册到 TimerJobManager"]
pub struct TimerJob {
    pub(crate) name: Option<String>,
    pub(crate) schedule: JobSchedule,
    pub(crate) handler: JobHandler,
    pub(crate) run_immediately: bool,
}

impl TimerJob {
    pub fn builder() -> TimerJobBuilder {
        TimerJobBuilder::default()
    }
}

/// 任务管理器生成的唯一任务 ID。
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TimerJobId(u64);

impl TimerJobId {
    pub(super) fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for TimerJobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// 可供管理界面展示的任务信息。
#[derive(Clone, Debug)]
pub struct TimerJobInfo {
    pub id: TimerJobId,
    pub name: String,
    pub run_immediately: bool,
    pub status: TimerJobStatus,
}

/// 任务当前的生命周期状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimerJobStatus {
    Stopped,
    Running,
    Stopping,
}

pub(super) type JobFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(super) type JobHandler = Arc<dyn Fn() -> JobFuture + Send + Sync + 'static>;

pub(super) struct TimerJobExecution {
    pub(super) schedule: JobSchedule,
    pub(super) handler: JobHandler,
    pub(super) run_immediately: bool,
    pub(super) cancellation: CancellationToken,
    pub(super) finished: CancellationToken,
}

pub(super) struct ManagedTimerJob {
    id: TimerJobId,
    name: String,
    run_immediately: bool,
    schedule: JobSchedule,
    handler: JobHandler,
    status: TimerJobStatus,
    cancellation: Option<CancellationToken>,
    finished: Option<CancellationToken>,
}

impl ManagedTimerJob {
    pub(super) fn new(
        id: TimerJobId,
        name: String,
        run_immediately: bool,
        schedule: JobSchedule,
        handler: JobHandler,
    ) -> Self {
        Self {
            id,
            name,
            run_immediately,
            schedule,
            handler,
            status: TimerJobStatus::Stopped,
            cancellation: None,
            finished: None,
        }
    }

    pub(super) fn info(&self) -> TimerJobInfo {
        TimerJobInfo {
            id: self.id,
            name: self.name.clone(),
            run_immediately: self.run_immediately,
            status: self.status,
        }
    }

    pub(super) fn start(&mut self, parent: &CancellationToken) -> Option<TimerJobExecution> {
        if self.status != TimerJobStatus::Stopped {
            return None;
        }

        let execution = TimerJobExecution {
            schedule: self.schedule.clone(),
            handler: Arc::clone(&self.handler),
            run_immediately: self.run_immediately,
            cancellation: parent.child_token(),
            finished: CancellationToken::new(),
        };

        self.status = TimerJobStatus::Running;
        self.cancellation = Some(execution.cancellation.clone());
        self.finished = Some(execution.finished.clone());

        Some(execution)
    }

    pub(super) fn request_stop(&mut self) -> Option<CancellationToken> {
        match self.status {
            TimerJobStatus::Running | TimerJobStatus::Stopping => {
                self.status = TimerJobStatus::Stopping;
                if let Some(cancellation) = &self.cancellation {
                    cancellation.cancel();
                }
                self.finished.clone()
            }
            TimerJobStatus::Stopped => None,
        }
    }

    pub(super) fn mark_stopped(&mut self) {
        self.status = TimerJobStatus::Stopped;
        self.cancellation = None;
        self.finished = None;
    }
}
