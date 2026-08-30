use jiff::Zoned;
use tokio::time::Instant;

use crate::{SchedulerCron, SchedulerDuration, TaskManagerError};

/// 创建任务时使用的调度定义。
///
/// [`TaskManager::add`](crate::TaskManager::add) 只依赖这个抽象，因此可以用同一个
/// 方法添加 cron 任务和固定间隔任务。
pub trait JobDefinition {
    /// 校验定义并创建一个可以被任务重复启动的调度配置。
    fn build(self) -> Result<JobSchedule, TaskManagerError>;
}

/// 任务管理器内置的调度类型。
#[derive(Clone, Debug)]
pub enum JobSchedule {
    Cron(Box<SchedulerCron>),
    Duration(SchedulerDuration),
}

impl JobSchedule {
    /// 为一次任务启动创建独立的调度运行态。
    ///
    /// 定义本身可以被重复启动；计划时间等可变状态只存在于本次运行中。
    pub(crate) fn activate(self, started_at: &ScheduleTime) -> ActiveSchedule {
        match self {
            Self::Cron(scheduler) => ActiveSchedule::Cron(scheduler),
            Self::Duration(scheduler) => ActiveSchedule::Duration {
                scheduler,
                schedule_from: started_at.monotonic,
            },
        }
    }
}

/// 单次任务启动期间持有的调度策略及其可变时间状态。
///
/// 运行循环只依赖这个统一接口，不需要分别实现 Cron 和 Duration 的等待、
/// 取消及任务调用流程。
pub(crate) enum ActiveSchedule {
    Cron(Box<SchedulerCron>),
    Duration {
        scheduler: SchedulerDuration,
        schedule_from: Instant,
    },
}

impl ActiveSchedule {
    /// 按当前调度策略计算下一次运行时间。
    pub(crate) fn next_run(&self, now: &ScheduleTime) -> Result<NextRun, TaskManagerError> {
        match self {
            Self::Cron(scheduler) => scheduler.next_run(now),
            Self::Duration {
                scheduler,
                schedule_from,
            } => Ok(scheduler.next_scheduled_run(*schedule_from, now.monotonic)),
        }
    }

    /// 记录一次按计划触发的计划时间。
    pub(crate) fn record_started(&mut self, next_run: &NextRun) {
        if let Self::Duration { schedule_from, .. } = self {
            *schedule_from = next_run.deadline;
        }
    }
}

/// 同一次调度计算使用的墙上时间和单调时间快照。
pub(crate) struct ScheduleTime {
    pub(crate) wall_time: Zoned,
    pub(crate) monotonic: Instant,
}

impl ScheduleTime {
    pub(crate) fn now() -> Self {
        Self {
            wall_time: Zoned::now(),
            monotonic: Instant::now(),
        }
    }
}

/// 调度策略计算出的下一次运行时间。
pub(crate) struct NextRun {
    pub(crate) deadline: Instant,
}

impl NextRun {
    pub(crate) fn new(deadline: Instant) -> Self {
        Self { deadline }
    }
}
