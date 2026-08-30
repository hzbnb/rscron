use std::time::Duration;

use tokio::time::Instant;

use crate::{
    TaskManagerError,
    job::{JobDefinition, JobSchedule, NextRun},
};

/// 创建固定间隔任务定义。
///
/// 第一次执行会发生在一个完整间隔之后。默认沿着首次启动时确定的计划时间轴
/// 推进；某次执行即使晚了几毫秒，也不会把这几毫秒累积到后续计划中。
pub fn job_duration(duration: Duration) -> JobDuration {
    JobDuration { duration }
}

/// 固定间隔任务定义。
#[derive(Clone, Copy, Debug)]
pub struct JobDuration {
    duration: Duration,
}

impl JobDefinition for JobDuration {
    fn build(self) -> Result<JobSchedule, TaskManagerError> {
        DurationScheduler::new(self.duration).map(JobSchedule::Duration)
    }
}

/// 负责计算固定间隔任务的下一次执行时间。
#[derive(Clone, Copy, Debug)]
pub struct DurationScheduler {
    duration: Duration,
    interval_from_start: bool,
}

impl DurationScheduler {
    /// 创建一个固定间隔调度器。间隔必须大于零。
    pub fn new(duration: Duration) -> Result<Self, TaskManagerError> {
        if duration.is_zero() {
            return Err(TaskManagerError::InvalidDuration(duration));
        }

        Ok(Self {
            duration,
            interval_from_start: false,
        })
    }

    pub(crate) fn next_run_from(&self, from: Instant) -> NextRun {
        NextRun::new(from + self.duration)
    }

    /// 从固定计划时间轴上选择第一个不早于当前时间的运行点。
    ///
    /// 和 gocron 一样，任务错过一个或多个 tick 时直接前进到未来的计划点，
    /// 而不是把实际延迟变成新的时间基准。
    pub(crate) fn next_scheduled_run(&self, scheduled_from: Instant, now: Instant) -> NextRun {
        let mut next = scheduled_from + self.duration;
        while next < now {
            next += self.duration;
        }
        NextRun::new(next)
    }

    pub(crate) fn set_interval_from_start(&mut self, enabled: bool) {
        self.interval_from_start = enabled;
    }

    pub(crate) fn interval_from_start(&self) -> bool {
        self.interval_from_start
    }
}
