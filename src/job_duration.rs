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
        SchedulerDuration::new(self.duration).map(JobSchedule::Duration)
    }
}

/// 负责计算固定间隔任务的下一次执行时间。
#[derive(Clone, Copy, Debug)]
pub struct SchedulerDuration {
    duration: Duration,
}

impl SchedulerDuration {
    /// 创建一个固定间隔调度器。间隔必须大于零。
    pub fn new(duration: Duration) -> Result<Self, TaskManagerError> {
        if duration.is_zero() {
            return Err(TaskManagerError::InvalidDuration(duration));
        }

        Ok(Self { duration })
    }

    /// 从固定计划时间轴上选择第一个不早于当前时间的运行点。
    ///
    /// 和 gocron 一样，任务错过一个或多个 tick 时直接前进到未来的计划点，
    /// 而不是把实际延迟变成新的时间基准。
    pub(crate) fn next_scheduled_run(&self, scheduled_from: Instant, now: Instant) -> NextRun {
        let elapsed = now.saturating_duration_since(scheduled_from);

        // schedule_from 本身是上一次计划时间；尚未经过任何时间时，下一次运行
        // 应当发生在一个完整间隔之后。
        if elapsed.is_zero() {
            return NextRun::new(scheduled_from + self.duration);
        }

        // 用除法取余一次定位到固定时间轴上的下一个 tick，避免因错过大量 tick
        // 而逐次累加。若 now 恰好落在 tick 上，则允许立即运行。
        let interval_nanos = self.duration.as_nanos();
        let remainder_nanos = elapsed.as_nanos() % interval_nanos;

        if remainder_nanos == 0 {
            return NextRun::new(now);
        }

        let until_next_nanos = interval_nanos - remainder_nanos;
        let until_next = Duration::new(
            (until_next_nanos / 1_000_000_000) as u64,
            (until_next_nanos % 1_000_000_000) as u32,
        );
        NextRun::new(now + until_next)
    }
}
