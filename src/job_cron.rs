use croner::{
    Cron,
    errors::CronError,
    parser::{CronParser, Seconds},
};

use crate::{
    TaskManagerError,
    job::{JobDefinition, JobSchedule, NextRun, ScheduleTime},
};

/// 创建 cron 任务定义。
pub fn job_cron(pattern: impl Into<String>) -> JobCron {
    JobCron {
        pattern: pattern.into(),
    }
}

/// cron 任务定义。
#[derive(Clone, Debug)]
pub struct JobCron {
    pattern: String,
}

impl JobDefinition for JobCron {
    fn build(self) -> Result<JobSchedule, TaskManagerError> {
        CronScheduler::new(&self.pattern)
            .map(Box::new)
            .map(JobSchedule::Cron)
            .map_err(TaskManagerError::from)
    }
}

/// 负责解析 cron 表达式并计算下一次执行时间。
#[derive(Clone, Debug)]
pub struct CronScheduler {
    cron: Cron,
}

impl CronScheduler {
    /// 创建一个调度器，同时接受五段和六段 cron 表达式。
    pub fn new(pattern: &str) -> Result<Self, CronError> {
        CronParser::builder()
            .seconds(Seconds::Optional)
            .build()
            .parse(pattern)
            .map(|cron| Self { cron })
    }

    pub(crate) fn next_run(&self, now: &ScheduleTime) -> Result<NextRun, TaskManagerError> {
        let scheduled_at = self.cron.find_next_occurrence(&now.wall_time, false)?;
        let delay = scheduled_at.duration_since(&now.wall_time).unsigned_abs();

        Ok(NextRun::new(now.monotonic + delay))
    }
}
