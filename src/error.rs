use std::{error::Error, fmt, time::Duration};

use croner::errors::CronError;

use super::TimerJobId;

/// 定时任务构建和管理操作可能产生的错误。
#[derive(Debug)]
pub enum TimerJobError {
    MissingJob,
    MissingTimer,
    DuplicateJob,
    DuplicateTimer,
    InvalidPattern(CronError),
    InvalidDuration(Duration),
    EmptyTaskName,
    NotFound(TimerJobId),
    AlreadyRunning(TimerJobId),
}

impl fmt::Display for TimerJobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingJob => write!(formatter, "缺少任务执行逻辑，请调用 add_job"),
            Self::MissingTimer => write!(formatter, "缺少任务调度方式，请调用 add_timer"),
            Self::DuplicateJob => write!(formatter, "add_job 只能调用一次"),
            Self::DuplicateTimer => write!(formatter, "add_timer 只能调用一次"),
            Self::InvalidPattern(error) => write!(formatter, "无效的 cron 表达式：{error}"),
            Self::InvalidDuration(duration) => {
                write!(formatter, "无效的任务间隔 {duration:?}：间隔必须大于 0")
            }
            Self::EmptyTaskName => write!(formatter, "任务名称不能为空"),
            Self::NotFound(id) => write!(formatter, "没有找到任务 {id}"),
            Self::AlreadyRunning(id) => write!(formatter, "任务 {id} 正在运行或停止中"),
        }
    }
}

impl Error for TimerJobError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPattern(error) => Some(error),
            Self::MissingJob
            | Self::MissingTimer
            | Self::DuplicateJob
            | Self::DuplicateTimer
            | Self::InvalidDuration(_)
            | Self::EmptyTaskName
            | Self::NotFound(_)
            | Self::AlreadyRunning(_) => None,
        }
    }
}

impl From<CronError> for TimerJobError {
    fn from(error: CronError) -> Self {
        Self::InvalidPattern(error)
    }
}
