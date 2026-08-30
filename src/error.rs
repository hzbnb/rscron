use std::{error::Error, fmt, time::Duration};

use croner::errors::CronError;

use super::TaskId;

/// 任务管理操作可能产生的错误。
#[derive(Debug)]
pub enum TaskManagerError {
    InvalidPattern(CronError),
    InvalidDuration(Duration),
    EmptyTaskName,
    NotFound(TaskId),
    AlreadyRunning(TaskId),
}

impl fmt::Display for TaskManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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

impl Error for TaskManagerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPattern(error) => Some(error),
            Self::InvalidDuration(_)
            | Self::EmptyTaskName
            | Self::NotFound(_)
            | Self::AlreadyRunning(_) => None,
        }
    }
}

impl From<CronError> for TaskManagerError {
    fn from(error: CronError) -> Self {
        Self::InvalidPattern(error)
    }
}
