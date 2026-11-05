use std::{future::Future, sync::Arc};

use crate::{
    JobDefinition, JobSchedule, TaskOption, TimerJob, TimerJobError, task_option::TaskOptions,
    timer_job::JobHandler,
};

type TimerFactory = Box<dyn FnOnce() -> Result<JobSchedule, TimerJobError> + Send + 'static>;

/// 配置定时任务，最后通过 [`Self::build`] 统一校验并生成 [`TimerJob`]。
///
/// `add_job` 和 `add_timer` 各需调用一次，顺序不限。漏配或重复设置均在
/// `build()` 时返回错误；`option` 可以多次调用，同类选项以后设置的为准。
#[derive(Default)]
#[must_use = "请调用 build() 完成定时任务构建"]
pub struct TimerJobBuilder {
    handler: Option<JobHandler>,
    timer: Option<TimerFactory>,
    options: TaskOptions,
    error: Option<TimerJobError>,
}

impl TimerJobBuilder {
    /// 设置可以重复执行的异步任务。重复调用会使最终的 `build()` 返回错误。
    pub fn add_job<F, Fut>(mut self, job: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if self.handler.is_some() {
            self.error.get_or_insert(TimerJobError::DuplicateJob);
        } else {
            self.handler = Some(Arc::new(move || Box::pin(job())));
        }
        self
    }

    /// 设置调度定义。定义的校验推迟到 `build()`，重复调用同样在构建时返回错误。
    pub fn add_timer<D>(mut self, timer: D) -> Self
    where
        D: JobDefinition + Send + 'static,
    {
        if self.timer.is_some() {
            self.error.get_or_insert(TimerJobError::DuplicateTimer);
        } else {
            self.timer = Some(Box::new(move || timer.build()));
        }
        self
    }

    /// 应用一个可选配置。同类选项以后设置的为准。
    pub fn option(mut self, option: TaskOption) -> Self {
        self.options.apply(option);
        self
    }

    /// 校验必填项、重复设置和配置值，并返回尚未注册的定时任务。
    ///
    /// 该方法不会调用任务执行逻辑，也不会启动任务。
    pub fn build(self) -> Result<TimerJob, TimerJobError> {
        if let Some(error) = self.error {
            return Err(error);
        }

        let handler = self.handler.ok_or(TimerJobError::MissingJob)?;
        let timer = self.timer.ok_or(TimerJobError::MissingTimer)?;
        self.options.validate()?;
        let schedule = timer()?;

        Ok(TimerJob {
            name: self.options.task_name,
            schedule,
            handler,
            run_immediately: self.options.run_immediately,
        })
    }
}
