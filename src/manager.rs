use std::{
    collections::HashMap,
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::{JobDefinition, JobSchedule, TaskOption, job::ScheduleTime, task_option::TaskOptions};

use super::{
    TaskId, TaskInfo, TaskManagerError,
    task::{ManagedTask, TaskExecution, TaskHandler},
};

/// 管理多个独立定时任务的异步任务管理器。
#[derive(Clone, Default)]
pub struct TaskManager {
    inner: Arc<TaskManagerInner>,
}

#[derive(Default)]
struct TaskManagerInner {
    next_id: AtomicU64,
    tasks: RwLock<HashMap<TaskId, ManagedTask>>,
    shutdown: CancellationToken,
}

impl Drop for TaskManagerInner {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl TaskManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// 创建一个任务构建器。应用选项后对构建器 `.await` 即可添加任务。
    pub fn add<D, F, Fut>(&self, handler: F, job_definition: D) -> TaskBuilder<'_, D, F>
    where
        D: JobDefinition,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        TaskBuilder {
            manager: self,
            job_definition,
            handler,
            options: TaskOptions::default(),
        }
    }

    async fn register<D, F, Fut>(
        &self,
        job_definition: D,
        handler: F,
        options: TaskOptions,
    ) -> Result<TaskId, TaskManagerError>
    where
        D: JobDefinition,
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        options.validate()?;
        let mut schedule = job_definition.build()?;
        schedule.apply_interval_from_start(options.interval_from_start);
        let id = TaskId::new(self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1);
        let name = options.task_name.unwrap_or_else(|| format!("task-{id}"));
        let handler: TaskHandler = Arc::new(move || Box::pin(handler()));
        let task = ManagedTask::new(id, name, options.run_immediately, schedule, handler);

        self.inner.tasks.write().await.insert(id, task);
        Ok(id)
    }

    /// 启动指定任务。该方法在后台任务创建完成后立即返回。
    pub async fn start(&self, id: TaskId) -> Result<(), TaskManagerError> {
        let execution = {
            let mut tasks = self.inner.tasks.write().await;
            let task = tasks.get_mut(&id).ok_or(TaskManagerError::NotFound(id))?;
            task.start(&self.inner.shutdown)
                .ok_or(TaskManagerError::AlreadyRunning(id))?
        };

        spawn_task(Arc::downgrade(&self.inner), id, execution);

        Ok(())
    }

    /// 启动所有已停止的任务。正在运行或停止中的任务会被跳过。
    pub async fn start_all(&self) {
        let executions = {
            let mut tasks = self.inner.tasks.write().await;
            let mut executions = Vec::new();

            for (&id, task) in tasks.iter_mut() {
                if let Some(execution) = task.start(&self.inner.shutdown) {
                    executions.push((id, execution));
                }
            }

            executions
        };

        let inner = Arc::downgrade(&self.inner);
        for (id, execution) in executions {
            spawn_task(inner.clone(), id, execution);
        }
    }

    /// 暂停指定任务。任务仍保留在管理器中，之后可以再次启动。
    pub async fn stop(&self, id: TaskId) -> Result<(), TaskManagerError> {
        let finished = {
            let mut tasks = self.inner.tasks.write().await;
            let task = tasks.get_mut(&id).ok_or(TaskManagerError::NotFound(id))?;
            task.request_stop()
        };

        wait_until_finished(finished).await;
        Ok(())
    }

    /// 停止并删除指定任务。
    pub async fn del(&self, id: TaskId) -> Result<(), TaskManagerError> {
        let finished = {
            let mut tasks = self.inner.tasks.write().await;
            let mut task = tasks.remove(&id).ok_or(TaskManagerError::NotFound(id))?;
            task.request_stop()
        };

        wait_until_finished(finished).await;
        Ok(())
    }

    /// 停止并删除全部任务。该方法会等待正在执行的任务结束。
    pub async fn del_all(&self) {
        let finished = {
            let mut tasks = self.inner.tasks.write().await;

            tasks
                .drain()
                .filter_map(|(_, mut task)| task.request_stop())
                .collect::<Vec<_>>()
        };

        for finished in finished {
            finished.cancelled().await;
        }
    }

    /// 获取一个任务的信息。
    pub async fn get(&self, id: TaskId) -> Option<TaskInfo> {
        self.inner
            .tasks
            .read()
            .await
            .get(&id)
            .map(ManagedTask::info)
    }

    /// 获取当前全部任务的信息，并按 ID 排序。
    pub async fn list(&self) -> Vec<TaskInfo> {
        let tasks = self.inner.tasks.read().await;
        let mut infos: Vec<_> = tasks.values().map(ManagedTask::info).collect();
        infos.sort_by_key(|info| info.id);
        infos
    }
}

/// 通过链式 `.option(...)` 配置并注册任务。
pub struct TaskBuilder<'a, D, F> {
    manager: &'a TaskManager,
    job_definition: D,
    handler: F,
    options: TaskOptions,
}

impl<D, F> TaskBuilder<'_, D, F> {
    /// 应用一个任务选项。相同类型的选项以后应用的为准。
    pub fn option(mut self, option: TaskOption) -> Self {
        self.options.apply(option);
        self
    }
}

impl<'a, D, F, Fut> IntoFuture for TaskBuilder<'a, D, F>
where
    D: JobDefinition + 'a,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    type Output = Result<TaskId, TaskManagerError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(
            self.manager
                .register(self.job_definition, self.handler, self.options),
        )
    }
}

fn spawn_task(inner: Weak<TaskManagerInner>, id: TaskId, execution: TaskExecution) {
    let TaskExecution {
        schedule,
        handler,
        run_immediately,
        cancellation,
        finished,
    } = execution;

    let worker = tokio::spawn(run_task_loop(
        schedule,
        handler,
        run_immediately,
        cancellation,
    ));

    // 独立监督 worker，确保 handler panic 时也能恢复任务状态。
    tokio::spawn(async move {
        let _ = worker.await;
        finish_task(inner, id, finished).await;
    });
}

async fn run_task_loop(
    schedule: JobSchedule,
    handler: TaskHandler,
    run_immediately: bool,
    cancellation: CancellationToken,
) {
    let mut schedule = schedule.activate(&ScheduleTime::now());

    if run_immediately {
        schedule.record_immediate_started(tokio::time::Instant::now());
        handler().await;
    }

    loop {
        let Ok(next_run) = schedule.next_run(&ScheduleTime::now()) else {
            break;
        };

        if !wait_until(next_run.deadline, &cancellation).await {
            break;
        }

        schedule.record_started(&next_run, tokio::time::Instant::now());
        handler().await;
    }
}

async fn wait_until(deadline: tokio::time::Instant, cancellation: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => false,
        _ = tokio::time::sleep_until(deadline) => true,
    }
}

async fn finish_task(inner: Weak<TaskManagerInner>, id: TaskId, finished: CancellationToken) {
    let Some(inner) = inner.upgrade() else {
        finished.cancel();
        return;
    };
    let mut tasks = inner.tasks.write().await;

    if let Some(task) = tasks.get_mut(&id) {
        task.mark_stopped();
    }

    // 在释放任务表写锁前发出完成通知，确保 stop 返回后能看到 Stopped 状态。
    finished.cancel();
}

async fn wait_until_finished(finished: Option<CancellationToken>) {
    if let Some(finished) = finished {
        finished.cancelled().await;
    }
}
