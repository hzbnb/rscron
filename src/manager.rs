use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::{JobSchedule, TimerJob, job::ScheduleTime};

use super::{
    TimerJobError, TimerJobId, TimerJobInfo,
    timer_job::{JobHandler, ManagedTimerJob, TimerJobExecution},
};

/// 管理多个独立定时任务的异步任务管理器。
#[derive(Clone, Default)]
pub struct TimerJobManager {
    inner: Arc<TimerJobManagerInner>,
}

#[derive(Default)]
struct TimerJobManagerInner {
    next_id: AtomicU64,
    tasks: RwLock<HashMap<TimerJobId, ManagedTimerJob>>,
    shutdown: CancellationToken,
}

impl Drop for TimerJobManagerInner {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl TimerJobManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册一个已构建的定时任务并返回 ID。任务需要显式启动。
    pub async fn add(&self, job: TimerJob) -> TimerJobId {
        let TimerJob {
            name,
            schedule,
            handler,
            run_immediately,
        } = job;
        let id = TimerJobId::new(self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1);
        let name = name.unwrap_or_else(|| format!("timer-job-{id}"));
        let task = ManagedTimerJob::new(id, name, run_immediately, schedule, handler);

        self.inner.tasks.write().await.insert(id, task);
        id
    }

    /// 启动指定任务。该方法在后台任务创建完成后立即返回。
    pub async fn start(&self, id: TimerJobId) -> Result<(), TimerJobError> {
        let execution = {
            let mut tasks = self.inner.tasks.write().await;
            let task = tasks.get_mut(&id).ok_or(TimerJobError::NotFound(id))?;
            task.start(&self.inner.shutdown)
                .ok_or(TimerJobError::AlreadyRunning(id))?
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
    pub async fn stop(&self, id: TimerJobId) -> Result<(), TimerJobError> {
        let finished = {
            let mut tasks = self.inner.tasks.write().await;
            let task = tasks.get_mut(&id).ok_or(TimerJobError::NotFound(id))?;
            task.request_stop()
        };

        wait_until_finished(finished).await;
        Ok(())
    }

    /// 停止并删除指定任务。
    pub async fn del(&self, id: TimerJobId) -> Result<(), TimerJobError> {
        let finished = {
            let mut tasks = self.inner.tasks.write().await;
            let mut task = tasks.remove(&id).ok_or(TimerJobError::NotFound(id))?;
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
    pub async fn get(&self, id: TimerJobId) -> Option<TimerJobInfo> {
        self.inner
            .tasks
            .read()
            .await
            .get(&id)
            .map(ManagedTimerJob::info)
    }

    /// 获取当前全部任务的信息，并按 ID 排序。
    pub async fn list(&self) -> Vec<TimerJobInfo> {
        let tasks = self.inner.tasks.read().await;
        let mut infos: Vec<_> = tasks.values().map(ManagedTimerJob::info).collect();
        infos.sort_by_key(|info| info.id);
        infos
    }
}

fn spawn_task(inner: Weak<TimerJobManagerInner>, id: TimerJobId, execution: TimerJobExecution) {
    let TimerJobExecution {
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
    handler: JobHandler,
    run_immediately: bool,
    cancellation: CancellationToken,
) {
    let mut schedule = schedule.activate(&ScheduleTime::now());

    if run_immediately {
        handler().await;
    }

    loop {
        let Ok(next_run) = schedule.next_run(&ScheduleTime::now()) else {
            break;
        };

        if !wait_until(next_run.deadline, &cancellation).await {
            break;
        }

        schedule.record_started(&next_run);
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

async fn finish_task(
    inner: Weak<TimerJobManagerInner>,
    id: TimerJobId,
    finished: CancellationToken,
) {
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
