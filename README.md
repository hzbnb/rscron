# rscron

## rust 定时任务库

先构建 `TimerJob`，再交给 `TimerJobManager` 注册和启动：

```no_run
use std::time::Duration;
use rscron::{TimerJob, TimerJobError, TimerJobManager, job_duration, task_option};

#[tokio::main]
async fn main() -> Result<(), TimerJobError> {
    let manager = TimerJobManager::new();

    let job = || async {
        println!("刷新缓存");
    };

    let timer_job = TimerJob::builder()
        .add_job(job)
        .add_timer(job_duration(Duration::from_secs(10)))
        .option(task_option::task_name("刷新缓存"))
        .option(task_option::run_immediately())
        .build()?;

    let job_id = manager.add(timer_job).await;
    manager.start(job_id).await?;

    tokio::signal::ctrl_c().await.expect("监听退出信号失败");
    manager.del_all().await;
    Ok(())
}
```

- `add_job` 和 `add_timer` 各设置一次，顺序不限；漏配或重复设置在 `build()` 返回错误。
- `build()` 同步校验零间隔、Cron 表达式和任务名称，不会执行任务或分配 ID。
- `option` 可以多次调用，同类选项以后设置的为准。
- Cron 调度使用 `.add_timer(job_cron("0 0 * * *"))`，支持五段和六段表达式。
- `add()` 只注册，返回 `TimerJobId`；`start()` 或 `start_all()` 才启动任务。
- `run_immediately()` 表示每次启动时立即执行一次，默认先等待调度时间。
- 未设置名称时，注册时自动生成 `timer-job-{id}`。

原 `TaskManager`、`TaskId`、`TaskInfo`、`TaskStatus`、`TaskManagerError` 分别改为
`TimerJobManager`、`TimerJobId`、`TimerJobInfo`、`TimerJobStatus`、`TimerJobError`。
原 `manager.add(handler, timer).option(...).await?` 改为先构建，再调用
`manager.add(timer_job).await`；`task_option` 和 `TaskOption` 保留。
