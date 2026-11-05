use std::{sync::Arc, time::Duration};

use rscron::{TimerJob, TimerJobManager, TimerJobStatus, job_duration, task_option};
use tokio::{
    sync::{Mutex, mpsc},
    time::timeout,
};

#[tokio::test]
async fn duration_task_run() {
    let manager = TimerJobManager::new();
    let (executed_tx, mut executed_rx) = mpsc::unbounded_channel();

    let job = move || {
        let executed_tx = executed_tx.clone();

        async move {
            let service_name = "demo-service";
            let task_name = "refresh-cache";
            let executed_at = jiff::Zoned::now();

            println!(
                "服务 {service_name} 执行任务 {task_name}：{} 时间戳：{}",
                executed_at.strftime("%F %T %:Q"),
                executed_at.timestamp().as_millisecond()
            );

            executed_tx.send(()).expect("记录任务执行结果失败");
        }
    };

    let timer_job = TimerJob::builder()
        .add_job(job)
        .add_timer(job_duration(Duration::from_millis(20)))
        .option(task_option::task_name("刷新缓存"))
        .option(task_option::run_immediately())
        .build()
        .expect("构建定时任务失败");
    let task_id = manager.add(timer_job).await;

    let task = manager.get(task_id).await.expect("找不到刚添加的任务");
    assert_eq!(task.name, "刷新缓存");
    assert_eq!(task.status, TimerJobStatus::Stopped);
    assert!(task.run_immediately);
    assert!(matches!(
        executed_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    manager.start_all().await;

    let task = manager.get(task_id).await.expect("找不到已启动的任务");
    assert_eq!(task.status, TimerJobStatus::Running);

    // 第一次来自 run_immediately，第二次来自固定间隔调度。
    timeout(Duration::from_secs(1), executed_rx.recv())
        .await
        .expect("立即执行任务超时")
        .expect("任务执行通道意外关闭");
    timeout(Duration::from_secs(1), executed_rx.recv())
        .await
        .expect("定时执行任务超时")
        .expect("任务执行通道意外关闭");

    manager.del_all().await;

    assert!(manager.list().await.is_empty());
}

/// 手动运行测试：保持任务运行，直到收到 Ctrl+C 或 SIGTERM。
///
/// 运行命令：
/// cargo test --test manager_test duration_task_runs_until_ctrl_c -- --ignored --nocapture
#[tokio::test]
#[ignore = "需要手动按 Ctrl+C 结束"]
async fn duration_task_runs_until_ctrl_c() {
    let manager = TimerJobManager::new();
    let test = Arc::new(Mutex::new(0));

    let job = move || {
        let test = Arc::clone(&test);

        async move {
            let mut test = test.lock().await;
            manual_run_task(&mut test).await;
        }
    };

    let timer_job = TimerJob::builder()
        .add_job(job)
        .add_timer(job_duration(Duration::from_secs(10)))
        .option(task_option::task_name("刷新缓存"))
        .option(task_option::run_immediately())
        .build()
        .expect("构建定时任务失败");
    let task_id = manager.add(timer_job).await;

    manager.start_all().await;
    println!("任务 {task_id} 已启动，按 Ctrl+C 或发送 SIGTERM 可停止服务。\n");

    wait_for_exit().await;
    shutdown(&manager).await;

    assert!(manager.list().await.is_empty());
}

async fn manual_run_task(test: &mut i32) {
    *test += 1;
    let service_name = "demo-service";
    let task_name = "refresh-cache";
    let executed_at = jiff::Zoned::now();

    println!(
        "服务 {service_name} 执行任务 {task_name}：{} 时间戳：{} 执行次数：{test}\n",
        executed_at.strftime("%F %T %:Q"),
        executed_at.timestamp().as_millisecond()
    );
}

#[tokio::test]
async fn duration_task_preserves_mutable_state() {
    let manager = TimerJobManager::new();
    let test = Arc::new(Mutex::new(0));
    let task_test = Arc::clone(&test);
    let (executed_tx, mut executed_rx) = mpsc::unbounded_channel();

    let job = move || {
        let test = Arc::clone(&task_test);
        let executed_tx = executed_tx.clone();

        async move {
            let mut test = test.lock().await;
            manual_run_task(&mut test).await;
            executed_tx.send(*test).expect("记录任务执行次数失败");
        }
    };

    let timer_job = TimerJob::builder()
        .add_job(job)
        .add_timer(job_duration(Duration::from_millis(20)))
        .option(task_option::run_immediately())
        .build()
        .expect("构建定时任务失败");
    manager.add(timer_job).await;

    manager.start_all().await;

    for expected in 1..=3 {
        let count = timeout(Duration::from_secs(1), executed_rx.recv())
            .await
            .expect("等待任务执行超时")
            .expect("任务执行通道意外关闭");
        assert_eq!(count, expected);
    }

    manager.del_all().await;
    assert!(*test.lock().await >= 3);
    assert!(manager.list().await.is_empty());
}

async fn wait_for_exit() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate()).expect("监听 SIGTERM 失败");

        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.expect("监听 Ctrl+C 失败");
            }
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.expect("监听 Ctrl+C 失败");
}

async fn shutdown(manager: &TimerJobManager) {
    println!("收到退出信号，正在停止所有任务……");

    manager.del_all().await;

    println!("所有任务已停止并删除，服务已退出");
}
