use std::time::Duration;

use rscron::{TaskManager, TaskStatus, job_duration, task_option};
use tokio::{sync::mpsc, time::timeout};

#[tokio::test]
async fn duration_task_run() {
    let manager = TaskManager::new();
    let (executed_tx, mut executed_rx) = mpsc::unbounded_channel();

    let task_id = manager
        .add(
            move || {
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
            },
            job_duration(Duration::from_millis(20)),
        )
        .option(task_option::task_name("刷新缓存"))
        .option(task_option::run_immediately())
        .await
        .expect("添加定时任务失败");

    let task = manager.get(task_id).await.expect("找不到刚添加的任务");
    assert_eq!(task.name, "刷新缓存");
    assert_eq!(task.status, TaskStatus::Stopped);
    assert!(task.run_immediately);

    manager.start_all().await;

    let task = manager.get(task_id).await.expect("找不到已启动的任务");
    assert_eq!(task.status, TaskStatus::Running);

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
    let manager = TaskManager::new();

    let task_id = manager
        .add(manual_run_task, job_duration(Duration::from_secs(10)))
        .option(task_option::task_name("刷新缓存"))
        .option(task_option::run_immediately())
        .await
        .expect("添加定时任务失败");

    manager.start_all().await;
    println!("任务 {task_id} 已启动，按 Ctrl+C 或发送 SIGTERM 可停止服务。\n");

    wait_for_exit().await;
    shutdown(&manager).await;

    assert!(manager.list().await.is_empty());
}

async fn manual_run_task() {
    let service_name = "demo-service";
    let task_name = "refresh-cache";
    let executed_at = jiff::Zoned::now();

    println!(
        "服务 {service_name} 执行任务 {task_name}：{} 时间戳：{}\n",
        executed_at.strftime("%F %T %:Q"),
        executed_at.timestamp().as_millisecond()
    );
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

async fn shutdown(manager: &TaskManager) {
    println!("收到退出信号，正在停止所有任务……");

    manager.del_all().await;

    println!("所有任务已停止并删除，服务已退出");
}
