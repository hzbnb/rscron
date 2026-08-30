use std::time::Duration;

use rscron::{TaskManager, TaskStatus, job_duration, task_option};
use tokio::{sync::mpsc, time::timeout};

#[tokio::test]
async fn duration_task_runs_and_can_be_deleted() {
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
