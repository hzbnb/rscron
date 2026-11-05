use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use rscron::{
    JobDefinition, JobSchedule, TimerJob, TimerJobError, TimerJobManager, TimerJobStatus, job_cron,
    job_duration, task_option,
};
use tokio::{sync::mpsc, time::timeout};

#[test]
fn missing_required_fields_are_reported_at_build() {
    assert!(matches!(
        TimerJob::builder().build(),
        Err(TimerJobError::MissingJob)
    ));
    assert!(matches!(
        TimerJob::builder()
            .add_timer(job_duration(Duration::from_secs(1)))
            .build(),
        Err(TimerJobError::MissingJob)
    ));
    assert!(matches!(
        TimerJob::builder().add_job(|| async {}).build(),
        Err(TimerJobError::MissingTimer)
    ));
}

#[test]
fn duplicate_jobs_are_reported_at_build() {
    let builder = TimerJob::builder()
        .add_job(|| async {})
        .add_job(|| async { tokio::task::yield_now().await })
        .add_timer(job_duration(Duration::from_secs(1)))
        .option(task_option::task_name("重复配置"));

    assert!(matches!(builder.build(), Err(TimerJobError::DuplicateJob)));
}

#[test]
fn duplicate_timers_are_reported_at_build() {
    let builder = TimerJob::builder()
        .add_timer(job_duration(Duration::from_secs(1)))
        .add_job(|| async {})
        .add_timer(job_cron("0 0 * * *"));

    assert!(matches!(
        builder.build(),
        Err(TimerJobError::DuplicateTimer)
    ));
}

#[test]
fn zero_duration_is_rejected_at_build() {
    let builder = TimerJob::builder()
        .add_job(|| async {})
        .add_timer(job_duration(Duration::ZERO));

    assert!(matches!(
        builder.build(),
        Err(TimerJobError::InvalidDuration(Duration::ZERO))
    ));
}

#[test]
fn invalid_cron_is_rejected_at_build() {
    let builder = TimerJob::builder()
        .add_job(|| async {})
        .add_timer(job_cron("not a cron expression"));

    assert!(matches!(
        builder.build(),
        Err(TimerJobError::InvalidPattern(_))
    ));
}

#[test]
fn empty_names_are_rejected_at_build() {
    for name in ["", " \t\n"] {
        let result = TimerJob::builder()
            .add_job(|| async {})
            .add_timer(job_duration(Duration::from_secs(1)))
            .option(task_option::task_name(name))
            .build();

        assert!(matches!(result, Err(TimerJobError::EmptyTaskName)));
    }
}

struct CountingTimer(Arc<AtomicUsize>);

impl JobDefinition for CountingTimer {
    fn build(self) -> Result<JobSchedule, TimerJobError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        job_duration(Duration::from_secs(3600)).build()
    }
}

#[tokio::test]
async fn timer_is_validated_once_at_build_and_job_waits_for_start() {
    let validations = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(AtomicUsize::new(0));
    let job_invocations = Arc::clone(&invocations);
    let (executed_tx, mut executed_rx) = mpsc::unbounded_channel();

    let job = move || {
        job_invocations.fetch_add(1, Ordering::SeqCst);
        let executed_tx = executed_tx.clone();
        async move { executed_tx.send(()).unwrap() }
    };

    let builder = TimerJob::builder()
        .add_timer(CountingTimer(Arc::clone(&validations)))
        .add_job(job)
        .option(task_option::run_immediately());
    assert_eq!(validations.load(Ordering::SeqCst), 0);
    assert_eq!(invocations.load(Ordering::SeqCst), 0);

    let timer_job = builder.build().unwrap();
    assert_eq!(validations.load(Ordering::SeqCst), 1);
    assert_eq!(invocations.load(Ordering::SeqCst), 0);

    let manager = TimerJobManager::new();
    let id = manager.add(timer_job).await;
    assert_eq!(validations.load(Ordering::SeqCst), 1);
    assert_eq!(invocations.load(Ordering::SeqCst), 0);
    let info = manager.get(id).await.unwrap();
    assert_eq!(info.name, format!("timer-job-{id}"));
    assert_eq!(info.status, TimerJobStatus::Stopped);

    // 每次启动均立即执行一次，且不会重新构建调度定义。
    for _ in 0..2 {
        manager.start(id).await.unwrap();
        timeout(Duration::from_secs(1), executed_rx.recv())
            .await
            .unwrap()
            .unwrap();
        manager.stop(id).await.unwrap();
        assert_eq!(
            manager.get(id).await.unwrap().status,
            TimerJobStatus::Stopped
        );
    }
    assert_eq!(invocations.load(Ordering::SeqCst), 2);
    assert_eq!(validations.load(Ordering::SeqCst), 1);
    manager.del_all().await;
}

#[test]
fn duplicate_configuration_does_not_validate_the_timer() {
    let validations = Arc::new(AtomicUsize::new(0));

    let result = TimerJob::builder()
        .add_job(|| async {})
        .add_timer(CountingTimer(Arc::clone(&validations)))
        .add_timer(CountingTimer(Arc::clone(&validations)))
        .build();

    assert!(matches!(result, Err(TimerJobError::DuplicateTimer)));
    assert_eq!(validations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn options_can_repeat_and_the_last_name_wins() {
    let timer_job = TimerJob::builder()
        .option(task_option::task_name(" "))
        .add_job(|| async {})
        .option(task_option::run_immediately())
        .add_timer(job_duration(Duration::from_secs(1)))
        .option(task_option::task_name("刷新缓存"))
        .option(task_option::run_immediately())
        .build()
        .unwrap();

    let manager = TimerJobManager::new();
    let id = manager.add(timer_job).await;
    let info = manager.get(id).await.unwrap();
    assert_eq!(info.name, "刷新缓存");
    assert!(info.run_immediately);
    assert_eq!(info.status, TimerJobStatus::Stopped);
}

#[tokio::test]
async fn cron_supports_five_and_six_fields_with_default_options() {
    let manager = TimerJobManager::new();
    for pattern in ["0 0 * * *", "0 0 0 * * *"] {
        let job = || async {};
        let timer_job = TimerJob::builder()
            .add_job(job)
            .add_timer(job_cron(pattern))
            .build()
            .unwrap();
        let id = manager.add(timer_job).await;
        let info = manager.get(id).await.unwrap();
        assert!(!info.run_immediately);
        assert_eq!(info.status, TimerJobStatus::Stopped);
    }
    let jobs = manager.list().await;
    assert_eq!(jobs.len(), 2);
    assert!(jobs[0].id < jobs[1].id);
}
