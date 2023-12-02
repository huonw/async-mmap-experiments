use std::io::Read;
use tokio::io::AsyncReadExt;

const BUF_SIZE: usize = 1 << 18;
const STEP_SIZE: usize = 512;
const NUM_REPEATS: usize = 1;

fn touch_mm(mm: &memmap2::Mmap) -> u8 {
    let mut sum = 0;
    for idx in (0..mm.len()).step_by(STEP_SIZE) {
        sum += mm[idx];
    }
    sum
}

fn touch_buffer(buf: &[u8], read: usize, idx: &mut usize, sum: &mut u8) {
    while *idx < read {
        *sum += buf[*idx];
        *idx += STEP_SIZE;
    }
    // next iteration does needs to be offset by whatever was left over
    *idx %= read;
}

fn run_sync_mmap(mms: Vec<memmap2::Mmap>) {
    let handles: Vec<_> = mms
        .into_iter()
        .map(|mm| std::thread::spawn(move || touch_mm(&mm)))
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

fn touch_sync_file(mut f: std::fs::File) -> u8 {
    let mut sum = 0;
    let mut buf = [0; BUF_SIZE];
    let mut idx = 0;
    loop {
        let read = f.read(&mut buf).unwrap();
        if read == 0 {
            break;
        }
        touch_buffer(&buf, read, &mut idx, &mut sum);
    }
    sum
}

fn run_sync_file(files: Vec<std::fs::File>) {
    let handles: Vec<_> = files
        .into_iter()
        .map(|f| std::thread::spawn(move || touch_sync_file(f)))
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

async fn run_async_mmap(mms: Vec<memmap2::Mmap>) {
    futures::future::join_all(mms.into_iter().map(|mm| async move { touch_mm(&mm) })).await;
}

async fn touch_async_file(mut f: tokio::fs::File) -> u8 {
    let mut sum = 0;
    let mut buf = [0; BUF_SIZE];
    let mut idx = 0;
    loop {
        let read = f.read(&mut buf).await.unwrap();
        if read == 0 {
            break;
        }
        touch_buffer(&buf, read, &mut idx, &mut sum);
    }
    sum
}

async fn run_async_file(files: Vec<std::fs::File>) {
    futures::future::join_all(
        files
            .into_iter()
            .map(|f| touch_async_file(tokio::fs::File::from_std(f))),
    )
    .await;
}

fn drop_caches() {
    // macOS specific
    std::process::Command::new("sync").output().unwrap();
    std::process::Command::new("sudo")
        .arg("purge")
        .output()
        .unwrap();
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    println!("use_async,use_mmap,cold_cache,repeat,duration");
    for use_async in [true, false] {
        for use_mmap in [true, false] {
            for cold_cache in [true, false] {
                for repeat in 0..NUM_REPEATS {
                    if cold_cache {
                        drop_caches();
                    }

                    let files: Vec<_> = (1..=8)
                        .map(|i| std::fs::File::open(format!("file.{i}")).unwrap())
                        .collect();
                    let mms: Vec<_> = files
                        .iter()
                        .map(|f| unsafe { memmap2::Mmap::map(f) }.unwrap())
                        .collect();

                    let start = std::time::Instant::now();

                    match (use_async, use_mmap) {
                        (false, false) => run_sync_file(files),
                        (false, true) => run_sync_mmap(mms),
                        (true, false) => rt.block_on(run_async_file(files)),
                        (true, true) => rt.block_on(run_async_mmap(mms)),
                    }

                    let end = std::time::Instant::now();
                    let duration = (end - start).as_secs_f64();
                    println!("{use_async},{use_mmap},{cold_cache},{repeat},{duration:.3}");
                }
            }
        }
    }
}
