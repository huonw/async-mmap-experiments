import aiofiles
import asyncio
import contextlib
import mmap
import subprocess
import time
import concurrent.futures

BUF_SIZE = 1 << 16
STEP_SIZE = 512
NUM_REPEATS = 10


def touch_buffer(buf, read, idx):
    while idx < read:
        buf[idx]
        idx += STEP_SIZE

    # next iteration does needs to be offset by whatever was left over
    return idx % read


def touch_mm(mm):
    touch_buffer(mm, len(mm), 0)


async def async_touch_mm(mm):
    touch_mm(mm)


async def run_async_mmap(mms):
    await asyncio.gather(*(async_touch_mm(mm) for mm in mms))


def run_sequential_sync_mmap(mms):
    for mm in mms:
        touch_mm(mm)


def run_sync_mmap(mms):
    with concurrent.futures.ThreadPoolExecutor(len(mms)) as tp:
        list(tp.map(touch_mm, mms))


def sync_touch_file(file):
    buf = bytearray(BUF_SIZE)
    idx = 0
    while True:
        read = file.readinto(buf)
        if read == 0:
            return

        idx = touch_buffer(buf, read, idx)


def run_sequential_sync_files(files):
    for f in files:
        sync_touch_file(f)


def run_sync_files(files):
    with concurrent.futures.ThreadPoolExecutor(len(files)) as tp:
        list(tp.map(sync_touch_file, files))


async def async_touch_file(sync_file):
    file = await aiofiles.open(sync_file.name, sync_file.mode)

    buf = bytearray(BUF_SIZE)
    idx = 0
    while True:
        read = await file.readinto(buf)
        if read == 0:
            return

        idx = touch_buffer(buf, read, idx)


async def run_sequential_async_files(files):
    for f in files:
        await async_touch_file(f)


async def run_async_files(files):
    await asyncio.gather(*(async_touch_file(f) for f in files))


def drop_caches():
    # macOS specific
    subprocess.run(["sync"])
    subprocess.run(["sudo", "purge"])


def main():
    names = [f"file.{idx + 1}" for idx in range(8)]

    print("use_async,use_mmap,use_parallel,cold_cache,repeat,duration")
    for repeat in range(NUM_REPEATS):
        for use_async in [True, False]:
            for use_mmap in [True, False]:
                for use_parallel in [True, False]:
                    for cold_cache in [True, False]:
                        if cold_cache:
                            drop_caches()

                        files = [open(n, "rb") for n in names]
                        mms = [
                            mmap.mmap(f.fileno(), 0, mmap.MAP_PRIVATE, mmap.PROT_READ)
                            for f in files
                        ]

                        start = time.perf_counter()

                        match (use_async, use_mmap, use_parallel):
                            case (False, False, False):
                                run_sequential_sync_files(files)
                            case (False, False, True):
                                run_sync_files(files)
                            case (False, True, False):
                                run_sequential_sync_mmap(mms)
                            case (False, True, True):
                                run_sync_mmap(mms)
                            case (True, False, False):
                                asyncio.run(run_sequential_async_files(files))
                            case (True, False, True):
                                asyncio.run(run_async_files(files))
                            # not implemented
                            case (True, True, False):
                                continue
                            case (True, True, True):
                                asyncio.run(run_async_mmap(mms))

                        end = time.perf_counter()
                        duration = end - start
                        print(
                            f"{use_async},{use_mmap},{use_parallel},{cold_cache},{repeat},{duration:.3}".lower()
                        )

                        for f in files:
                            f.close()
                        for mm in mms:
                            mm.close()


if __name__ == "__main__":
    main()
