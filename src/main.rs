mod tools;

use std::os::unix::fs::FileExt;
use std::time::Instant;
use std::usize;
use std::{fs::File, io::Error, sync::Arc};

use tokio::sync::{Mutex, MutexGuard};
use tokio::task::JoinHandle;
use tools::{get_data_path, StationDataItem};

const CHUNK_SIZE: usize = 512 * 512;

fn main() -> Result<(), Error> {
    let path = get_data_path();

    let file = File::open(path).unwrap();
    let file = Arc::new(file);

    let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let last = Arc::new(Mutex::new(JoinHandle::from(rt.spawn(async move {}))));
    let pointer = Arc::new(Mutex::new(0));

    let start = Instant::now();

    loop {
        let lock_pointer = pointer.blocking_lock();
        if *lock_pointer == file.metadata().unwrap().len() {
            break;
        }
        let (chunk, left_offset) = get_chunk(&file, lock_pointer);
        let mut lock_last = last.blocking_lock();

        *lock_last = rt.spawn_blocking(move || parse_chunk(chunk, left_offset));
    }

    loop {
        if last.blocking_lock().is_finished() {
            break;
        }
    }

    let end = Instant::now();

    println!("finished in: {}", end.duration_since(start).as_millis());

    Ok(())
}

fn get_chunk(file: &File, mut pointer: MutexGuard<'_, u64>) -> (Box<[u8]>, usize) {
    let mut buf = [0; CHUNK_SIZE];
    let right_align = CHUNK_SIZE as i64 - (file.metadata().unwrap().len() - *pointer) as i64;

    if right_align > 0 {
        *pointer -= right_align as u64;
        file.read_exact_at(&mut buf, *pointer).unwrap();

        *pointer += CHUNK_SIZE as u64;
        return (Box::new(buf), 0);
    } else {
        file.read_exact_at(&mut buf, *pointer).unwrap();
        let mut left_align = buf.len() - 1;
        while left_align > 0 {
            if buf[left_align] == b'\n' {
                break;
            } else {
                left_align -= 1
            }
        }

        *pointer += left_align as u64;

        return (Box::new(buf), left_align);
    }
}

fn parse_chunk(chunk: Box<[u8]>, left_offset: usize) -> () {
    let chunk = &chunk[..left_offset];

    for line in chunk.split(|&b| b == b'\n').filter(|line| !line.is_empty()) {
        let delim_opt = line
            .iter()
            .enumerate()
            .find_map(|(idx, byte)| (byte == &b';').then_some(idx));

        match delim_opt {
            Some(delim) => {
                let station = unsafe { String::from_utf8_unchecked(line[..delim].to_vec()) };
                let temperature =
                    unsafe { String::from_utf8_unchecked(line[(delim + 1)..].to_vec()) };

                let item = StationDataItem {
                    station: station.as_str(),
                    temperature: temperature.parse().unwrap(),
                };
            }
            None => {
                let output = String::from_utf8(line.to_vec()).unwrap();
                println!("line: {}", output);
                panic!()
            }
        }
    }
}
