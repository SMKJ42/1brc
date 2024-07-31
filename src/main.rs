use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::Instant;
use std::{fs::File, io::Error, sync::Arc};

use rust1brc::{get_data_path, StationData};

const CHUNK_SIZE: usize = 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let start = Instant::now();
    let path = get_data_path();
    let file = Arc::new(File::open(path).unwrap());
    let file_len = file.metadata().unwrap().len();
    let offset = Arc::new(Mutex::new(0u64));

    let map = Arc::new(Mutex::new(HashMap::new()));
    thread::scope(|scope| {
        for _thread in 0..thread::available_parallelism().unwrap().into() {
            let file = file.clone();
            let offset = offset.clone();
            let map = map.clone();
            scope.spawn(move || loop {
                let buf = [0u8; CHUNK_SIZE];
                let offset = offset.clone();
                let map = map.clone();

                let lock_offset = offset.lock().unwrap();
                if *lock_offset > file_len + CHUNK_SIZE as u64 {
                    break;
                }

                let chunk = load_chunk(&file, buf, lock_offset);
                parse_chunk(chunk.to_vec(), map);
            });
        }
    });
    let lock_map = map.lock().unwrap();
    let mut keys = lock_map.keys().collect::<Vec<_>>();
    keys.sort_unstable();

    for key in keys {
        println!("{}", lock_map[key].to_string());
    }

    let end = Instant::now();

    println!("time elapsed {}", end.duration_since(start).as_secs());

    Ok(())
}

fn load_chunk<'a>(
    file: &Arc<File>,
    mut buf: [u8; CHUNK_SIZE],
    mut offset: MutexGuard<u64>,
) -> Vec<u8> {
    let mut tail = buf.len();
    let head;

    file.read_at(&mut buf, *offset).unwrap();

    *offset += CHUNK_SIZE as u64;
    loop {
        if tail == 0 {
            return Vec::new();
        }
        tail -= 1;
        if buf[tail] == b'\n' {
            break;
        }
    }

    match *offset {
        0 => head = 0,
        _ => {
            let idx = buf.iter().position(|b| b == &b'\n');
            match idx {
                Some(e) => head = e,
                None => {
                    head = tail;
                }
            }
        }
    }

    return buf[head..tail].to_vec();
}

fn parse_chunk(chunk: Vec<u8>, outer_map: Arc<Mutex<HashMap<String, StationData>>>) -> () {
    let mut map = HashMap::new();
    for line in chunk.split(|&b| b == b'\n').filter(|line| !line.is_empty()) {
        let delim_opt = line
            .iter()
            .enumerate()
            .find_map(|(idx, byte)| (byte == &b';').then_some(idx));

        match delim_opt {
            Some(delim) => {
                let station_name = unsafe { String::from_utf8_unchecked(line[..delim].to_vec()) };

                let temperature =
                    unsafe { String::from_utf8_unchecked(line[(delim + 1)..].to_vec()) };

                map.entry(station_name.clone())
                    .and_modify(|station: &mut StationData| {
                        station.insert(temperature.parse().unwrap())
                    })
                    .or_insert_with(|| {
                        StationData::new(station_name, temperature.trim().parse().unwrap())
                    });
            }
            None => {
                let output = String::from_utf8(line.to_vec()).unwrap();
                println!("line: {}", output);
                panic!()
            }
        }
    }

    let mut outer = outer_map.lock().expect("non-poisoned mutex");
    for (station_name, records) in map.into_iter() {
        outer
            .entry(station_name)
            .and_modify(|station| station.merge(records.clone()))
            .or_insert(records);
    }
}
