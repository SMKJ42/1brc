use std::cell::RefCell;
use std::io::SeekFrom;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use hashbrown::HashMap;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeek, AsyncSeekExt, BufReader};

const CHUNK_SIZE: usize = 1024 * 1024 * 4;
const PEEK: usize = 150;

#[inline]
#[tokio::main]
async fn main() {
    let path = get_data_path();

    let start = Instant::now();

    let mut offset = CHUNK_SIZE as u64 - PEEK as u64;

    let mut file = File::open(path.clone()).await.unwrap();
    let stations: Arc<RwLock<HashMap<Vec<u8>, Arc<Mutex<StationData>>>>> =
        Arc::new(RwLock::new(HashMap::new()));

    let mut chunk_offsets = vec![0];

    let mut scan_buf = [0; PEEK];
    let mut read = 1;
    while read != 0 {
        file.seek(std::io::SeekFrom::Start(offset)).await.unwrap();
        read = file.read(&mut scan_buf).await.unwrap();

        for (idx, ch) in scan_buf.iter().rev().enumerate() {
            if ch == &0xA {
                offset += (CHUNK_SIZE - idx) as u64;
                chunk_offsets.push(offset);
                break;
            }
        }
    }

    let idx = chunk_offsets.len() - 1;
    file.seek(SeekFrom::End(0)).await.unwrap();
    chunk_offsets[idx] = file.stream_position().await.unwrap();

    println!("chunk: {}", chunk_offsets[idx]);

    let mut i = 0;

    const THREAD_COUNT: usize = 12;
    let mut file = File::open(path).await.unwrap();

    while i < chunk_offsets.len() {
        let mut futs = Vec::new();

        let mut chunks: Vec<RefCell<Reader>> = Vec::new();
        // println!("offset: {total}, i: {i}, len: {}", chunk_offsets.len());

        for _ in 0..THREAD_COUNT {
            let len = (chunk_offsets[i + 1] - chunk_offsets[i]) as usize;
            let mut buf = vec![0; len];
            file.read_exact(&mut buf).await.unwrap();
            let reader = Reader::new(buf);
            chunks.push(RefCell::new(reader));
        }

        for chunk in chunks.iter() {
            let stations = stations.clone();
            let mut chunk = chunk.take();

            let handle = tokio::spawn(async move {
                let mut t_stations = HashMap::new();

                parse_chunk(&mut chunk, &mut t_stations);

                let mut stations_read = stations.read().unwrap();

                for (name, data) in t_stations {
                    // println!("offset: {total}, i: {i}");
                    // println!("i: {i} . {}", String::from_utf8(name.clone()).unwrap());

                    if let Some(station) = stations_read.get(&name) {
                        let mut station = station.lock().unwrap();
                        station.combine(&data);
                    } else {
                        drop(stations_read);
                        stations
                            .write()
                            .unwrap()
                            .insert(name, Arc::new(Mutex::new(data)));
                        stations_read = stations.read().unwrap();
                    }
                }
            });

            futs.push(handle);

            i += 1;
        }
        for fut in futs {
            tokio::join!(fut).0.unwrap();
        }
    }
}

fn parse_chunk(reader: &mut Reader, stations: &mut HashMap<Vec<u8>, StationData>) {
    while reader.has_remaining() {
        let station_name = reader.read_station();
        if let Some(station) = stations.get_mut(&station_name) {
            let temp = reader.read_temp();
            station.add_temp_data(temp);
        } else {
            let temp = reader.read_temp();
            let station = StationData::new(temp);
            stations.insert(station_name.to_vec(), station);
        }
    }
}

#[derive(Default)]
struct Reader {
    buf: Vec<u8>,
    pos: usize,
}

impl Reader {
    fn new(buf: Vec<u8>) -> Self {
        return Self { buf, pos: 0 };
    }

    fn read_station(&mut self) -> Vec<u8> {
        let mut last = self.pos;

        while last < self.buf.len() && self.buf[last] != b';' {
            last += 1;
        }

        let str = &self.buf[self.pos..last];
        self.pos = last + 1;
        return str.to_vec();
    }

    fn read_temp(&mut self) -> i64 {
        let mut temp = 0;
        let neg: bool;

        if self.has_remaining() && self.buf[self.pos] == b'-' {
            self.pos += 1;
            neg = true;
        } else {
            neg = false;
        }

        while self.has_remaining() && self.buf[self.pos] != 0xA {
            debug_assert!(!(self.pos >= self.buf.len()));
            if self.buf[self.pos] != b'.' {
                temp = temp * 10 + (self.buf[self.pos] & 15) as i64;
            }
            self.pos += 1;
        }
        self.pos += 1;
        if neg {
            temp = -1 * temp;
        }
        return temp;
    }

    fn has_remaining(&self) -> bool {
        return self.pos < self.buf.len();
    }
}

use std::fmt::{Display, Formatter};

pub fn get_data_path() -> String {
    let args = std::env::args().collect::<Vec<String>>();
    let source = args.get(1).unwrap_or_else(|| {
        println!("Please provide a file path");
        std::process::exit(1);
    });
    return "data/".to_string() + source + ".txt";
}

#[derive(PartialEq, Debug, Clone)]
pub struct StationData {
    count: usize,
    min: i64,
    max: i64,
    sum: i64,
}

impl StationData {
    pub fn new(temp: i64) -> Self {
        Self {
            count: 1,
            min: temp,
            max: temp,
            sum: temp,
        }
    }

    pub fn add_temp_data(&mut self, temperature: i64) {
        self.min = self.min.min(temperature);
        self.max = self.max.max(temperature);
        self.sum += temperature;
        self.count += 1;
    }

    fn calculate_mean(&self) -> i64 {
        return self.sum / self.count as i64;
    }

    fn combine(&mut self, other: &Self) {
        self.count += other.count;
        self.min.min(other.min);
        self.max.max(other.max);
        self.sum += other.sum;
    }
}

impl Display for StationData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:.1}/{:.1}/{:.1}",
            self.min as f32 / 10.,
            self.max as f32 / 10.,
            self.calculate_mean() as f32 / 10.,
        )
    }
}
