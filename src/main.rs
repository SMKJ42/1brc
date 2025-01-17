use std::cell::RefCell;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::iter::Peekable;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use std::{str, usize};

use hashbrown::HashMap;

const CHUNK_SIZE: u64 = 1024 * 1024 * 64;
const THREAD_COUNT: usize = 16;
const PEEK: usize = 100;

#[inline]
#[tokio::main]
async fn main() {
    let path = get_data_path();
    let start = Instant::now();

    let mut file = File::open(path.clone()).unwrap();

    let file_len = file.metadata().unwrap().len();

    let stations: Arc<RwLock<HashMap<Vec<u8>, Arc<Mutex<StationData>>>>> =
        Arc::new(RwLock::new(HashMap::new()));

    let delims = align_chunks(&mut file, file_len).await;

    let mut delims_iter = Box::new(delims.iter()).peekable();

    let mut total = 0;

    while delims_iter.peek().is_some() {
        let mut chunks = read_chunks(&mut file, &mut delims_iter).await;

        total += chunks.len();

        parse_chunks(&mut chunks, &stations).await;
    }

    print_out(stations);

    println!(
        "total chunks: {}, processed chunks: {}",
        delims.len(),
        total
    );

    println!(
        "Elapsed: {} ms",
        Instant::now().duration_since(start).as_millis()
    );
}

#[inline]
async fn align_chunks(file: &mut File, file_len: u64) -> Vec<u64> {
    let mut offset = CHUNK_SIZE;
    let mut scan_buf = [0; PEEK];
    let mut chunk_offsets = vec![0];

    while offset < file_len {
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        file.read(&mut scan_buf).unwrap();

        let mut found_delimeter = false;
        for (idx, ch) in scan_buf.iter().rev().enumerate() {
            // branch on line feed char
            if *ch == 0xA {
                found_delimeter = true;
                // subtract the offset
                offset += (PEEK - idx) as u64;

                // push the delimeter into the vector.
                chunk_offsets.push(offset);

                // advance the file
                offset += CHUNK_SIZE;
                break;
            }
        }
        assert!(found_delimeter);
    }

    let mut prev = chunk_offsets[0];

    // turn the index of the delimeters into a iterator of distances.
    let mut out: Vec<u64> = chunk_offsets
        .iter()
        .skip(1)
        .map(move |curr| {
            let test = curr - prev;
            prev = *curr;
            return test;
        })
        .collect();

    // if we failed to align the last chunk, which is likely, push it to the output.
    if offset != file_len {
        let last = chunk_offsets[chunk_offsets.len() - 1];
        out.push(file_len - last);
    }

    // reset the file back to start.
    file.seek(SeekFrom::Start(0)).unwrap();

    return out;
}

#[inline]
async fn read_chunks<'a>(
    file: &mut File,
    delims_iter: &mut Peekable<Box<std::slice::Iter<'a, u64>>>,
) -> Vec<RefCell<Reader>> {
    let mut chunks: Vec<RefCell<Reader>> = Vec::new();

    for _ in 0..THREAD_COUNT {
        let len: usize;
        if let Some(delim) = delims_iter.next() {
            len = *delim as usize;
        } else {
            break;
        }

        let mut buf = vec![0; len];

        file.read_exact(&mut buf).unwrap();
        let reader = Reader::new(buf);
        chunks.push(RefCell::new(reader));
    }

    return chunks;
}

#[inline]
async fn parse_chunks(
    chunks: &mut Vec<RefCell<Reader>>,
    stations: &Arc<RwLock<HashMap<Vec<u8>, Arc<Mutex<StationData>>>>>,
) {
    let mut futs = Vec::new();

    for chunk in chunks.iter() {
        let stations = stations.clone();
        let mut chunk = chunk.take();

        let handle = std::thread::spawn(move || {
            let mut t_stations = HashMap::new();

            parse_chunk(&mut chunk, &mut t_stations);

            let mut stations_read = stations.read().unwrap();

            for (name, data) in t_stations {
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
                };
            }
        });

        futs.push(handle);
    }

    for fut in futs {
        fut.join().unwrap();
    }
}

#[inline]
fn parse_chunk(reader: &mut Reader, stations: &mut HashMap<Vec<u8>, StationData>) {
    while reader.has_remaining() {
        let station_name = reader.read_station_name();

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

#[inline]
fn print_out(stations: Arc<RwLock<HashMap<Vec<u8>, Arc<Mutex<StationData>>>>>) {
    let mut sum: usize = 0;

    let all = stations.read().unwrap();
    let mut all: Vec<_> = all
        .clone()
        .into_iter()
        .map(|x| (x.0, x.1.lock().unwrap().clone()))
        .collect();

    all.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    for (station_name, station_data) in all.into_iter() {
        sum += station_data.count;
        println!("{}={station_data}", unsafe {
            str::from_utf8_unchecked(&station_name)
        })
    }

    if sum != 1_000_000_000 {
        println!(
            "\r\n*** WARNING: Did not parse all 1bn rows. If you're not testing on the full data set, disreguard.
    rows parsed: {sum}
"
        )
    } else {
        println!("\r\nprocessed 1bn lines.")
    };
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

    #[inline]
    fn read_station_name(&mut self) -> Vec<u8> {
        let mut last = self.pos;

        while last < self.buf.len() {
            if self.buf[last] == 0x3B {
                break;
            }
            last += 1;
        }

        let str = &self.buf[self.pos..last];
        self.pos = last + 1;
        return str.to_vec();
    }

    #[inline]
    fn read_temp(&mut self) -> i64 {
        let mut temp = 0;
        let neg: bool;

        assert!(self.has_remaining());

        if self.buf[self.pos] == b'-' {
            self.pos += 1;
            neg = true;
        } else {
            neg = false;
        }

        while self.has_remaining() && self.buf[self.pos] != 0xA {
            assert!(!(self.pos >= self.buf.len()));
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

    #[inline]
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

    #[inline]
    pub fn add_temp_data(&mut self, temperature: i64) {
        self.count += 1;
        self.min = self.min.min(temperature);
        self.max = self.max.max(temperature);
        self.sum += temperature;
    }

    fn calculate_mean(&self) -> i64 {
        return self.sum / self.count as i64;
    }

    #[inline]
    fn combine(&mut self, other: &Self) {
        self.count += other.count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
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
