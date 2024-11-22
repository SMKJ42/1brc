use core::str;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek};
use std::time::Instant;

use rust1brc::{get_data_path, StationData};

const CHUNK_SIZE: usize = 1024 * 1024 * 4;
// const CHUNK_SIZE: usize = ;

fn main() {
    let start = Instant::now();
    let path = get_data_path();
    let mut file = File::open(path).unwrap();
    let mut offset = 0;
    let mut stations: HashMap<String, StationData> = HashMap::new();

    let mut buf = [0; CHUNK_SIZE];
    loop {
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        let read = file.read(&mut buf).unwrap();
        let mut last = 0;

        for (idx, ch) in buf.iter().rev().enumerate() {
            if ch == &0xA {
                last = CHUNK_SIZE - idx;
                offset = offset + last as u64;
                break;
            }
        }

        let mut reader = Reader::new(&buf[0..last]);

        while reader.has_remaining() {
            parse_data(&mut reader, &mut stations);
        }

        if read != CHUNK_SIZE {
            break;
        }
    }

    file.seek(std::io::SeekFrom::Start(offset)).unwrap();
    let read = file.read(&mut buf).unwrap();
    let mut reader = Reader::new(&buf[..read]);
    while reader.has_remaining() {
        parse_data(&mut reader, &mut stations);
    }

    let end = Instant::now();

    let mut all: Vec<_> = stations.into_iter().collect();
    all.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    for (_, station) in all.iter() {
        println!("{}", station);
    }

    println!("num stations: {}", all.len());

    println!("time elapsed {}", end.duration_since(start).as_millis());
}

fn parse_data(reader: &mut Reader, stations: &mut HashMap<String, StationData>) {
    while reader.has_remaining() {
        let station_name = reader.read_str();
        if let Some(station) = stations.get_mut(station_name) {
            let temp = reader.read_temp();
            station.add_temp_data(temp);
        } else {
            let station_name = station_name.to_string();
            let temp = reader.read_temp();
            let station = StationData::new(station_name.clone(), temp);
            stations.insert(station_name, station);
        }
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        return Self { buf, pos: 0 };
    }

    fn read_str(&mut self) -> &str {
        let mut last = self.pos;
        while last < self.buf.len() && self.buf[last] != b';' {
            last += 1;
        }

        let str = unsafe { str::from_utf8_unchecked(&self.buf[self.pos..last]) };
        self.pos = last + 1;
        return str;
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

        // ew...
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
