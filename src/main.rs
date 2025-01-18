use std::fmt::{Display, Formatter};
use std::sync::{mpsc::SyncSender, Arc, Mutex};
use std::{io::SeekFrom, pin::Pin, str, task::Poll, time::Instant, usize};

use futures::{Stream, StreamExt};

use hashbrown::HashMap;

use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::{fs::File, runtime::Builder, task::JoinHandle};

const CHUNK_SIZE: u64 = 1024 * 1024 * 32;
const THREAD_COUNT: usize = 12;
const PEEK: usize = 100;

#[inline]
fn main() {
    let path = get_data_path();
    let start = Instant::now();

    let mut builder = Builder::new_multi_thread();
    builder.worker_threads(THREAD_COUNT);
    builder.enable_all();

    let rt = builder.build().unwrap();

    rt.block_on(async {
        let mut file = File::open(path.clone()).await.unwrap();
        let file_len = file.metadata().await.unwrap().len();

        let mut stations = StationMap::new();
        let chunks = AlignmentStream::new(align_chunks(&mut file, file_len).await);

        let (tx, rx) = std::sync::mpsc::sync_channel(THREAD_COUNT * 4);
        let mut thread_pool = Vec::new();

        for _ in 0..THREAD_COUNT {
            thread_pool.push(dispatch_thread(&chunks, &tx, path.clone()));
        }

        let mut ackd = 0;
        let mut total = 0;
        let mut chunks_count = 0;

        while ackd < THREAD_COUNT {
            let data = rx.recv().unwrap();
            match data {
                ChannelSignal::Data(data) => {
                    for station in &data.inner {
                        total += station.1.count;
                    }
                    chunks_count += 1;
                    stations.combine(data);
                }
                ChannelSignal::End => {
                    ackd += 1;
                }
            }
        }

        print_out(stations);
        println!(
            "\r\nTotal Elapsed time: {} ms",
            Instant::now().duration_since(start).as_millis()
        );

        if total != 1_000_000_000 {
            println!("\r\n\t*** WARNING: Did not parse all 1bn rows. If you're not testing on the full data set, disreguard.\r\trows parsed: {total}\r\n")
        } else {
            println!("\tProcessed 1bn lines.")
        };

        println!(
            "\tTotal chunks: {}, Processed chunks: {}",
            chunks.inner.len(), chunks_count
        );

    })
}

#[inline]
fn dispatch_thread(
    chunks: &AlignmentStream,
    tx: &SyncSender<ChannelSignal>,
    path: String,
) -> JoinHandle<()> {
    let tx = tx.clone();
    let path = path.clone();
    let mut chunks = chunks.clone();
    tokio::spawn(async move {
        loop {
            let mut buf: Vec<u8>;
            // read in a chunk if one is queued. else, return END signal
            if let Some(chunk) = chunks.next().await {
                let mut file = File::open(&path).await.unwrap();
                file.seek(chunk.start()).await.unwrap();
                let size = (chunk.end - chunk.start) as usize;
                buf = vec![0; size];
                file.read_exact(&mut buf).await.unwrap();
            } else {
                tx.send(ChannelSignal::End).unwrap();
                return;
            }

            let mut reader = Reader::new(buf);
            let mut stations = StationMap::new();
            parse_chunk(&mut reader, &mut stations);
            tx.send(ChannelSignal::Data(stations)).unwrap();
        }
    })
}

#[inline]
async fn align_chunks(file: &mut File, file_len: u64) -> Vec<Alignment> {
    let mut offset = CHUNK_SIZE;
    let mut scan_buf = [0; PEEK];
    let mut chunk_offsets = vec![0];

    while offset < file_len {
        file.seek(std::io::SeekFrom::Start(offset)).await.unwrap();
        file.read(&mut scan_buf).await.unwrap();

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
    let mut out: Vec<Alignment> = chunk_offsets
        .iter()
        .skip(1)
        .map(|curr| {
            let align = Alignment {
                start: prev,
                end: *curr,
            };
            prev = *curr;
            return align;
        })
        .collect();

    // if we failed to align the last chunk, which is likely, push it to the output.
    if offset != file_len {
        let last = chunk_offsets[chunk_offsets.len() - 1];
        out.push(Alignment {
            start: last,
            end: file_len,
        })
    }

    // reset the file back to start.
    file.seek(SeekFrom::Start(0)).await.unwrap();

    return out;
}

#[inline]
fn parse_chunk(reader: &mut Reader, stations: &mut StationMap) {
    while reader.has_remaining() {
        let station_name = reader.read_station_name();

        if let Some(station) = stations.get_mut(station_name) {
            let temp = reader.read_temp();
            station.add_temp_data(temp);
        } else {
            let station_name = station_name.to_owned();
            let station = StationData::new(reader.read_temp());
            stations.insert(station_name, station);
        }
    }
}

#[inline]
fn print_out(stations: StationMap) {
    let mut sum: usize = 0;

    let all = stations.inner;
    let mut all: Vec<_> = all.clone().into_iter().map(|x| (x.0, x.1)).collect();

    all.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    for (station_name, station_data) in all.into_iter() {
        sum += station_data.count;
        println!("{}={station_data}", unsafe {
            str::from_utf8_unchecked(&station_name)
        })
    }
}

pub fn get_data_path() -> String {
    let args = std::env::args().collect::<Vec<String>>();
    let source = args.get(1).unwrap_or_else(|| {
        println!("Please provide a file path");
        std::process::exit(1);
    });
    return "data/".to_string() + source + ".txt";
}

#[derive(Clone)]
struct AlignmentStream {
    inner: Pin<Vec<Alignment>>,
    idx: Arc<Mutex<usize>>,
}

impl AlignmentStream {
    #[inline]
    fn new(inner: Vec<Alignment>) -> Self {
        return Self {
            inner: Pin::new(inner),
            idx: Arc::new(Mutex::new(0)),
        };
    }
}

impl Stream for AlignmentStream {
    type Item = Alignment;

    // hackey solution to get an iterator to be Send + Sync
    #[inline]
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut idx = self.idx.lock().unwrap();

        if *idx < self.inner.len() {
            let out = self.inner[*idx].clone();
            *idx += 1;
            return Poll::Ready(Some(out));
        } else {
            return Poll::Ready(None);
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        return (*self.idx.lock().unwrap(), Some(self.inner.len()));
    }
}

enum ChannelSignal {
    End,
    Data(StationMap),
}

#[derive(Clone)]
struct Alignment {
    start: u64,
    end: u64,
}

impl Alignment {
    #[inline]
    fn start(&self) -> SeekFrom {
        return SeekFrom::Start(self.start);
    }

    #[inline]
    fn end(&self) -> SeekFrom {
        return SeekFrom::Start(self.end);
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

    #[inline]
    fn read_station_name<'a>(&'a mut self) -> &[u8] {
        let mut last = self.pos;

        while last < self.buf.len() {
            if self.buf[last] == 0x3B {
                break;
            }
            last += 1;
        }

        let str = &self.buf[self.pos..last];
        self.pos = last + 1;
        return str;
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

#[derive(PartialEq, Debug, Clone)]
pub struct StationData {
    count: usize,
    min: i64,
    max: i64,
    sum: i64,
}

struct StationMap {
    inner: HashMap<Vec<u8>, StationData>,
}

impl StationMap {
    fn new() -> Self {
        return Self {
            inner: HashMap::new(),
        };
    }

    fn get_mut(&mut self, key: &[u8]) -> Option<&mut StationData> {
        return self.inner.get_mut(key);
    }

    #[inline]
    fn combine(&mut self, other: Self) {
        for (name, data) in other.inner {
            self.insert(name, data);
        }
    }

    #[inline]
    fn insert(&mut self, name: Vec<u8>, data: StationData) {
        self.inner.insert(name, data);
    }
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

    #[inline]
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
