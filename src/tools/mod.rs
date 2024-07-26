use std::fmt::{Display, Formatter};

pub mod metric;

pub fn get_data_path() -> String {
    let args = std::env::args().collect::<Vec<String>>();
    let source = args.get(1).unwrap_or_else(|| {
        println!("Please provide a file path");
        std::process::exit(1);
    });
    return "data/".to_string() + source + ".txt";
}

#[derive(Debug)]
pub struct StationDataItem<'a> {
    pub station: &'a str,
    pub temperature: f32,
}

impl Display for StationDataItem<'_> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}:{}", self.station, self.temperature)
    }
}

// #[derive(Hash, Eq, PartialEq, Debug)]
pub struct StationData {
    station: String,
    count: usize,
    min: f32,
    max: f32,
    sum: f32,
}

impl StationData {
    fn new(station: String, temp: f32) -> Self {
        Self {
            station,
            count: 1,
            min: temp,
            max: temp,
            sum: temp,
        }
    }

    fn insert(&mut self, temperature: f32) {
        self.min = self.min.min(temperature);
        self.max = self.max.max(temperature);
        self.sum += temperature;
        self.count += 1;
    }

    fn calculate_mean(&self) -> f32 {
        return self.sum / self.count as f32;
    }

    fn to_string(&self) -> String {
        format!(
            "{}={}/{}/{}",
            self.station,
            self.min,
            self.max,
            self.calculate_mean()
        )
    }
}

pub type StationDataCollection = Vec<StationData>;
