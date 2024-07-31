use std::fmt::{Display, Formatter};

pub fn get_data_path() -> String {
    let args = std::env::args().collect::<Vec<String>>();
    let source = args.get(1).unwrap_or_else(|| {
        println!("Please provide a file path");
        std::process::exit(1);
    });
    return "data/".to_string() + source + ".txt";
}

#[derive(Debug)]
pub struct StationDataItem {
    pub temperature: f64,
}

impl Display for StationDataItem {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.temperature)
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct StationData {
    station: String,
    count: usize,
    min: f64,
    max: f64,
    sum: f64,
}

impl StationData {
    pub fn new(station: String, temp: f64) -> Self {
        Self {
            station,
            count: 1,
            min: temp,
            max: temp,
            sum: temp,
        }
    }

    pub fn insert(&mut self, temperature: f64) {
        self.min = self.min.min(temperature);
        self.max = self.max.max(temperature);
        self.sum += temperature;
        self.count += 1;
    }

    pub fn merge(&mut self, other: Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    fn calculate_mean(&self) -> f64 {
        return self.sum / self.count as f64;
    }

    pub fn to_string(&self) -> String {
        format!(
            "{}={}/{}/{}",
            self.station,
            self.min,
            self.max,
            self.calculate_mean()
        )
    }
}
