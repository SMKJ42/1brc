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
    station: String,
    count: usize,
    min: i64,
    max: i64,
    sum: i64,
}

impl StationData {
    pub fn new(station: String, temp: i64) -> Self {
        Self {
            station,
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
}

impl Display for StationData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}={}/{}/{}",
            self.station,
            self.min as f32 / 10000.,
            self.max as f32 / 10000.,
            self.calculate_mean() as f32 / 10000.
        )
    }
}
