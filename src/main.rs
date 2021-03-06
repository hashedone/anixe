extern crate argparse;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate chrono;
extern crate csv;
extern crate serde_json;

use std::collections::HashMap;

struct Config {
    input_path: String,
    output_path: String,
    hotels_path: String,
    rooms_path: String,
}

fn serialize_price<S>(price: &f32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    // Just fancy rounding to 2 decimal places
    let fpval = (price * 100.0).round();
    let s = format!("{}", fpval);
    let (l, r) = s.split_at(s.len() - 2);
    let s = format!("{}.{}", l, r);
    serializer.serialize_str(&s)
}

fn deserialize_checkin<'a, D>(deserializer: D) -> Result<chrono::NaiveDate, D::Error>
where
    D: serde::Deserializer<'a>,
{
    use serde::de::Error;
    use serde::Deserialize;

    let val = String::deserialize(deserializer)?;
    chrono::NaiveDate::parse_from_str(&val, "%Y%m%d")
        .map_err(|e| D::Error::custom(format!("Expected date in format %Y%m%d: {}", e)))
}

/* Some fields could possibly be some less heavy types, but as long as this is only
 * copying of data, it is possibly better to keep everything as strings so no
 * serialization/deserialization is needed (excepts fields used in some tranformations) */

#[derive(Deserialize)]
struct Input {
    city_code: String,
    hotel_code: String,
    room_type: String,
    room_code: String,
    meal: String,
    #[serde(deserialize_with = "deserialize_checkin")]
    checkin: chrono::NaiveDate,
    adults: u32,
    children: u32,
    price: f32,
    source: String,
}

#[derive(Deserialize)]
struct Hotel {
    id: String,
    name: String,
    category: f32,
    city: String,
}

#[derive(Deserialize)]
struct RoomName {
    hotel_code: String,
    source: String,
    room_name: String,
    room_code: String,
}

// It would be better to do some tricks with Cow Strings, to avoid copying, but this is this way
// just because of simplicity unless this is serious, profiled issue
#[derive(Hash, PartialEq, Eq, Clone)]
struct RoomKey {
    hotel_code: String,
    source: String,
    room_code: String,
}

#[derive(Serialize)]
struct Output {
    #[serde(rename = "room_type meal")]
    room_type_with_meal: String, // Probably better split those and create custom serialized, but this is way simpler
    room_code: String,
    source: String,
    hotel_name: String,
    city_name: String,
    city_code: String,
    hotel_category: String, // Could be f32, but this way is easier to handle proper precision
    pax: u32,
    adults: u32,
    children: u32,
    room_name: String,
    checkin: chrono::NaiveDate,
    checkout: chrono::NaiveDate,
    #[serde(serialize_with = "serialize_price")]
    price: f32,
}

impl Output {
    fn new(input: Input, hotel: &Hotel, room_name: String) -> Self {
        Output {
            room_type_with_meal: format!("{} {}", input.room_type, input.meal),
            room_code: input.room_code,
            source: input.source,
            hotel_name: hotel.name.clone(),
            city_name: hotel.city.clone(),
            city_code: input.city_code,
            hotel_category: format!("{:.1}", hotel.category),
            pax: input.adults + input.children,
            adults: input.adults,
            children: input.children,
            room_name: room_name,
            checkin: input.checkin,
            checkout: input.checkin.succ(),
            price: input.price / (input.adults + input.children) as f32,
        }
    }
}

impl Config {
    fn parse() -> Self {
        let mut input_path: String = "input.csv".into();
        let mut output_path: String = "output.csv".into();
        let mut hotels_path: String = "hotels.json".into();
        let mut rooms_path: String = "room_names.csv".into();
        {
            use argparse::{ArgumentParser, Store};

            let mut ap = ArgumentParser::new();
            ap.set_description("Enriches checking book");

            ap.refer(&mut input_path)
                .add_option(&["-i", "--input"], Store, "Path to input file");
            ap.refer(&mut output_path).add_option(
                &["-o", "--output"],
                Store,
                "Path to output file",
            );
            ap.refer(&mut hotels_path).add_option(
                &["-t", "--hotels"],
                Store,
                "Path to addidional hotels info file",
            );
            ap.refer(&mut rooms_path).add_option(
                &["-r", "--rooms"],
                Store,
                "Path to additional rooms info file",
            );

            ap.parse_args_or_exit();
        }

        Config {
            input_path,
            output_path,
            hotels_path,
            rooms_path,
        }
    }
}

fn prepare_hotels<R>(read: R) -> HashMap<String, Hotel>
where
    R: std::io::Read,
{
    use std::io::BufRead;

    let read = std::io::BufReader::new(read);
    read.lines()
        .filter_map(|line| {
            line.map_err(|e| println!("While reading hotel file: {}", e))
                .ok()
        })
        .filter_map(|line| {
            serde_json::from_str(&line)
                .map_err(|e| println!("Ignoring invalid hotel entry: {} ({})", &line, e))
                .ok()
        })
        .map(|item: Hotel| (item.id.clone(), item))
        .collect()
}

fn prepare_room_names<R>(read: R) -> HashMap<RoomKey, String>
where
    R: std::io::Read,
{
    let reader = csv::ReaderBuilder::new()
        .delimiter(b'|')
        .has_headers(false)
        .from_reader(read);

    reader
        .into_deserialize::<RoomName>()
        .filter_map(|input| {
            input
                .map_err(|e| println!("Ignoring invalid line: {}", e))
                .ok()
        })
        .map(|item| {
            let key = RoomKey {
                hotel_code: item.hotel_code,
                source: item.source,
                room_code: item.room_code,
            };
            (key, item.room_name)
        })
        .collect()
}

fn process_input<R>(
    read: R,
    hotels: HashMap<String, Hotel>,
    room_names: HashMap<RoomKey, String>,
) -> impl Iterator<Item = Output>
where
    R: std::io::Read,
{
    // csv::Reader is internally buffered so it's safe even for big inputs
    let reader = csv::ReaderBuilder::new().delimiter(b'|').from_reader(read);

    reader
        .into_deserialize::<Input>()
        .filter_map(|input| {
            input
                .map_err(|e| println!("Ignoring invalid line: {}", e))
                .ok()
        })
        .filter_map(move |item| {
            let hotel = hotels.get(&item.hotel_code).or_else(|| {
                println!("No hotel with id {}, ignoring entry", item.hotel_code);
                None
            });

            let room_key = RoomKey {
                hotel_code: item.hotel_code.clone(),
                source: item.source.clone(),
                room_code: item.room_code.clone(),
            };
            let room = room_names.get(&room_key).or_else(|| {
                println!(
                    "No room with id {}/{}, ignoring entry",
                    item.hotel_code, item.room_code
                );
                None
            });

            hotel.and_then(move |hotel| room.map(move |room| (hotel, room))) // Just zipping Options
                .map(move |(hotel, room)| Output::new(item, hotel, room.clone()))
        })
}

fn store_output<W, I>(write: W, iter: I)
where
    W: std::io::Write,
    I: IntoIterator<Item = Output>,
{
    // csv::Writer is internally buffered so it's safe even for big outputs
    let mut writer = csv::WriterBuilder::new().delimiter(b';').from_writer(write);

    for item in iter {
        writer
            .serialize(item)
            .unwrap_or_else(|e| println!("Cannot serialize item: {}", e));
    }

    writer
        .into_inner()
        .map_err(|e| {
            println!(
                "Cannot flush file, output may be incomplete or corrupted: {}",
                e
            )
        })
        .ok();
}

fn main() {
    let config = Config::parse();

    let input_file = std::fs::File::open(&config.input_path).expect("Cannot open input file");
    let output_file = std::fs::File::create(&config.output_path).expect("Cannot open output file");
    let hotels_file = std::fs::File::open(&config.hotels_path).expect("Cannot open hotels file");
    let rooms_file = std::fs::File::open(&config.rooms_path).expect("Cannto opent rooms file");

    let hotels = prepare_hotels(hotels_file);
    let rooms = prepare_room_names(rooms_file);
    let processed = process_input(input_file, hotels, rooms);
    store_output(output_file, processed);
}
