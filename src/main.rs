extern crate argparse;
#[macro_use]
extern crate serde_derive;
extern crate csv;

#[derive(Debug)]
struct Config {
    input_path: String,
    output_path: String,
    hotels_path: String,
}

/* Some fields could possibly be some less heavy types, but as long as this is only
 * copying of data, it is possibly better to keep everything as strings so no
 * serialization/deserialization is needed (excepts fields used in some tranformations) */

#[derive(Debug, Deserialize)]
struct Input {
    city_code: String,
    hotel_code: String,
    room_type: String,
    room_code: String,
    meal: String,
    checkin: String,
    adults: String,
    children: String,
    price: String,
    source: String,
}

#[derive(Debug, Serialize)]
struct Output {
    #[serde(rename = "room_type meal")]
    room_type_with_meal: String, // Probably better split those and create custom serialized, but this is way simpler
    room_code: String,
    source: String,
    hotel_name: String,
    city_name: String,
    city_code: String,
    hotel_category: String,
    pax: String,
    adults: String,
    children: String,
    room_name: String,
    checkin: String,
    checkout: String,
    price: String,
}

impl Output {
    fn new(input: Input) -> Self {
        Output {
            room_type_with_meal: format!("{} {}", input.room_type, input.meal),
            room_code: input.room_code,
            source: input.source,
            hotel_name: "".into(), // TODO: find this
            city_name: "".into(), // TODO: find this
            city_code: input.city_code,
            hotel_category: "".into(), // TODO: find this
            pax: "".into(), // TODO: find this
            adults: input.adults,
            children: input.children,
            room_name: "".into(), // TODO: find this
            checkin: input.checkin,
            checkout: "".into(), // TODO: calculate this
            price: "".into(), // TODO: calculate this
        }
    }
}

impl Config {
    fn parse() -> Self {
        let mut input_path: String = "input.csv".into();
        let mut output_path: String = "output.csv".into();
        let mut hotels_path: String = "hotels.json".into();
        {
            use argparse::{ArgumentParser, Store};

            let mut ap = ArgumentParser::new();
            ap.set_description("Enriches checking book");

            ap.refer(&mut input_path)
                .add_option(&["-i", "--input"], Store, "Path to input file");
            ap.refer(&mut output_path)
                .add_option(&["-o", "--output"], Store, "Path to output file");
            ap.refer(&mut hotels_path)
                .add_option(&["-t", "--hotels"], Store, "Path to addidional hotels info file");

            ap.parse_args_or_exit();
        }

        Config {
            input_path,
            output_path,
            hotels_path,
        }
    }
}

fn process_input<R>(read: R) -> impl Iterator<Item=Output> where R: std::io::Read {
    // csv::Reader is internally buffered so it's safe even for big inputs
    let reader = csv::ReaderBuilder::new()
        .delimiter(b'|')
        .from_reader(read);

    reader
        .into_deserialize::<Input>()
        .filter_map(|input| {
            input.map_err(|e| println!("Ignoring invalid line: {}", e))
                .ok()
                .map(|item| {
                    println!("Input item: {:?}", item);
                    Output::new(item)
                })
        })
}

fn store_output<W, I>(write: W, iter: I) where W: std::io::Write, I: IntoIterator<Item=Output> {
    // csv::Writer is internally buffered so it's safe even for big outputs
    let mut writer = csv::WriterBuilder::new()
        .delimiter(b';')
        .from_writer(write);

    for item in iter {
        println!("Out item: {:?}", &item);
        writer.serialize(item)
            .unwrap_or_else(|e| println!("Cannot serialize item: {}", e));
    }

    writer.flush()
        .unwrap_or_else(|e| println!("Cannot flush file, output may be incomplete or corrupted: {}", e));
}

fn main() {
    let config = Config::parse();
    println!("Using config: {:?}", config);

    let input_file = std::fs::File::open(&config.input_path).expect("Cannot open input file");
    let output_file = std::fs::File::create(&config.output_path).expect("Cannot open output file");

    let processed = process_input(input_file);
    store_output(output_file, processed);
}
