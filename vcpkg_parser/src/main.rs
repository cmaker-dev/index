use std::io::Write;

use reqwest::blocking::get;
use serde::{Deserialize, Serialize};
use clap::Parser;

#[derive(Serialize, Deserialize, Debug)]
#[allow(non_snake_case)]
struct Item {
    Name: String,
    Homepage: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[allow(non_snake_case)]
struct Obj {
    Baseline: String,
    Size: usize,
    Source: Vec<Item>,
}

#[derive(Parser,Debug)]
#[command(author = "Brainfart", version, about, long_about = None)]
struct Args {
    /// Output filename
    #[arg(short, long)]
    out_file: Option<String>,
}


fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args = Args::parse();

    // Parse the file name or default to sample.json
    let mut output_file_name = String::from("../index.json");
    if let Some(out_name) = args.out_file {
        output_file_name = out_name
    }

    // Request json file 
    println!("[#] Downloading output.json");
    let resp: Obj = get("https://vcpkg.io/output.json")?.json()?;
    println!("[#] Download complete");
    let mut filtered = Vec::new();

    // filter responce if Homepage exists and is a github link

    println!("[#] Sorting...");
    for item in resp.Source.iter() {

        println!("[#] Checking: {:?}",item);
        if let Some(val) = item.Homepage.clone() {
            if val.contains("github") {
                filtered.push(item);
            }
        }
    }
//jump back a directory and create a folder with filtered Name and save a json with Name,
    //Homepage in the directory
    for item in filtered.iter() {
        let mut path = String::from("../");
        path.push_str(&item.Name);
        std::fs::create_dir_all(path.clone())?;
        path.push_str("/info.json");
        let mut file = std::fs::File::create(path)?;
        let string = serde_json::to_string(item)?;
        file.write_all(string.as_bytes())?;
    }

    // stringify json and write to file
    println!("[#] Writing to file: {}",output_file_name);
    if let Ok(string) = serde_json::to_string(&filtered) {
        match std::fs::write(output_file_name, string) {
            Ok(_) => {
                println!("[#] File Succesfully written");
            }
            Err(e) => {
                println!("[!] Could not write file for some reason.\n {}",e);
            }
        }
    }

    Ok(())
}
