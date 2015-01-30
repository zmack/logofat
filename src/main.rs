use std::io::File;
use std::io::BufferedReader;
use std::string::String;
use std::str::FromStr;
use std::collections::{HashMap, BTreeMap};
use std::collections::btree_map::Entry;
use std::str::from_utf8;

#[deriving(Clone,Show)]
struct Event {
    pid: u64,
    db_time: u64,
    render_time: u64,
    total_time: u64,
    status: u16
}

fn main() {
    const BUF_SIZE:usize = 200 << 20;
    let file = File::open(&Path::new("short-prod-log"));
    let mut buffer = BufferedReader::with_capacity(BUF_SIZE, file);

    let mut lines:u64 = 0;
    let mut last_newline = 0;
    let mut remainder:Vec<u8> = Vec::new();
    let mut buffer_length:usize;
    let mut pids_in_flux: HashMap<u64, String> = HashMap::with_capacity(100);
    let mut events: BTreeMap<String, Vec<Event>> = BTreeMap::new();
    loop {
        {
            let result = buffer.fill_buf();
            let buf = match result {
                Err(_) => break,
                Ok(a) => a
            };
            println!("New Buffer -> {}", buf.len());
            buffer_length = buf.len();
            for index in range(0, buffer_length) {
                if buf[index] == 10 {
                    lines += 1;
                    if last_newline == 0 {
                        remainder.push_all(&buf[last_newline..index]);
                        do_line_things(from_utf8(remainder.as_slice()).unwrap(), &mut pids_in_flux, &mut events);
                    } else {
                        do_line_things(from_utf8(&buf[last_newline..index]).unwrap(), &mut pids_in_flux, &mut events);
                    }
                    last_newline = index + 1;

                    if lines % 100_000 == 0 {
                        println!("Completed {} lines ( {} )", lines, (lines * 100) / 62543690 );
                    }
                }
            }
            remainder = Vec::new();
            remainder.push_all(&buf[last_newline..buffer_length]);
        }
        buffer.consume(buffer_length);
        last_newline = 0;
    }

    for action in events.keys() {
        println!("{}", action);
    }
}

fn do_line_things(slice: &str, pids_in_flux: &mut HashMap<u64, String>, events: &mut BTreeMap<String, Vec<Event>>) {
    let mut i:usize;
    let mut initial:usize;
    let mut controller:String = "".to_string();

    if slice.len() < 30 {
        return;
    }

    i = 20;

    if slice.char_at(i) != '[' {
        return;
    }

    while slice.char_at(i) != ']' {
        i += 1;
    }

    let pid:u64 = FromStr::from_str(slice.slice_chars(21, i)).unwrap();

    match pids_in_flux.get(&pid) {
        Some(s) => { controller = s.clone() },
        None => {}
    }

    match slice.slice_chars(i + 2, i + 11) {
        "Completed" => {},
        "Processin" => {
            initial = i + 16;
            i = initial + 1;
            while slice.char_at(i) != ' ' {
                i = i + 1;
            }

            let controller = slice.slice_chars(initial, i).to_string();
            pids_in_flux.insert(pid, controller);

            return
        },
        _ => return
    }

    pids_in_flux.remove(&pid);
    let status:u16 = match FromStr::from_str(slice.slice_chars(i + 12, i + 15)) {
        Some(status) => status,
        None => { println!("Missed {:?}", slice.slice_chars(i + 12, i + 15)); 0u16 }
    };

    i = i + 16;
    while slice.slice_chars(i-1, i+1) != "in" {
        i = i + 1;
    }

    initial = i + 2;
    i = initial;

    while slice.char_at(i) != '.' {
        i = i + 1;
    }

    let time_taken:u64 = match FromStr::from_str(slice.slice_chars(initial, i)) {
        Some(time_taken) => time_taken,
        None => { println!("Missed {:?}", slice.slice_chars(initial, i)); 0 }
    };

    let mut view_time:u64 = 0;

    if slice.len() < i + 6 {
        return;
    }

    if slice.char_at(i+6) == 'V' {
        initial = i + 13;
        i = initial;

        while slice.char_at(i) != '.' {
            i = i + 1;
        }

        view_time = match FromStr::from_str(slice.slice_chars(initial, i)) {
            Some(view_time) => view_time,
            None => { println!("Missed view_time {:?}", slice.slice_chars(initial, i)); 0 }
        };

        initial = i + 21;
        i = initial;
    } else {
        initial = i + 20;
        i = initial;
    }

    if slice.len() < i {
        return;
    }

    while slice.char_at(i) != '.' {
        i = i + 1;
    }

    let db_time:u64 = match FromStr::from_str(slice.slice_chars(initial, i)) {
        Some(view_time) => view_time,
        None => { println!("Missed db_time {:?}", slice.slice_chars(initial, i)); 0 }
    };

    let event = Event {
        pid: pid,
        status: status,
        total_time: time_taken,
        render_time: view_time,
        db_time: db_time
    };

    match events.entry(controller) {
        Entry::Occupied(mut view) => {
            let vec = view.get_mut();
            vec.push(event);
        },
        Entry::Vacant(view) => {
            view.insert(vec!(event));
        }
    }

    // println!("Completed! {} {} {} {} {} {}", controller, pid, status, time_taken, view_time, db_time);
}

fn main2() {
    const BUF_SIZE:usize = 100 << 20;
    let file = File::open(&Path::new("prod-log"));
    let mut pids_in_flux: HashMap<u64, String> = HashMap::with_capacity(100);

    let mut buffer = BufferedReader::with_capacity(BUF_SIZE, file);
    
    println!("Starting {}", BUF_SIZE);
    let mut i:usize;
    let mut initial:usize;
    let mut controller:String = "".to_string();
    let mut lines = 0;

    for line_result in buffer.lines() {
        lines += 1;
        let line = line_result.unwrap();

        if line.len() < 30 {
            continue;
        }

        let slice = line.as_slice();

        i = 20;

        if slice.char_at(i) != '[' {
            continue;
        }

        while slice.char_at(i) != ']' {
            i += 1;
        }

        let pid:u64 = FromStr::from_str(slice.slice_chars(21, i)).unwrap();

        match pids_in_flux.get(&pid) {
            Some(s) => { controller = s.clone() },
            None => {}
        }

        match slice.slice_chars(i + 2, i + 11) {
            "Completed" => {},
            "Processin" => {
                initial = i + 16;
                i = initial + 1;
                while slice.char_at(i) != ' ' {
                    i = i + 1;
                }

                let controller = slice.slice_chars(initial, i).to_string();
                pids_in_flux.insert(pid, controller);
                
                continue
            },
            _ => continue
        }

        pids_in_flux.remove(&pid);
        let status:u16 = match FromStr::from_str(slice.slice_chars(i + 12, i + 15)) {
            Some(status) => status,
            None => { println!("Missed {:?}", slice.slice_chars(i + 12, i + 15)); 0u16 }
        };

        i = i + 16;
        while slice.slice_chars(i-1, i+1) != "in" {
            i = i + 1;
        }

        initial = i + 2;
        i = initial;

        while slice.char_at(i) != '.' {
            i = i + 1;
        }

        let time_taken:u16 = match FromStr::from_str(slice.slice_chars(initial, i)) {
            Some(time_taken) => time_taken,
            None => { println!("Missed {:?}", slice.slice_chars(initial, i)); 0u16 }
        };

        let mut view_time:u16 = 0;

        if slice.len() < i + 6 {
            continue;
        }

        if slice.char_at(i+6) == 'V' {
            initial = i + 13;
            i = initial;

            while slice.char_at(i) != '.' {
                i = i + 1;
            }

            view_time = match FromStr::from_str(slice.slice_chars(initial, i)) {
                Some(view_time) => view_time,
                None => { println!("Missed view_time {:?}", slice.slice_chars(initial, i)); 0u16 }
            };

            initial = i + 21;
            i = initial;
        } else {
            initial = i + 20;
            i = initial;
        }

        if slice.len() < i {
            continue;
        }

        while slice.char_at(i) != '.' {
            i = i + 1;
        }

        let db_time:u16 = match FromStr::from_str(slice.slice_chars(initial, i)) {
            Some(view_time) => view_time,
            None => { println!("Missed db_time {:?}", slice.slice_chars(initial, i)); 0u16 }
        };


        // println!("Completed! {} {} {} {} {} {}", controller, pid, status, time_taken, view_time, db_time);
        if lines % 10_000 == 0 {
            println!("Completed {} lines", lines);
        }
    }

    println!("Done!");

}
