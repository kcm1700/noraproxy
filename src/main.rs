extern crate irc;
extern crate clap;
extern crate redis;


use std::process::exit;
use std::thread;
use std::time::Duration;
use std::str::FromStr;
use clap::{Arg, App};
use irc::client::prelude::*;
use redis::Commands;

#[derive(Clone, Debug)]
struct AppConfig {
    redis_address: String,
    history_count: isize
}

fn irc_write_loop(server: &IrcServer, config: &AppConfig) {
    let redis_client = match redis::Client::open(&*config.redis_address) {
        Ok(x) => x,
        Err(x) => {
            println!("Panic redis client {:?}", x);
            exit(2);
        }
    };
    let conn = match redis_client.get_connection() {
        Ok(x) => x,
        Err(x) => {
            println!("Panic redis connection {:?}", x);
            exit(2);
        }
    };
    loop {
        let pop_result : Result<Vec<String>, _> = conn.blpop("irc-write", 10usize);
        if let Ok(pop_result) = pop_result {
            if pop_result.len() == 2 {
                let value = pop_result[1].clone();
                let value = if value.ends_with("\r\n") { value } else { value + "\r\n" };
                let parsed = Message::from_str(&value);
                println!("received write message {:?}  --->> {:?}", value, parsed);
                if let Ok(message) = parsed {
                    match server.send(message) {
                        Ok(_) => (),
                        Err(x) => println!("[redis->irc] sending command failed {:?}", x)
                    }
                    /* flood protection: sleep for a second */
                    thread::sleep(Duration::new(1, 0));
                } else if let Err(e) = parsed {
                    println!("[redis->irc] irc-write parse error {:?}", e);
                }
            }
        }
    }
}

fn process_irc_message(message: &Message, redis_conn: &redis::Connection, config: &AppConfig) {
    let irc_command: String = message.to_string();
    /* increment counter */
    let new_count = redis_conn.incr("irc-read-cnt", 1).unwrap_or(0i64);
    /* append information */
    redis_conn.rpush("irc-read", format!("{} {}",new_count, irc_command)).unwrap_or(());
    /* preserve recent 100 logs */
    redis_conn.ltrim("irc-read", -config.history_count, -1).unwrap_or(());
    /* publish read information */
    redis::cmd("PUBLISH").arg("irc-read").arg(new_count).execute(redis_conn);
}

fn irc_read_loop(server: &IrcServer, config: &AppConfig) {
    let redis_client = match redis::Client::open(&*config.redis_address) {
        Ok(x) => x,
        Err(x) => {
            println!("Panic redis client {:?}", x);
            exit(2);
        }
    };
    let redis_connection = match redis_client.get_connection() {
        Ok(x) => x,
        Err(x) => {
            println!("Panic redis connection {:?}", x);
            exit(2);
        }
    };
    loop {
        for wrapped_message in server.iter() {
            match wrapped_message {
                Ok(ref message) => process_irc_message(message, &redis_connection, config),
                Err(e) => println!("[Error] {:?}", e)
            }
        }

        /* unexpected EOF */
        println!("connection closed by server.");
        println!("waiting before reconnection");
        thread::sleep(Duration::new(60, 0));
        println!("trying to reconnect");
        let _ = server.reconnect().and_then(|_| server.identify());
    }
}

fn main() {
    let arg_matches = App::new("noraproxy")
                    .arg(Arg::with_name("config")
                        .short("c")
                        .long("config")
                        .value_name("CONFIG")
                        .help("Sets the config file for irc [default: config.json]")
                        .takes_value(true))
                    .arg(Arg::with_name("redis")
                        .short("r")
                        .long("redis")
                        .value_name("REDIS")
                        .help("Specify redis address [default: redis://localhost/1]")
                        .takes_value(true))
                    .arg(Arg::with_name("history")
                        .short("h")
                        .long("history-length")
                        .value_name("history")
                        .help("number of lines to be saved in the redis db [default: 1000]")
                        .takes_value(true))
                    .get_matches();
    let config_path = arg_matches.value_of("config").unwrap_or("config.json");
    let redis_addr: String = arg_matches.value_of("redis").unwrap_or("redis://localhost/1").to_string();
    let app_config = AppConfig {
        redis_address: redis_addr,
        history_count: 1000
    };
    let config = match Config::load(config_path) {
        Ok(x) => x,
        Err(x) => {
            println!("loading config file {path} failed: {err:?}", path = config_path, err = x);
            exit(1);
        }
    };
    let server = match IrcServer::from_config(config) {
        Ok(x) => x,
        Err(x) => {
            println!("failed creating irc connection: {err:?}", err = x);
            exit(1);
        }
    };

    match server.identify() {
        Err(x) => {
            println!("failed identifying: {err:?}", err = x);
            exit(1);
        },
        _ => ()
    };

    let write_server = server.clone();
    let cloned_config = app_config.clone();
    let irc_send_thread = thread::spawn(move || {
        let server = write_server;
        irc_write_loop(&server, &cloned_config);
    });

    let read_server = server.clone();
    let cloned_config = app_config.clone();
    let irc_recv_thread = thread::spawn(move || {
        let server = read_server;
        irc_read_loop(&server, &cloned_config);
    });

    let _ = irc_send_thread.join();
    let _ = irc_recv_thread.join();
}

