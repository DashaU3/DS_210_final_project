mod age;
mod csv;
mod genre;
mod graph;

use crate::age::*;
use crate::csv::*;
use crate::genre::*;
use crate::graph::*;
use std::collections::HashMap;
use std::io;

// Read the csvs
// Print out the average number of connections, as well as the average for a user-inputted age-bracket and genre
// No inputs or outputs, just print statements
// Also I exported actors_graph as a csv, which I then plotted in google colab
fn main() {
    // Read the combined csv
    let mut combined = DataFrame::new();
    combined.read_csv("combined.csv", &[1, 2, 2, 1, 1]).unwrap();

    // Read the top_1000 csv
    let mut top_1000 = DataFrame::new();
    top_1000
        .read_csv(
            "imdb_top_1000.csv",
            &[1, 1, 2, 1, 4, 1, 3, 1, 2, 1, 1, 1, 1, 1, 2, 1],
        )
        .unwrap();

    // Calculate the average number of connections between all of the actors in the top_1000 csv
    let actors_hash = connections(top_1000.clone());
    let actors_graph = hash_graph(actors_hash.clone());
    let average_connections = actors_graph.bfs().1;
    println!(
        "The average number of connections between actors is: {}",
        average_connections
    );
    &actors_graph.export_to_csv("actors_graph.csv"); //Export my graph as a csv

    // Use the ages_bfs function on combined and a hash_map containing all of the actors
    // More detail in age.rs
    let ages_bfs = ages_bfs(combined.clone(), actors_hash.clone());

    // Get user to input a number from 1 to 4, stored as age_bracket
    let mut age_bracket = String::new();
    println!("Please enter a number from 1 to 4");
    io::stdin()
        .read_line(&mut age_bracket)
        .expect("Failed to read line");
    let age_bracket: u32 = age_bracket.trim().parse().expect("Value is not a number");

    // Print out the age_range for actors in the age bracket, as well as their average number of connections to each other
    match age_bracket {
        1 => {
            println!(
                "The youngest actors are between {} and {}, and have {} connections to each other on average.",
                ages_bfs.8.0, ages_bfs.8.1, ages_bfs.4.1
            );
        }
        2 => {
            println!(
                "The second youngest actors are between {} and {}, and have {} connections to each other on average.",
                ages_bfs.9.0, ages_bfs.9.1, ages_bfs.5.1
            );
        }
        3 => {
            println!(
                "The second oldest actors are between {} and {}, and have {} connections to each other on average.",
                ages_bfs.10.0, ages_bfs.10.1, ages_bfs.6.1
            );
        }
        4 => {
            println!(
                "The oldest actors are between {} and {}, and have {} connections to each other on average.",
                ages_bfs.11.0, ages_bfs.11.1, ages_bfs.7.1
            );
        }
        _ => println!("Invalid input. Please enter a number between 1 and 4."),
    }

    // Use the genres_bfs function on top_1000 and a hash containing all of the actors
    // More detail in genre.rs
    let genres_bfs_map: HashMap<
        String,
        (
            HashMap<ColumnVal, Vec<String>>,
            Graph,
            Vec<(usize, usize, u32)>,
            u32,
        ),
    > = genres_bfs(top_1000, actors_hash.clone());

    // Ask user to enter a genre and print the average number of connections in that genre
    let mut genre = String::new();
    println!("Please enter a genre");
    io::stdin()
        .read_line(&mut genre)
        .expect("Failed to read line");
    let genre = genre.trim().to_lowercase();
    if let Some(genre_data) = genres_bfs_map.get(&genre) {
        let genre_average = genre_data.3;
        println!(
            "Actors in the {:?} genre have {:?} connections to each other on average",
            genre, genre_average
        );
    } else {
        println!("Genre not found.");
    }
}

// Test the code on a small csv where the BFS could be calculated by hand
#[test]
fn small_test() {
    let mut small = DataFrame::new();
    small
        .read_csv("small.csv", &[1, 1, 1, 1, 1, 1, 1, 1])
        .unwrap();
    let small_hash = connections(small.clone());
    let small_graph = hash_graph(small_hash.clone());
    let small_average = small_graph.bfs().1;
    assert_eq!(small_average, 1);
}

// Confirm that the average for an arbitrarily chosen age bracket is correct
// (In this case, the oldest age bracket)
#[test]
fn oldest_test() {
    let mut top_1000 = DataFrame::new();
    top_1000
        .read_csv(
            "imdb_top_1000.csv",
            &[1, 1, 2, 1, 4, 1, 3, 1, 2, 1, 1, 1, 1, 1, 2, 1],
        )
        .unwrap();

    let mut combined = DataFrame::new();
    combined.read_csv("combined.csv", &[1, 2, 2, 1, 1]).unwrap();
    let hash = connections(top_1000.clone());
    let result = ages_bfs(combined, hash);
    assert_eq!(result.7 .1, 5); // Check average BFS value for oldest group
}

// Confirm that the average for an arbitrarily chosen genre is correct
// (In this case, the horror genre)
#[test]
fn comedy_test() {
    let mut df = DataFrame::new();
    df.read_csv(
        "imdb_top_1000.csv",
        &[1, 1, 2, 1, 4, 1, 3, 1, 2, 1, 1, 1, 1, 1, 2, 1],
    )
    .unwrap();
    let hash = connections(df.clone());
    let genre_data = genres_bfs(df, hash);
    let comedy_data = genre_data.get("comedy").expect("No horror genre found");
    assert_eq!(comedy_data.3, 5);
}
