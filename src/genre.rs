// This mod allows me to sort the actors by genre, and then perform a bfs on the actors in a given genre

use crate::csv::*;
use crate::graph::*;
use std::collections::{HashMap, HashSet};

// Store all of the actors in each genre
// Input: a dataframe
// Output: a hashmap contaning genres and all of the actors in that genre
pub fn genre(data: &DataFrame) -> HashMap<String, Vec<ColumnVal>> {
    let mut genres_hash: HashMap<String, HashSet<String>> = HashMap::new();

    // Get genre column index
    let genre_index = data
        .labels
        .iter()
        .position(|label| label == "Genre")
        .expect("Genre column not found");

    // Iterate over each row
    for row in &data.table {
        let genre_cell = &row[genre_index];
        let genre_string = genre_cell.to_string();
        let genres = genre_string
            .split(',')
            .map(|g| g.trim().to_lowercase());

        // Iterate over all actors in this row, and add them to the list of actors in that genre
        for (i, label) in data.labels.iter().enumerate() {
            if label.contains("Star") {
                if let ColumnVal::One(actor_name) = &row[i] {
                    if !actor_name.trim().is_empty() {
                        for genre in genres.clone() {
                            genres_hash
                                .entry(genre)
                                .or_insert_with(HashSet::new)
                                .insert(actor_name.clone());
                        }
                    }
                }
            }
        }
    }

    // Convert the HashSet to a  vector of columnvals
    genres_hash
        .into_iter()
        .map(|(genre, actors)| {
            let vals = actors.into_iter().map(ColumnVal::One).collect();
            (genre, vals)
        })
        .collect()
}

// Creates a bfs for actors in a specfic genre
// Inputs: a dataframe, and a hashmap of actors and their connections
// Outputs explained individually
pub fn genres_bfs(
    data: DataFrame,
    hash: HashMap<ColumnVal, Vec<String>>,
) -> HashMap<
    String, //The name of a genre
    (
        HashMap<
            ColumnVal, // The name of an actor
            Vec<String>,
        >, // That actor's friends
        Graph,                    // A graph for that genre
        Vec<(usize, usize, u32)>, // A vector containing tuples with the start node, end node, and distance b/w them
        u32,                      // The average distance between actors in that genre
    ),
> {
    // Create an empty hashmap
    let mut genres_meta_hash = HashMap::new();

    // For every genre and its actors, calculate that genre's bfs
    for (genre, actors) in genre(&data) {
        let mut genre_hash = HashMap::new();

        for actor in actors {
            if let Some(actor_connections) = hash.get(&actor) {
                genre_hash.insert(actor.clone(), actor_connections.clone());
            }
        }

        let genre_graph = crate::graph::hash_graph(genre_hash.clone());
        let (genre_bfs, avg_distance) = genre_graph.bfs();
        genres_meta_hash.insert(genre, (genre_hash, genre_graph, genre_bfs, avg_distance));
    }

    genres_meta_hash
}
