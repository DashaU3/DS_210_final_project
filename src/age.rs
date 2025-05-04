// This mod calculates the age of an actor, and then performs bfs on the actors divided by age brackets

use crate::csv::{ColumnVal, DataFrame}; // Your custom data structures
use crate::graph::Graph;
use chrono::{Datelike, Utc};
use std::collections::HashMap;

// Calculate the actor's age
pub fn age(data: DataFrame) -> Vec<(String, Option<ColumnVal>)> {
    let mut actors_and_ages = vec![];
    let current_year = Utc::now().year();

    let birth_years = data.get_column("birthYear").unwrap();
    let death_years = data.get_column("deathYear").unwrap();
    let actors = data.get_column("primaryName").unwrap();

    for ((birth_year_val, death_year_val), actor_val) in birth_years
        .iter()
        .zip(death_years.iter())
        .zip(actors.iter())
    {
        // Extract actor name from ColumnVal
        let actor = match actor_val {
            ColumnVal::One(name) => name,
            _ => continue, // Skip if actor name is not in expected format
        };

        if actor.trim().is_empty() {
            continue;
        }

        let age = match (birth_year_val, death_year_val) {
            (ColumnVal::Two(birth), ColumnVal::Two(death)) => {
                if *birth <= 1900 {
                    None
                } else if *death == 0 {
                    Some(ColumnVal::Two(current_year as i64 - *birth))
                } else if death > birth {
                    Some(ColumnVal::Two(*death - *birth))
                } else {
                    None
                }
            }
            _ => None,
        };

        actors_and_ages.push((actor.clone(), age)); // `actor` is a String now
    }

    actors_and_ages
}

// Extract integer value from the age tuple
// Input: Option<&(String, Option<ColumnVal>)>
// Output: A result containing an i32 and a boxed dynamic error
fn extract_val(
    opt: Option<&(String, Option<ColumnVal>)>,
) -> Result<i32, Box<dyn std::error::Error>> {
    match opt {
        Some((_, Some(ColumnVal::Two(val)))) => {
            if *val >= i32::MIN as i64 && *val <= i32::MAX as i64 {
                Ok(*val as i32)
            } else {
                Err("Value out of i32 range".into())
            }
        }
        _ => Err("Expected ColumnVal::Two or missing value".into()),
    }
}

// Perform BFS grouped by age quartiles
// Inputs: dataframe and hashmap of actors and ages
// Outputs: actor graphs, actors BFS results, tuples containing youngest and oldest actors in each bracket
pub fn ages_bfs(
    data: DataFrame,
    hash: HashMap<ColumnVal, Vec<String>>,
) -> (
    Graph,
    Graph,
    Graph,
    Graph,
    (Vec<(usize, usize, u32)>, u32),
    (Vec<(usize, usize, u32)>, u32),
    (Vec<(usize, usize, u32)>, u32),
    (Vec<(usize, usize, u32)>, u32),
    (i32, i32),
    (i32, i32),
    (i32, i32),
    (i32, i32),
) {
    let mut actors_and_ages = age(data);

    actors_and_ages = actors_and_ages
        .into_iter()
        .filter(|(_, maybe_age)| matches!(maybe_age, Some(ColumnVal::Two(val)) if *val > 0))
        .collect();

    actors_and_ages.sort_by(|a, b| match (&a.1, &b.1) {
        (Some(ColumnVal::Two(a_age)), Some(ColumnVal::Two(b_age))) => a_age.cmp(b_age),
        _ => std::cmp::Ordering::Equal,
    });

    let total = actors_and_ages.len();
    let q = total / 4;

    let youngest = &actors_and_ages[0..q];
    let second = &actors_and_ages[q..2 * q];
    let third = &actors_and_ages[2 * q..3 * q];
    let oldest = &actors_and_ages[3 * q..];


    // Builds connections for actors in each age bracket
    // Inputs: vector of actors in given age bracket, hashmap of all the actors and their ages
    // Outputs: hashmap of actors and their connections within a bracket
    fn build_connections(
        group: &[(String, Option<ColumnVal>)],
        all: &HashMap<ColumnVal, Vec<String>>,
    ) -> HashMap<ColumnVal, Vec<String>> {
        let mut result = HashMap::new();
        for (actor, _) in group {
            let key = ColumnVal::One(actor.clone());
            if let Some(connections) = all.get(&key) {
                result.insert(key, connections.clone());
            }
        }
        result
    }

    let youngest_graph = crate::graph::hash_graph(build_connections(youngest, &hash));
    let second_graph = crate::graph::hash_graph(build_connections(second, &hash));
    let third_graph = crate::graph::hash_graph(build_connections(third, &hash));
    let oldest_graph = crate::graph::hash_graph(build_connections(oldest, &hash));

    let youngest_bfs = youngest_graph.bfs();
    let second_bfs = second_graph.bfs();
    let third_bfs = third_graph.bfs();
    let oldest_bfs = oldest_graph.bfs();

    (
        youngest_graph,
        second_graph,
        third_graph,
        oldest_graph,
        youngest_bfs,
        second_bfs,
        third_bfs,
        oldest_bfs,
        (
            extract_val(youngest.first()).unwrap_or_default(), // for each, use extract_val to turn option into i32
            extract_val(youngest.last()).unwrap_or_default(),
        ),
        (
            extract_val(second.first()).unwrap_or_default(),
            extract_val(second.last()).unwrap_or_default(),
        ),
        (
            extract_val(third.first()).unwrap_or_default(),
            extract_val(third.last()).unwrap_or_default(),
        ),
        (
            extract_val(oldest.first()).unwrap_or_default(),
            extract_val(oldest.last()).unwrap_or_default(),
        ),
    )
}
