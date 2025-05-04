// This mod allows me to perform graph operations as well as to process a dataframe into a graph, and a graph into a csv

pub type Vertex = usize;
pub type ListOfEdges = Vec<(Vertex, Vertex)>;
pub type AdjacencyLists = Vec<Vec<Vertex>>;
use crate::csv::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fs::File;
use std::io::Write;

// Create a graph that contains vertex labels and a list of adjacent edges
#[derive(Debug, Clone)]
pub struct Graph {
    pub n: usize, // Vertex labels in {0,...,n-1}
    pub outedges: AdjacencyLists,
}

// Reverse direction of edges on a list
// Takes as input a list of edges, and outputs a reversed list of edges
fn reverse_edges(list: &ListOfEdges) -> ListOfEdges {
    let mut new_list = vec![];
    for (u, v) in list {
        new_list.push((*v, *u));
    }
    new_list
}

impl Graph {
    // Add directed edges to a graph
    // Inputs: self and a list of edges, no outputs
    pub fn add_directed_edges(&mut self, edges: &ListOfEdges) {
        let mut seen = HashSet::new();
        for (u, v) in edges {
            if seen.insert((*u, *v)) {
                self.outedges[*u].push(*v);
            }
        }
    }

    // Sort the edges in a graph (and then remove duplicates)
    // Inputs: self, outputs: none
    pub fn sort_graph_lists(&mut self) {
        for l in self.outedges.iter_mut() {
            l.sort();
            l.dedup(); // Remove consecutive duplicates
        }
    }

    // Create a directed graph
    // Inputs: n (the number of vertices), a list of edges
    // Outputs: a graph
    pub fn create_directed(n: usize, edges: &ListOfEdges) -> Graph {
        let mut g = Graph {
            n,
            outedges: vec![vec![]; n],
        };
        g.add_directed_edges(edges);
        g.sort_graph_lists();
        g
    }

    // Create an undirected graph, by creating a directed graph, and then a list of the reverse edges
    // (So now there are edges going both directions)
    // Inputs: n (the number of vertices), a list of edges
    // Outputs: a graph
    pub fn create_undirected(n: usize, edges: &ListOfEdges) -> Graph {
        let mut g = Self::create_directed(n, edges);
        g.add_directed_edges(&reverse_edges(edges));
        g.sort_graph_lists();
        g
    }

    // Implement bfs
    // Input: self
    // Outputs: a vector of tuples that contain start node, end node, and distance; average distance
    pub fn bfs(&self) -> (Vec<(usize, usize, u32)>, u32) {
        // Create an empty distances vector
        let mut distances = vec![];

        // For each actor (stored as a number), calculate the distance to that actor
        for actor in 0..self.n {
            // Initially all distances are none (unvisited)
            // Distance to self is 0
            let mut distance: Vec<Option<u32>> = vec![None; self.n];
            distance[actor] = Some(0);

            // Create an empty queue, and start from the actor
            let mut queue: VecDeque<Vertex> = VecDeque::new();
            queue.push_back(actor);

            // Pop the next vertex (v), and look at its neigbors (u)
            // If u hasn't been visited, assign it's distance as one more than the distance to v, enqueue it
            while let Some(v) = queue.pop_front() {
                for &u in &self.outedges[v] {
                    if distance[u].is_none() {
                        distance[u] = Some(distance[v].unwrap() + 1);
                        queue.push_back(u);
                    }
                }
            }

            // Iterate through all the distances, and if there's a distance to v, add that distance to distances
            for v in 0..self.n {
                if let Some(d) = distance[v] {
                    distances.push((actor, v, d));
                }
            }
        }

        // Calculate the average distance by returning a sum of all of the distances divided by the length of distances
        let total_distance: u32 = distances.iter().map(|&(_, _, d)| d).sum();
        let average_distance = if !distances.is_empty() {
            total_distance / distances.len() as u32
        } else {
            0
        };

        // Return a duple containing the distances vector and the average distance
        (distances, average_distance)
    }

    // Export my graph to a csv so it can be plotted
    // Inputs: self, path
    // Outputs: A result containing a boxed dynamic error
    pub fn export_to_csv(&self, path: &str) -> Result<(), Box<dyn Error>> {
        let mut file = File::create(path)?;
        writeln!(file, "source,target")?;
        for (src, targets) in self.outedges.iter().enumerate() {
            for &dst in targets {
                writeln!(file, "{},{}", src, dst)?;
            }
        }
        Ok(())
    }
}

// Store actors' connections as graph
// Takes as input a dataframe, outputs a hashmap containing an actor and their collaborators
pub fn connections(data: DataFrame) -> HashMap<ColumnVal, Vec<String>> {
    // Find the indices containing actors
    // In the case of imdb_top_1000.csv, where the label contains "star"
    let mut actor_indices = vec![];
    for (i, label) in data.labels.iter().enumerate() {
        if label.contains("Star") {
            actor_indices.push(i);
        }
    }

    // Create an empty hashmap
    let mut actors_hash: HashMap<ColumnVal, Vec<String>> = HashMap::new();

    // For each row in the table, get the actors in that row (based on actor_indices)
    // For each actor in a row, set that actor's collaborators to be all of the actors in that row
    // If the actor is not in hash_map, make them the key, and their collaborators the values
    // If that actor is already in hash_map, add the collaborators to the values corresponding to that actor
    for row in &data.table {
        let actors: Vec<_> = actor_indices.iter().map(|&i| &row[i]).collect();
        for (i, actor) in actors.iter().enumerate() {
            if let ColumnVal::One(_) = actor {
                let mut collaborators = vec![];
                for (j, other) in actors.iter().enumerate() {
                    if i != j {
                        if let ColumnVal::One(collab) = other {
                            collaborators.push(collab.clone());
                        }
                    }
                }
                actors_hash
                    .entry((*actor).clone())
                    .or_default()
                    .extend(collaborators);
            }
        }
    }

    // Temporarily store the collaborators as a hashset to remove duplicates, then add them back to the hashmap
    for (key, value) in actors_hash.clone() {
        let set: HashSet<String> = value.into_iter().collect();
        actors_hash.insert(key, set.into_iter().collect()); // Convert back to Vec<String>
    }

    actors_hash
}

// Turn the values in a hashmap into a graph
// Input: a hashmap (designed for actors_hash)
// Output: a graph
pub fn hash_graph(hash: HashMap<ColumnVal, Vec<String>>) -> Graph {
    let mut connections: ListOfEdges = vec![]; // To store actor's connections as indices
    let mut actor_to_index: HashMap<String, usize> = HashMap::new();
    let mut index = 0;

    // Iterate through the actors and store them as indices, so they can be processed as a graph
    for actor in hash.keys() {
        actor_to_index.entry(actor.to_string()).or_insert_with(|| {
            let current_index = index;
            index += 1;
            current_index
        });
    }

    // Then store each actor's friend as the correct index
    for (actor, friends) in &hash {
        if let Some(&actor_idx) = actor_to_index.get(&actor.to_string()) {
            for friend in friends {
                if let Some(&friend_idx) = actor_to_index.get(friend) {
                    connections.push((actor_idx, friend_idx));
                }
            }
        }
    }

    // Create an undirected graph with size of the hashmap's length, and edges as connections
    let actors_graph = Graph::create_undirected(actor_to_index.len(), &connections);

    return actors_graph;
}
