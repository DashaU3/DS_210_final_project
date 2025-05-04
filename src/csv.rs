// This mod allows me to process a csv as a dataframe, so that the values in it can be easily accessed

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};

// Allows each entry to be sorted into a columnval, and processed accordingly
// (Based on what that entry's type is)
#[derive(Clone, Debug)]
pub enum ColumnVal {
    One(String),
    Two(i64),
    Three(f64), // excluded from Eq/Hash
}

// Custom Eq and PartialEq — exclude `Three` b/c f64
impl PartialEq for ColumnVal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ColumnVal::One(a), ColumnVal::One(b)) => a == b,
            (ColumnVal::Two(a), ColumnVal::Two(b)) => a == b,
            _ => false, // Three or mixed types always false
        }
    }
}

impl Eq for ColumnVal {}

// Define how to order ColumnVals
// Currently only defined for two because that's all I needed (to sort the ages)
impl Ord for ColumnVal {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (ColumnVal::Two(a), ColumnVal::Two(b)) => a.cmp(b),
            _ => Ordering::Equal, // Define behavior for other variants if needed
        }
    }
}

// Likewise, define PartialOrd for ColumnVals
impl PartialOrd for ColumnVal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Define hash for columnvals - Three is excluded b/c f64s can't be hashed
impl Hash for ColumnVal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ColumnVal::One(s) => {
                1u8.hash(state);
                s.hash(state);
            }
            ColumnVal::Two(n) => {
                2u8.hash(state);
                n.hash(state);
            }
            ColumnVal::Three(_) => {
                panic!("Cannot hash ColumnVal::Three due to f64 non-hashability");
            }
        }
    }
}

// Allow me to print ColumnVals
impl fmt::Display for ColumnVal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnVal::One(val) => write!(f, "{}", val),
            ColumnVal::Two(val) => write!(f, "{}", val),
            ColumnVal::Three(val) => write!(f, "{}", val),
        }
    }
}

//Create a DataFrame struct which will allow me to store my data so that it is easy to access and manipulate
#[derive(Clone, Debug)]
pub struct DataFrame {
    pub labels: Vec<String>,
    pub table: Vec<Vec<ColumnVal>>,
    pub types: Vec<u32>,
}

// For returning errors
#[derive(Clone, Debug)]
pub struct MyError(String);

// Define how errors will be displayed
impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "There is an error: {}", self.0)
    }
}
impl Error for MyError {}

impl DataFrame {
    // Create a new empty dataframe, outputs Self
    // Contains a data table, a vector of that table's labels, and a vector containing its types
    pub fn new() -> Self {
        DataFrame {
            table: Vec::new(),
            labels: Vec::new(),
            types: Vec::new(),
        }
    }

    // Takes as input self, a path as a string, and a vector of types
    // Outputs a result containing a boxed dynamic error
    pub fn read_csv(&mut self, path: &str, types: &[u32]) -> Result<(), Box<dyn Error>> {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b',') // Commas are the delimiter
            .has_headers(true) // The CSV has headers, so we will read them
            .quote(b'"') // Ensures quoted fields are handled correctly
            .flexible(true)
            .from_path(path)?;

        self.types = types.to_vec();

        // Read column labels (headers)
        self.labels = rdr.headers()?.iter().map(|s| s.to_string()).collect();

        // If there's an error reading a line, skip it
        for result in rdr.records() {
            let r = match result {
                Ok(record) => record,
                Err(_) => continue,
            };

            // Create an empty row
            let mut row: Vec<ColumnVal> = vec![];

            // Based on the value in types, process each value in row as the appropriate ColumnVal
            for (i, elem) in r.iter().enumerate() {
                let cell = match types[i] {
                    // Convert the value to a string
                    1 => ColumnVal::One(elem.to_string()),
                    // Parse the value as an integer, or else replace it with 0
                    // (Don't want to skip the row because actors who are still living won't have death years)
                    2 => {
                        if elem.trim().is_empty() {
                            ColumnVal::Two(0)
                        } else {
                            match elem.parse::<i64>() {
                                Ok(parsed) => ColumnVal::Two(parsed),
                                Err(_) => ColumnVal::Two(0),
                            }
                        }
                    }
                    // Parse the value as f64 or else skip the row
                    3 => {
                        if elem.trim().is_empty() {
                            continue;
                        }
                        match elem.parse::<f64>() {
                            Ok(parsed) => ColumnVal::Three(parsed),
                            Err(_) => continue,
                        }
                    }

                    // Allows me to process runtime, turn it into ColumnVal of type Two
                    // Removes the last four characters and then parses the rest as i64
                    // If there's an error, replace the value with 0
                    4 => {
                        if elem.trim().is_empty() {
                            ColumnVal::Two(0)
                        } else {
                            match elem[..elem.len().saturating_sub(4)].parse::<i64>() {
                                Ok(parsed) => ColumnVal::Two(parsed),
                                Err(_) => ColumnVal::Two(0),
                            }
                        }
                    }
                    _ => continue,
                };
                // Add the processed value to the row vector
                row.push(cell);
            }

            //If no values in the row were skipped, push the row to self.table
            if row.len() == types.len() {
                self.table.push(row);
            }
        }
        Ok(())
    }

    // Get the values in a column from that column's name
    // Inputs: self and the column label (as a string)
    // Output: a result that contains a vector of that column's values and a boxed dynamic error
    pub fn get_column(&self, label: &str) -> Result<Vec<ColumnVal>, Box<dyn Error>> {
        let index = self
            .labels
            .iter()
            .position(|x| x == label)
            .ok_or_else(|| MyError(format!("Label {} not found", label)))?;
        Ok(self.table.iter().map(|row| row[index].clone()).collect())
    }
}
