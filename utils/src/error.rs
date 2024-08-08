
#[derive(Debug)]
#[allow(dead_code)] //suppress incorrect field not read warnings (see https://github.com/rust-lang/rust/issues/123068)
pub enum Error {
    
    Dialoguer(dialoguer::Error),
    Io(std::io::Error),
    Osmquadtree(osmquadtree::utils::Error),
    OutputFileExists(std::string::String),
    InvalidInputError(std::string::String),
    GetStateError(std::string::String),
    
}

impl std::error::Error for Error {}

impl std::convert::From<dialoguer::Error> for Error {
    fn from(e: dialoguer::Error) -> Self {
        Error::Dialoguer(e)
    }
}

impl std::convert::From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl std::convert::From<osmquadtree::utils::Error> for Error {
    fn from(e: osmquadtree::utils::Error) -> Self {
        Error::Osmquadtree(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}


pub type Result<T> = std::result::Result<T, Error>;

