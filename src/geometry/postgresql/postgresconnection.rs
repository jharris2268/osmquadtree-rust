use postgres::{Client,NoTls};
use std::io::{Error,ErrorKind};
struct Connection {
    client: Client,
}

fn wrap_postgres_error<T>(x: Result<T,postgres::Error>) -> std::io::Result<T> {
    match x {
        Ok(t) => Ok(t),
        Err(e) => Err(Error::new(ErrorKind::Other, format!("{:?}",e)))
    }
}

impl Connection {
    pub fn connect(connstr: &str) -> Result<Connection> {
        
        Ok(Connection{client: wrap_postgres_error(Client::connect(connstr, NoTls))?})
    }
    
    pub fn execute(&mut self, sql: &str) -> std::io::Result<()> {
        
        wrap_postgres_error(self.client.execute(sql, &[]))?;
        Ok(())
    }
    
    pub fn copy(&mut self, cmd: &str, data: &[&[u8]]) -> std::io::Result<()> {
        
        let mut writer = wrap_postgres_error(self.client.copy_in(cmd))?;
        
        for d in data {
            writer.write_all(d)?;
        }
        wrap_postgres_error(writer.finish())?;
        Ok(())
            
    }
}
