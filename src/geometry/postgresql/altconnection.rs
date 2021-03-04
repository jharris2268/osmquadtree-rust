use pq_sys::{
    PGconn, /*PQerrorMessage,*/ PQclear, PQconnectdb, PQexec, PQfinish, PQresultStatus,
    PGRES_COMMAND_OK,
};
use pq_sys::{PQgetResult, PQputCopyData, PQputCopyEnd, PQstatus, CONNECTION_BAD, PGRES_COPY_IN};
use std::io::{Error, ErrorKind, Result};

use std::ffi::CString;
use std::ops::Drop;

/*
  size_t copy_func(const std::string& tab, const std::string& data) {
            if (!init) {
                conn = PQconnectdb(connection_string.c_str());
                if (!conn) {
                    Logger::Message() << "connection to postgresql failed [" << connection_string << "]";
                    throw std::domain_error("connection to postgressql failed");
                }
                auto res = PQexec(conn,"begin");
                if (PQresultStatus(res)!=PGRES_COMMAND_OK) {
                    Logger::Message() << "begin failed?? " <<  PQerrorMessage(conn);
                    PQclear(res);
                    PQfinish(conn);
                    throw std::domain_error("begin failed");
                    return 0;
                }
                PQclear(res);
                init=true;
            }


            std::string sql="COPY "+tab+" FROM STDIN";

            if (as_binary) {
                sql += " (FORMAT binary)";
            } else {
                sql += " csv QUOTE e'\x01' DELIMITER e'\x02'";
                if (with_header) {
                    sql += " HEADER";
                }
            }

            auto res = PQexec(conn,sql.c_str());

            if (PQresultStatus(res) != PGRES_COPY_IN) {
                Logger::Message() << "PQresultStatus != PGRES_COPY_IN [" << PQresultStatus(res) << "] " <<  PQerrorMessage(conn);
                Logger::Message() << sql;
                PQclear(res);
                PQfinish(conn);
                init=false;
                throw std::domain_error("PQresultStatus != PGRES_COPY_IN");
                return 0;
            }

            int r = PQputCopyData(conn,data.data(),data.size());
            if (r!=1) {
                Logger::Message() << "copy data failed {r=" << r<< "} [" << sql << "]" << PQerrorMessage(conn) << "\n" ;
                PQputCopyEnd(conn,nullptr);
                PQclear(res);
                return 0;
            }



            r = PQputCopyEnd(conn,nullptr);
            if (r!=PGRES_COMMAND_OK) {
                Logger::Message() << "\n*****\ncopy failed [" << sql << "]" << PQerrorMessage(conn) << "\n" ;

                return 0;
            }

            PQclear(res);

            res = PQgetResult(conn);
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                Logger::Message() << "copy end failed: " << PQerrorMessage(conn);
                throw std::domain_error("failed");
            }

            PQclear(res);
            return 1;
        }
        std::string connection_string;
        std::string table_prfx;
        bool with_header;
        bool as_binary;
        PGconn* conn;
        bool init;
*/

pub struct Connection {
    conn: *mut PGconn,
}
unsafe impl Send for Connection {}

impl Connection {
    pub fn connect(connstr: &str) -> Result<Connection> {
        let connstr_cstr = CString::new(connstr).expect("failed to read connstr");

        let conn;
        unsafe {
            conn = PQconnectdb(connstr_cstr.as_ptr());
            if PQstatus(conn) == CONNECTION_BAD {
                PQfinish(conn);
                return Err(Error::new(ErrorKind::Other, "failed to connect"));
            }
        }
        Ok(Connection { conn })
    }

    pub fn execute(&mut self, sql: &str) -> Result<()> {
        let sql_cstr = CString::new(sql).expect("failed to read sql str");
        unsafe {
            let res = PQexec(self.conn, sql_cstr.as_ptr());
            if PQresultStatus(res) != PGRES_COMMAND_OK {
                PQclear(res);
                return Err(Error::new(ErrorKind::Other, "failed to execute"));
            }
            PQclear(res);
            Ok(())
        }
    }

    pub fn copy(&mut self, cmd: &str, data: &[&[u8]]) -> Result<()> {
        let cmd_cstr = CString::new(cmd).expect("failed to read cmd str");

        unsafe {
            let res = PQexec(self.conn, cmd_cstr.as_ptr());

            if PQresultStatus(res) != PGRES_COPY_IN {
                PQclear(res);
                return Err(Error::new(ErrorKind::Other, "failed to copy"));
            }

            for data_part in data {
                let r = PQputCopyData(
                    self.conn,
                    data_part.as_ptr() as *mut i8,
                    data_part.len() as i32,
                );

                if r != 1 {
                    PQputCopyEnd(self.conn, std::ptr::null());
                    PQclear(res);
                    return Err(Error::new(ErrorKind::Other, "failed to copy"));
                }
            }

            let r = PQputCopyEnd(self.conn, std::ptr::null());
            if r != 1 {
                PQclear(res);
                return Err(Error::new(ErrorKind::Other, format!("failed to copy: ")));
                //{}", PQerrorMessage(self.conn))));
            }

            PQclear(res);

            let res = PQgetResult(self.conn);
            if PQresultStatus(res) != PGRES_COMMAND_OK {
                PQclear(res);
                return Err(Error::new(ErrorKind::Other, "copy end failed"));
            }
            PQclear(res);
        }
        Ok(())
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            PQfinish(self.conn);
        }
    }
}
