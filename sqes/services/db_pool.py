import mysql.connector
import mysql.connector.pooling
import psycopg2.pool
import time
import logging

logger = logging.getLogger(__name__)

class DBPool(object):
    """
    A generic database connection pool class supporting both MySQL and PostgreSQL.
    """
    def __init__(self, db_type="mysql", host="127.0.0.1", port=None, user="root",
                 password="root", database="test", pool_name="db_pool",
                 pool_size=3, max_reconnect_attempts=3):
        self._db_type = db_type.lower()
        self._host = host
        self._user = user
        self._password = password
        self._database = database
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_attempts = 0
        self._pool_name = pool_name
        self._pool_size = int(pool_size) # Ensure pool_size is int
        self.dbconfig = {}
        self.pool = None
        
        try:
            if self._db_type == "mysql":
                self._port = port if port else "3306"
                self.dbconfig["host"] = self._host
                self.dbconfig["port"] = int(self._port)
                self.dbconfig["user"] = self._user
                self.dbconfig["password"] = self._password
                self.dbconfig["database"] = self._database
                self.pool = self._create_mysql_pool(pool_name=pool_name, pool_size=self._pool_size)
            elif self._db_type == "postgresql":
                self._port = port if port else "5432"
                self.dbconfig["host"] = self._host
                self.dbconfig["port"] = int(self._port)
                self.dbconfig["user"] = self._user
                self.dbconfig["password"] = self._password
                self.dbconfig["dbname"] = self._database
                self.pool = self._create_postgresql_pool(pool_name=pool_name, pool_size=self._pool_size)
            else:
                raise ValueError("Unsupported database type. Choose 'mysql' or 'postgresql'.")
        except Exception as e:
            logger.error(f"Error initializing DBPool for {self._db_type}: {e}", exc_info=True)
            self.pool = None

    def _create_mysql_pool(self, pool_name="sqes_pool", pool_size=3):
        try:
            pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name=pool_name,
                pool_size=pool_size,
                pool_reset_session=True,
                **self.dbconfig)
            logger.debug(f"MySQL pool '{pool_name}' created.")
            return pool
        except Exception as e:
            logger.error(f"Error creating MySQL pool: {e}", exc_info=True)
            return None

    def _create_postgresql_pool(self, pool_name="pg_pool", pool_size=3):
        try:
            pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=pool_size,
                maxconn=pool_size,
                **self.dbconfig
            )
            logger.debug(f"PostgreSQL pool created.")
            return pool
        except Exception as e:
            logger.error(f"Error creating PostgreSQL pool: {e}", exc_info=True)
            return None

    def close(self, conn, cursor):
        """Closes connection and cursor, returning connection to pool."""
        if cursor:
            cursor.close()
        if conn:
            if self.pool is not None:
                if self._db_type == "postgresql":
                    self.pool.putconn(conn) # type: ignore
                else: # mysql.connector
                    conn.close() # Returns to pool
            else:
                # If pool is None, just close the connection
                conn.close()

    def _get_connection_from_pool(self):
        if self.pool is None:
            raise ConnectionError(f"Database pool for {self._db_type} is not initialized.")
        
        if self._db_type == "mysql":
            return self.pool.get_connection() # type: ignore
        elif self._db_type == "postgresql":
            return self.pool.getconn()        # type: ignore
        else:
            raise ValueError(f"Unknown database type: {self._db_type}")

    def execute(self, sql, args=None, commit=False):
        """Executes a SQL query."""
        conn = None
        cursor = None
        try:
            conn = self._get_connection_from_pool()
            cursor = conn.cursor()
            
            if args:
                # Ensure args is a tuple for consistency
                args_to_execute = args if isinstance(args, (tuple, list)) else (args,)
                cursor.execute(sql, args_to_execute)
            else:
                cursor.execute(sql)
            
            if commit:
                conn.commit()
                self._reconnect_attempts = 0
                return None
            else:
                res = cursor.fetchall()
                self._reconnect_attempts = 0
                return res
        except (mysql.connector.Error, psycopg2.Error, ConnectionError) as e:
            logger.error(f"!! DBPool Error ({self._db_type}): {e}")
            return self.handle_error(conn, cursor, self.execute, sql, args=args, commit=commit)
        except Exception as e:
            logger.error(f"!! DBPool General Error: {e}", exc_info=True)
            return self.handle_error(conn, cursor, self.execute, sql, args=args, commit=commit)
        finally:
            self.close(conn, cursor)

    def executemany(self, sql, args, commit=False):
        """Executes a SQL query with executemany."""
        conn = None
        cursor = None
        try:
            conn = self._get_connection_from_pool()
            cursor = conn.cursor()
            
            cursor.executemany(sql, args)
            
            if commit:
                conn.commit()
                self._reconnect_attempts = 0
                return None
            else:
                res = cursor.fetchall()
                self._reconnect_attempts = 0
                return res
        except (mysql.connector.Error, psycopg2.Error, ConnectionError) as e:
            logger.error(f"!! DBPool Error ({self._db_type}): {e}")
            return self.handle_error(conn, cursor, self.executemany, sql, args=args, commit=commit)
        except Exception as e:
            logger.error(f"!! DBPool General Error: {e}", exc_info=True)
            return self.handle_error(conn, cursor, self.executemany, sql, args=args, commit=commit)
        finally:
            self.close(conn, cursor)

    def is_db_connected(self):
        """Checks if the database pool is functional."""
        conn = None
        cursor = None
        try:
            conn = self._get_connection_from_pool()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchall()
            logger.info(f"DB Pool Connection Check: OK, client: {self._db_type}")
            return True
        except (mysql.connector.Error, psycopg2.Error, ConnectionError) as e:
            logger.error(f"!! DBPool Connection Check Error ({self._db_type}): {e}")
            return False
        finally:
            self.close(conn, cursor)

    def handle_error(self, conn, cursor, method, *args, **kwargs):
        """Handles errors, attempting to reconnect and recreate the pool."""
        if self._reconnect_attempts < self._max_reconnect_attempts:
            try:
                self._reconnect_attempts += 1
                self.close(conn, cursor) # Close bad connection
                logger.warning(f"!! DBPool Attempting to recreate pool... ({self._reconnect_attempts})")
                
                if self._db_type == "mysql":
                    self.pool = self._create_mysql_pool(pool_name=self._pool_name, pool_size=self._pool_size)
                elif self._db_type == "postgresql":
                    self.pool = self._create_postgresql_pool(pool_name=self._pool_name, pool_size=self._pool_size)
                
                if self.pool is None:
                    raise ConnectionError("Failed to re-create database pool.")
                    
                time.sleep(5)
                return method(*args, **kwargs) # Retry the original method
            except Exception as e:
                logger.error(f"!! DBPool Error during reconnection: {e}", exc_info=True)
                time.sleep(5)
                # Pass None for conn/cursor as they are already closed
                return self.handle_error(None, None, method, *args, **kwargs)
        else:
            self.close(conn, cursor)
            logger.critical(f"!! DBPool Error: Exceeded max reconnect attempts.")
            return None