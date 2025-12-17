# sqes/analysis/repository.py
import logging
from typing import Optional
from .db_pool import DBPool

logger = logging.getLogger(__name__)

class QCRepository:
    """
    Handles all database interactions for the QC application.
    This is the *only* place SQL queries should exist.
    """
    def __init__(self, pool: DBPool, db_type: str):
        self.pool = pool
        self.db_type = db_type
        logger.debug(f"QCRepository initialized for {db_type}")

    def _get_query(self, query_name: str) -> str:
        """Centralized query storage."""
        
        # --- THIS IS THE UPDATED CTE ---
        # This new query joins *only* on station code.
        station_tuple_base_cte = """
            WITH sensor_info AS (
                -- 1. Get all sensor data, ranking locations per station
                SELECT 
                    code,
                    COALESCE(location, '') AS loc,
                    SUBSTRING(channel, 1, 2) AS channel_prefix,
                    SUBSTRING(channel, 3, 1) AS channel_component,
                    CASE SUBSTRING(channel, 1, 2) 
                        WHEN 'SH' THEN 1 WHEN 'BH' THEN 2 
                        WHEN 'HH' THEN 3 WHEN 'HN' THEN 4 
                        ELSE 5 
                    END AS sort_order,
                    -- Rank locations: '00' is best, then '', then others.
                    ROW_NUMBER() OVER(
                        PARTITION BY code 
                        ORDER BY CASE WHEN location = '00' THEN 1 WHEN location = '' THEN 2 ELSE 3 END, location
                    ) as loc_rank
                FROM stations_sensor
            ),
            distinct_prefixes AS (
                -- 2. Get unique, sorted prefixes *per code*
                SELECT DISTINCT code, channel_prefix, sort_order
                FROM sensor_info
            ),
            distinct_components AS (
                -- 3. Get unique components *per code*
                SELECT DISTINCT code, channel_component
                FROM sensor_info
            ),
            aggregated_prefixes AS (
                -- 4. Aggregate prefixes
                SELECT code, {agg_function_prefix} AS channel_prefixes
                FROM distinct_prefixes
                GROUP BY code
            ),
            aggregated_components AS (
                -- 5. Aggregate components
                SELECT code, {agg_function_comp} AS channel_components
                FROM distinct_components
                GROUP BY code
            ),
            primary_location AS (
                -- 6. Select the "best" location for each code (the one ranked #1)
                SELECT DISTINCT code, loc
                FROM sensor_info
                WHERE loc_rank = 1
            )
            -- 7. Final Join: Join all aggregates to the main stations table
            SELECT 
                s.network, s.code, 
                -- Use the "best" location from stations_sensor
                COALESCE(pl.loc, '') AS location, 
                s.network_group, 
                COALESCE(ap.channel_prefixes, '') AS channel_prefixes,
                COALESCE(ac.channel_components, '') AS channel_components
            FROM stations AS s 
            -- Join *only* on s.code
            LEFT JOIN aggregated_prefixes AS ap ON s.code = ap.code
            LEFT JOIN aggregated_components AS ac ON s.code = ac.code
            LEFT JOIN primary_location AS pl ON s.code = pl.code
        """
        
        mysql_cte = station_tuple_base_cte.format(
            agg_function_prefix="GROUP_CONCAT(channel_prefix ORDER BY sort_order SEPARATOR ',')", 
            agg_function_comp="GROUP_CONCAT(DISTINCT channel_component SEPARATOR ',')"
        )
        postgresql_cte = station_tuple_base_cte.format(
            agg_function_prefix="STRING_AGG(channel_prefix, ',' ORDER BY sort_order)", 
            agg_function_comp="STRING_AGG(DISTINCT channel_component, ',')"
        )

        queries = {
            'mysql': {
                'get_stations': f"""
                    {mysql_cte}
                    WHERE s.code NOT IN (
                        SELECT code FROM tb_qcdetail WHERE tanggal = %s GROUP BY code HAVING COUNT(code) >= 3
                    );
                """,
                'get_station_tuple': f"""
                    {mysql_cte}
                    WHERE s.code = %s;
                """,
                'get_station_tuples_base': f"""
                    {mysql_cte}
                    WHERE s.code IN
                """,
                'get_stragglers': """
                    SELECT DISTINCT T1.kode 
                    FROM tb_qcdetail AS T1
                    LEFT JOIN tb_qcres AS T2
                      ON T1.kode = T2.kode_res AND T1.tanggal = T2.tanggal_res
                    WHERE T1.tanggal = %s AND T2.kode_res IS NULL
                """,
                'flush_details': "DELETE FROM tb_qcdetail WHERE tanggal = %s",
                'flush_results': "DELETE FROM tb_qcres WHERE tanggal_res = %s",
                'check_detail': "SELECT id_kode FROM tb_qcdetail WHERE id_kode = %s AND tanggal = %s",
                'delete_detail': "DELETE FROM tb_qcdetail WHERE id_kode = %s AND tanggal = %s",
                'insert_detail': """
                    INSERT INTO tb_qcdetail (
                        id_kode, kode, tanggal, komp, rms, ratioamp, avail, ngap, nover, num_spikes, 
                        pct_above, pct_below, dead_channel_lin, dead_channel_gsn, 
                        diff20_100, diff5_20, diff5
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                'get_qc_details': "SELECT * FROM tb_qcdetail WHERE tanggal = %s AND kode = %s",
                'get_station_info': "SELECT kode_sensor, lokasi_sensor, sistem_sensor FROM tb_slmon WHERE kode_sensor = %s",
                'check_analysis': "SELECT * FROM tb_qcres WHERE tanggal_res = %s AND kode_res = %s",
                'delete_analysis': "DELETE FROM tb_qcres WHERE tanggal_res = %s AND kode_res = %s",
                'insert_analysis': """
                    INSERT INTO tb_qcres 
                    (kode_res, tanggal_res, percqc, kualitas, tipe, keterangan) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
            },
            'postgresql': {
                'get_stations': f"""
                    {postgresql_cte}
                    WHERE s.code NOT IN (
                        SELECT code FROM stations_qc_details WHERE date = %s GROUP BY code HAVING COUNT(code) >= 3
                    );
                """,
                'get_station_tuple': f"""
                    {postgresql_cte}
                    WHERE s.code = %s;
                """,
                'get_station_tuples_base': f"""
                    {postgresql_cte}
                    WHERE s.code IN
                """,
                'get_stragglers': """
                    SELECT DISTINCT T1.code 
                    FROM stations_qc_details AS T1
                    LEFT JOIN stations_data_quality AS T2
                      ON T1.code = T2.code AND T1.date = T2.date
                    WHERE T1.date = %s AND T2.code IS NULL
                """,
                'flush_details': "DELETE FROM stations_qc_details WHERE date = %s",
                'flush_results': "DELETE FROM stations_data_quality WHERE date = %s",
                'check_detail': "SELECT id FROM stations_qc_details WHERE id = %s AND date = %s",
                'delete_detail': "DELETE FROM stations_qc_details WHERE id = %s AND date = %s",
                'insert_detail': """
                    INSERT INTO stations_qc_details (
                        id, code, date, channel, rms, amplitude_ratio, availability, num_gap, 
                        num_overlap, num_spikes, perc_above_nhnm, perc_below_nlnm, 
                        linear_dead_channel, gsn_dead_channel, lp_percentage, bw_percentage, 
                        sp_percentage
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                'get_qc_details': "SELECT * FROM stations_qc_details WHERE date = %s AND code = %s",
                'get_station_info': "SELECT network, code, location, network_group FROM stations WHERE code = %s",
                'check_analysis': "SELECT * FROM stations_data_quality WHERE date = %s AND code = %s",
                'delete_analysis': "DELETE FROM stations_data_quality WHERE date = %s AND code = %s",
                'insert_analysis': """
                    INSERT INTO stations_data_quality 
                    (code, date, quality_percentage, result, details) 
                    VALUES (%s, %s, %s, %s, %s)
                """
            }
        }
        try:
            return queries[self.db_type][query_name]
        except KeyError:
            logger.critical(f"Query '{query_name}' not defined for database type '{self.db_type}'")
            raise

    # --- Main Workflow Methods ---

    def get_stations_to_process(self, tgl: str, network: Optional[list] = None):
        query = self._get_query('get_stations')
        
        args = [tgl]
        if network:
            # Remove trailing semicolon if present
            query = query.strip().rstrip(';')
            
            placeholders = ', '.join(['%s'] * len(network))
            query += f" AND s.network IN ({placeholders});"
            args.extend(network)
            
        logger.debug(f"Querying stations to process for {tgl} (Network: {network})")
        return self.pool.execute(query, args=tuple(args))

    def get_station_tuple(self, station_code: str):
        """Fetches the full station tuple for a single station."""
        query = self._get_query('get_station_tuple')
        result = self.pool.execute(query, args=(station_code,))
        if result:
            return result[0]
        else:
            return None

    def get_station_tuples(self, station_list: list, network: Optional[list] = None):
        """Fetches station tuples for a specific list of stations."""
        if not station_list:
            return []
        
        placeholders = ', '.join(['%s'] * len(station_list))
        base_query = self._get_query('get_station_tuples_base')
        
        # Add IN clause
        query = f"{base_query} ({placeholders})"
        
        args = list(station_list)
        if network:
            net_placeholders = ', '.join(['%s'] * len(network))
            query += f" AND s.network IN ({net_placeholders})"
            args.extend(network)
            
        query += ";"
        
        return self.pool.execute(query, args=tuple(args))

    def get_straggler_stations(self, tgl: str, station_list: Optional[list] = None):
        """Fetches straggler stations, optionally filtered by a list."""
        query = self._get_query('get_stragglers')
        args = [tgl]
        
        if station_list:
            placeholders = ', '.join(['%s'] * len(station_list))
            if self.db_type == 'mysql':
                query += f" AND T1.kode IN ({placeholders})"
            else: # postgresql
                query += f" AND T1.code IN ({placeholders})"
            args.extend(station_list)
        
        return self.pool.execute(query, args=tuple(args))

    def flush_daily_data(self, tgl: str):
        logger.warning(f"Flushing ALL data for {tgl}!")
        query_details = self._get_query('flush_details')
        self.pool.execute(query_details, args=(tgl,), commit=True)
        logger.debug(f"Flushed qc_details for {tgl}")
        
        query_results = self._get_query('flush_results')
        self.pool.execute(query_results, args=(tgl,), commit=True)
        logger.debug(f"Flushed qc_results/data_quality for {tgl}")

    def check_and_delete_qc_detail(self, id_kode: str, tgl: str):
        """Checks if data exists and deletes it."""
        check_query = self._get_query('check_detail')
        data = self.pool.execute(check_query, args=(id_kode, tgl))
        
        if data:
            logger.info(f"{id_kode} data exists, deleting previous data")
            delete_query = self._get_query('delete_detail')
            self.pool.execute(delete_query, args=(id_kode, tgl), commit=True)
            return True
        return False

    def insert_qc_detail(self, metrics: dict):
        """Inserts a full row of metrics."""
        query = self._get_query('insert_detail')
        args = (
            metrics['id_kode'], metrics['kode'], metrics['tgl'], metrics['cha'],
            metrics['rms'], metrics['ratioamp'], metrics['psdata'],
            metrics['ngap'], metrics['nover'], metrics['num_spikes'],
            metrics['pctH'], metrics['pctL'], metrics['dcl'], metrics['dcg'],
            metrics['long_period'], metrics['microseism'], metrics['short_period']
        )
        self.pool.execute(query, args=args, commit=True)
        logger.debug(f"Inserted full metrics for {metrics['id_kode']}")

    def insert_default_qc_detail(self, id_kode, kode, tgl, cha, metrics: dict):
        """Inserts a row with default (bad) data."""
        query = self._get_query('insert_detail')
        args = (
            id_kode, kode, tgl, cha,
            metrics['rms'], metrics['ratioamp'], metrics['psdata'],
            metrics['ngap'], metrics['nover'], metrics['num_spikes'],
            '100', '0', '0', '0', '0', '0', '0' # Default PPSD values
        )
        self.pool.execute(query, args=args, commit=True)
        logger.debug(f"Inserted default metrics for {id_kode}")

    # --- QC Analysis Methods ---

    def get_station_info(self, station_code: str):
        query = self._get_query('get_station_info')
        return self.pool.execute(query, args=(station_code,))

    def get_qc_details_for_station(self, tgl: str, station_code: str):
        query = self._get_query('get_qc_details')
        return self.pool.execute(query, args=(tgl, station_code))

    def flush_analysis_result(self, tgl: str, station_code: str):
        """Checks and deletes a previous analysis result."""
        check_query = self._get_query('check_analysis')
        data = self.pool.execute(check_query, args=(tgl, station_code))
        
        if data:
            logger.info(f"Analysis data for {station_code} on {tgl} exists, flushing.")
            delete_query = self._get_query('delete_analysis')
            self.pool.execute(delete_query, args=(tgl, station_code), commit=True)
            return True
        return False

    def insert_qc_analysis_result(self, code, date, percqc, result, tipe, details):
        """Inserts the final analysis result."""
        query = self._get_query('insert_analysis')
        
        ket_str = ''
        if isinstance(details, list) and details:
            ket_str = ', '.join(map(str, details))
            
        if self.db_type == 'mysql':
            args = (code, date, percqc, result, tipe, ket_str)
        else: # postgresql
            args = (code, date, percqc, result, ket_str)
            
        self.pool.execute(query, args=args, commit=True)
        logger.debug(f"Inserted analysis result for {code} on {date}")

    # --- Station Management Methods ---

    def get_all_stations_basic(self):
        """Fetches basic station information (code, latitude, longitude)."""
        if self.db_type == 'postgresql':
            query = "SELECT code, latitude, longitude, network, province, location, upt, digitizer_type, communication_type FROM stations"
        else:  # mysql
            query = "SELECT kode_sensor AS code, latitude, longitude FROM tb_slmon"
        return self.pool.execute(query)

    def insert_station(self, station_data: dict):
        """Inserts a new station into the stations table."""
        if self.db_type == 'postgresql':
            query = """
                INSERT INTO stations (code, network, latitude, longitude, province, location, upt, digitizer_type, communication_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            args = (
                station_data['code'],
                station_data.get('network'),
                station_data['latitude'],
                station_data['longitude'],
                station_data.get('province'),
                station_data.get('location'),
                station_data.get('upt'),
                station_data.get('digitizer_type'),
                station_data.get('communication_type')
            )
            self.pool.execute(query, args=args, commit=True)
            logger.debug(f"Inserted new station: {station_data['code']}")
        else:
            logger.warning("insert_station is only supported for PostgreSQL")

    def update_station(self, code: str, updates: dict):
        """Updates an existing station with the provided field updates."""
        if self.db_type == 'postgresql':
            if not updates:
                logger.debug(f"No updates provided for station {code}")
                return
            
            # Dynamically build the SET clause
            set_clause = ", ".join([f"{key} = %s" for key in updates.keys()])
            query = f"UPDATE stations SET {set_clause} WHERE code = %s"
            
            # Parameters in order: update values + station code
            args = tuple(updates.values()) + (code,)
            
            self.pool.execute(query, args=args, commit=True)
            logger.debug(f"Updated station {code} with fields: {list(updates.keys())}")
        else:
            logger.warning("update_station is only supported for PostgreSQL")

    def get_station_codes_from_table(self, table_name: str):
        """Fetches station codes from a specific table."""
        if self.db_type == 'postgresql':
            query = f"SELECT code FROM {table_name}"
            return self.pool.execute(query)
        else:
            logger.warning(f"get_station_codes_from_table is only supported for PostgreSQL")
            return []

    def insert_station_into_table(self, table_name: str, station_code: str):
        """Inserts a station code into a specific table."""
        if self.db_type == 'postgresql':
            query = f"INSERT INTO {table_name} (code) VALUES (%s)"
            self.pool.execute(query, args=(station_code,), commit=True)
            logger.debug(f"Inserted station {station_code} into {table_name}")
        else:
            logger.warning(f"insert_station_into_table is only supported for PostgreSQL")

    def delete_sensor_data_for_stations(self, station_codes: list):
        """Deletes sensor data for a list of station codes."""
        if self.db_type == 'postgresql':
            if not station_codes:
                logger.debug("No station codes provided for sensor deletion")
                return
            
            placeholders = ', '.join(['%s'] * len(station_codes))
            query = f"DELETE FROM stations_sensor WHERE code IN ({placeholders})"
            self.pool.execute(query, args=tuple(station_codes), commit=True)
            logger.debug(f"Deleted sensor data for {len(station_codes)} stations")
        else:
            logger.warning("delete_sensor_data_for_stations is only supported for PostgreSQL")

    def bulk_insert_sensor_data(self, sensor_records: list):
        """Bulk inserts sensor data records."""
        if self.db_type == 'postgresql':
            if not sensor_records:
                logger.debug("No sensor records to insert")
                return
            
            query = """
                INSERT INTO stations_sensor (code, location, channel, sensor)
                VALUES (%s, %s, %s, %s)
            """
            # Prepare all records as tuples
            args_list = [
                (record['code'], record['location'], record['channel'], record['sensor'])
                for record in sensor_records
            ]
            
            # Use executemany for bulk insert
            self.pool.executemany(query, args_list, commit=True)
            logger.debug(f"Bulk inserted {len(sensor_records)} sensor records")
        else:
            logger.warning("bulk_insert_sensor_data is only supported for PostgreSQL")

    def bulk_insert_latency_data(self, latency_records: list):
        """Bulk inserts latency data records."""
        if self.db_type == 'postgresql':
            if not latency_records:
                logger.debug("No latency records to insert")
                return
            
            query = """
                INSERT INTO stations_sensor_latency (net, sta, datetime, channel, last_time_channel, latency, color_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            # Prepare all records as tuples
            # Expected dictionary keys match the columns in the query
            args_list = [
                (
                    record['net'], 
                    record['sta'], 
                    record['datetime'], 
                    record['channel'], 
                    record['last_time_channel'], 
                    record['latency'], 
                    record['color_code']
                )
                for record in latency_records
            ]
            
            # Use executemany for bulk insert
            self.pool.executemany(query, args_list, commit=True)
            logger.debug(f"Bulk inserted {len(latency_records)} latency records")
        else:
            logger.warning("bulk_insert_latency_data is only supported for PostgreSQL")