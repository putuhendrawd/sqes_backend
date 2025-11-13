# sqes/analysis/repository.py
import logging
from typing import Optional
from sqes.services.db_pool import DBPool

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
        
        # This CTE (Common Table Expression) builds the station list.
        # It now aggregates both channel_prefixes (BH,HH) and
        # channel_components (E,N,Z).
        station_tuple_base_cte = """
            WITH distinct_channels_prefixes AS (
                -- 1. Find all *unique* channel prefixes and their sort order
                SELECT DISTINCT 
                    code, 
                    location, 
                    SUBSTRING(channel, 1, 2) AS channel_prefix,
                    CASE SUBSTRING(channel, 1, 2) 
                        WHEN 'SH' THEN 1 WHEN 'BH' THEN 2 
                        WHEN 'HH' THEN 3 WHEN 'HN' THEN 4 
                        ELSE 5 
                    END AS sort_order 
                FROM stations_sensor
            ),
            distinct_channel_components AS (
                -- 2. Find all *unique* channel components
                SELECT DISTINCT
                    code,
                    location,
                    SUBSTRING(channel, 3, 1) AS channel_component
                FROM stations_sensor
            ),
            aggregated_prefixes AS (
                -- 3. Aggregate the unique prefixes, *now* we can ORDER BY sort_order
                SELECT
                    code,
                    location,
                    {agg_function_prefix} AS channel_prefixes
                FROM distinct_channels_prefixes
                GROUP BY code, location
            ),
            aggregated_components AS (
                -- 4. Aggregate the unique components
                SELECT
                    code,
                    location,
                    {agg_function_comp} AS channel_components
                FROM distinct_channel_components
                GROUP BY code, location
            )
            -- 5. Join all the results
            SELECT 
                s.network, s.code, ac.location, s.network_group, 
                ap.channel_prefixes,
                ac.channel_components
            FROM stations AS s 
            LEFT JOIN aggregated_prefixes AS ap ON s.code = ap.code AND s.location = ap.location
            LEFT JOIN aggregated_components AS ac ON s.code = ac.code AND s.location = ac.location
        """
        
        # SQL dialects handle string aggregation differently.
        mysql_cte = station_tuple_base_cte.format(
            agg_function_prefix="GROUP_CONCAT(channel_prefix ORDER BY sort_order SEPARATOR ',')", 
            agg_function_comp="GROUP_CONCAT(channel_component SEPARATOR ',')"
        )
        # This new format is now valid for PostgreSQL
        postgresql_cte = station_tuple_base_cte.format(
            agg_function_prefix="STRING_AGG(channel_prefix, ',' ORDER BY sort_order)", 
            agg_function_comp="STRING_AGG(channel_component, ',')"
        )

        queries = {
            'mysql': {
                'get_stations': f"""
                    {mysql_cte}
                    LEFT JOIN (
                        SELECT code FROM tb_qcdetail WHERE tanggal = %s GROUP BY code HAVING COUNT(code) >= 3
                    ) AS cte ON s.code = cte.code 
                    WHERE cte.code IS NULL;
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
                    LEFT JOIN (
                        SELECT code FROM stations_qc_details WHERE date = %s GROUP BY code HAVING COUNT(code) >= 3
                    ) AS cte ON s.code = cte.code 
                    WHERE cte.code IS NULL;
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
                'get_station_info': "SELECT code, location, network_group FROM stations WHERE code = %s",
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

    def get_stations_to_process(self, tgl: str):
        query = self._get_query('get_stations')
        logger.debug(f"Querying stations to process for {tgl}")
        return self.pool.execute(query, args=(tgl,))

    def get_station_tuple(self, station_code: str):
        """Fetches the full station tuple for a single station."""
        query = self._get_query('get_station_tuple')
        result = self.pool.execute(query, args=(station_code,))
        if result:
            return result[0] # Return the first (and only) row
        else:
            return None

    def get_station_tuples(self, station_list: list):
        """Fetches station tuples for a specific list of stations."""
        if not station_list:
            return []
        
        placeholders = ', '.join(['%s'] * len(station_list))
        base_query = self._get_query('get_station_tuples_base')
        final_query = f"{base_query} ({placeholders});"
        
        return self.pool.execute(final_query, args=tuple(station_list))

    def get_straggler_stations(self, tgl: str, station_list: Optional[list] = None):
        """Fetches straggler stations, optionally filtered by a list."""
        query = self._get_query('get_stragglers')
        args = [tgl, tgl]
        
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
            metrics['diff20_100'], metrics['diff5_20'], metrics['diff5']
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
        
        ket_str = 'N/A'
        if isinstance(details, list) and details:
            ket_str = ', '.join(map(str, details))
            
        if self.db_type == 'mysql':
            args = (code, date, percqc, result, tipe, ket_str)
        else: # postgresql
            args = (code, date, percqc, result, ket_str)
            
        self.pool.execute(query, args=args, commit=True)
        logger.debug(f"Inserted analysis result for {code} on {date}")