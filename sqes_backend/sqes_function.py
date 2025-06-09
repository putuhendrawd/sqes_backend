from ast import arg
from obspy.signal import PPSD
import os, sys, math, mysql.connector, time
import mysql.connector.pooling
import psycopg2.pool
import time
import numpy as np
from scipy import stats
import pandas as pd
from obspy.signal.quality_control import MSEEDMetadata
from obspy.clients.fdsn import Client
from numpy.polynomial.polynomial import polyfit
from obspy import read_inventory
from configparser import ConfigParser
from typing import Dict

class InventoryMissing(ValueError):
	pass

class DataMissing(ValueError):
	pass

class Config():
    @staticmethod
    def load_config(filename : str='config.ini', section:str='postgresql') -> Dict[str,str]:
        module_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(module_path, '..' ,'config', filename)
        parser=ConfigParser()
        parser.read(config_path)
        # get config
        config={}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                if param[0] == 'cpu_number_used' and param[1]:
                    config[param[0]] = int(param[1])
                else:
                    config[param[0]] = param[1]
        else:
            raise Exception(f'Section {section} not found in the {filename} file')
        return config

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
        self._pool_size = pool_size

        self.dbconfig = {}
        self.pool = None # Initialize self.pool to None

        try:
            if self._db_type == "mysql":
                self._port = port if port else "3306"
                self.dbconfig["host"] = self._host
                self.dbconfig["port"] = int(self._port)
                self.dbconfig["user"] = self._user
                self.dbconfig["password"] = self._password
                self.dbconfig["database"] = self._database
                self.pool = self._create_mysql_pool(pool_name=pool_name, pool_size=pool_size)
            elif self._db_type == "postgresql":
                self._port = port if port else "5432"
                self.dbconfig["host"] = self._host
                self.dbconfig["port"] = int(self._port)
                self.dbconfig["user"] = self._user
                self.dbconfig["password"] = self._password
                self.dbconfig["dbname"] = self._database
                self.pool = self._create_postgresql_pool(pool_name=pool_name, pool_size=pool_size)
            else:
                raise ValueError("Unsupported database type. Choose 'mysql' or 'postgresql'.")
        except Exception as e:
            print(f"Error initializing DBPool for {self._db_type}: {e}", flush=True)
            self.pool = None

    def _create_mysql_pool(self, pool_name="sqes_pool", pool_size=3):
        try:
            pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name=pool_name,
                pool_size=pool_size,
                pool_reset_session=True,
                **self.dbconfig)
            return pool
        except Exception as e:
            print(f"Error creating MySQL pool: {e}", flush=True)
            return None

    def _create_postgresql_pool(self, pool_name="pg_pool", pool_size=3):
        try:
            pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=pool_size,
                maxconn=pool_size,
                **self.dbconfig
            )
            return pool
        except Exception as e:
            print(f"Error creating PostgreSQL pool: {e}", flush=True)
            return None

    def close(self, conn, cursor):
        """
        A method used to close connection and cursor.
        """
        if cursor:
            cursor.close()
        if conn:
            if self.pool is not None: # Ensure pool exists before returning connection
                if self._db_type == "postgresql":
                    self.pool.putconn(conn) # type: ignore 
                else: # mysql
                    conn.close() # Return connection to pool for mysql.connector
            else:
                # If pool is None, just close the connection if it's not from a valid pool
                conn.close()


    def _get_connection_from_pool(self):
        if self.pool is None:
            raise ConnectionError(f"Database pool for {self._db_type} is not initialized or failed to connect.")
        
        if self._db_type == "mysql":
            return self.pool.get_connection() # type: ignore # Correct for MySQL
        elif self._db_type == "postgresql":
            return self.pool.getconn()        # type: ignore # Correct for PostgreSQL
        else:
            raise ValueError(f"Unknown database type: {self._db_type}")

    def execute(self, sql, args=None, commit=False):
        conn = None
        cursor = None
        try:
            conn = self._get_connection_from_pool()
            cursor = conn.cursor()

            if args:
                if self._db_type == "mysql":
                    cursor.execute(sql, args)
                elif self._db_type == "postgresql":
                    # Ensure args is always a tuple for psycopg2.execute
                    if not isinstance(args, (tuple, list)):
                        args_to_execute = (args,)
                    else:
                        args_to_execute = tuple(args) # Convert any list to tuple for consistency
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
            print(f"!! DBPool Error ({self._db_type}): {e}", flush=True)
            # Pass *args and **kwargs as they were received by the original call
            return self.handle_error(conn, cursor, self.execute, sql, args=args, commit=commit)
        except IndexError as e:
            print(sql)
            print(f"!! IndexError: {e}", flush=True)
            # Pass *args and **kwargs as they were received by the original call
            return self.handle_error(conn, cursor, self.execute, sql, args=args, commit=commit)
        finally:
            self.close(conn, cursor)


    def executemany(self, sql, args, commit=False):
        conn = None
        cursor = None
        try:
            conn = self._get_connection_from_pool()
            cursor = conn.cursor()
            
            if self._db_type == "mysql":
                cursor.executemany(sql, args)
            elif self._db_type == "postgresql":
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
            print(f"!! DBPool Error ({self._db_type}): {e}", flush=True)
            return self.handle_error(conn, cursor, self.executemany, sql, args=args, commit=commit)
        except IndexError as e:
            print(sql)
            print(f"!! IndexError: {e}", flush=True)
            return self.handle_error(conn, cursor, self.executemany, sql, args=args, commit=commit)
        finally:
            self.close(conn, cursor)
            
    def is_db_connected(self):
        conn = None
        cursor = None
        try:
            conn = self._get_connection_from_pool()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchall()
            return print(f"DB Pool Connection Check: OK, client: {self._db_type}", flush=True)
        except (mysql.connector.Error, psycopg2.Error, ConnectionError) as e:
            print(f"!! DBPool Connection Check Error ({self._db_type}): {e}", flush=True)
            return False
        finally:
            self.close(conn, cursor)

    def handle_error(self, conn, cursor, method, *args, **kwargs):
        if self._reconnect_attempts < self._max_reconnect_attempts:
            try:
                self._reconnect_attempts += 1
                self.close(conn, cursor) # Close the individual connection

                print(f"!! DBPool Attempting to recreate pool and reconnect ({self._db_type})... ({self._reconnect_attempts})", flush=True)
                
                # Recreate the entire pool (this will replace the potentially broken pool)
                if self._db_type == "mysql":
                    self.pool = self._create_mysql_pool(pool_name=self._pool_name, pool_size=self._pool_size)
                elif self._db_type == "postgresql":
                    self.pool = self._create_postgresql_pool(pool_name=self._pool_name, pool_size=self._pool_size)
                
                if self.pool is None: # If pool recreation also failed
                    raise ConnectionError("Failed to re-create database pool after error.")
                    
                time.sleep(5)

                return method(*args, **kwargs)
            except Exception as e:
                print(f"!! DBPool Error ({self._db_type}): Error during reconnection attempt: {e}", flush=True)
                time.sleep(5)
                return self.handle_error(conn, cursor, method, *args, **kwargs)
        else:
            self.close(conn, cursor)
            print(f"!! DBPool Error ({self._db_type}): Exceeded maximum reconnect attempts. Pool might be unavailable.", flush=True)
            print("!! Warning, some data may be skipped !!", flush=True)
            return None
            
    def print_db(self):
        print(self.dbconfig, flush=True)

class Calculation():
    @staticmethod
    def get_models(periods,powers):
        NHNM = []
        NLNM = []
        PERIODS = []# the indices corresponding to periods within the defined models
        # NHNM
        Ph = [0.10, 0.22, 0.32, 0.80, 3.80, 4.60, 6.30, 7.90, 15.40, 20.00, 354.80, 100000.00]
        Ah = [-108.73, -150.34, -122.31, -116.85, -108.48, -74.66, 0.66, -93.37, 73.54, -151.52, -206.66]
        Bh = [-17.23, -80.50, -23.87, 32.51, 18.08, -32.95, -127.18, -22.42, -162.98, 10.01, 31.63]
        # NLNM
        Pl = [0.10, 0.17, 0.40, 0.80, 1.24, 2.40, 4.30, 5.00, 6.00, 10.00, 12.00, 15.60, 21.90, 
                31.60, 45.00, 70.00, 101.00, 154.00, 328.00, 600.00, 10000.00, 100000.00]
        Al = [-162.36, -166.7, -170.00, -166.40, -168.60, -159.98, -141.10, -71.36, -97.26, 
                -132.18, -205.27, -37.65, -114.37, -160.58, -187.50, -216.47, -185.00, -168.34, 
                -217.43, -258.28, -346.88]
        Bl = [5.64, 0.00, -8.30, 28.90, 52.48, 29.81, 0.00, -99.77, -66.49, -31.57, 36.16, 
                -104.33, -47.10, -16.28, 0.00, 15.70, 0.00, -7.61, 11.90, 26.60, 48.75]
        pInd=0
        for period in periods:
        # find where this period lies in the list of noise model periods
            try:
                highInd = [i for i, x in enumerate([period > Ph][0]) if x][-1]
                lowInd = [i for i, x in enumerate([period > Pl][0]) if x][-1]
            #print(highInd,lowInd)
            except:
                pInd += 1
                continue
            nhnm = Ah[highInd] + Bh[highInd] * math.log(period, 10)# power value
            nhnmInd = [nhnm for i, x in enumerate(powers) if x == int(nhnm)][0]# index for that power
            nlnm = Al[lowInd] + Bl[lowInd] * math.log(period, 10)
            nlnmInd = [nlnm for i, x in enumerate(powers) if x == int(nlnm)][0] 
            NHNM.append(nhnmInd)
            NLNM.append(nlnmInd)
            PERIODS.append(pInd)
            pInd += 1
        return NHNM, NLNM, PERIODS
    
    @staticmethod
    def dead_channel_gsn(psd,model,t,t0=4.0,t1=8.0):
        #f dalam periode
        psd = psd[(t>t0) & (t<t1)]
        model = model[(t>t0) & (t<t1)]
        dcg = np.mean(model-psd)
        return dcg

    @staticmethod
    def pct_model(psd,AHNM,ALNM):
        percH=0
        percL=0
        for i in range(len(psd)):
            if psd[i] > AHNM[i]:
                percH += 1
            if psd[i] < ALNM[i]:
                percL += 1
        percH = round(float(percH*100/len(psd)),2)
        percL = round(float(percL*100/len(psd)),2)
        return percH, percL
    
    @staticmethod
    def pct_model_period(psd,LNM,HNM,t,t0,t1):
        percH=0
        psd = psd[(t>t0) & (t<t1)]
        LNM = LNM[(t>t0) & (t<t1)]
        HNM = HNM[(t>t0) & (t<t1)]

        for i in range(len(psd)):
            if (psd[i] <= HNM[i]) and (psd[i] >= LNM[i]):
                percH += 1
        percH = round(float(percH*100/len(psd)),2)
        return percH

    @staticmethod
    def dead_channel_lin(psd,t,fs):
        #f dalam periode
        t0=0.1;t1=100.0
        psd = psd[(t>t0) & (t<t1)]
        tn = t[(t>t0) & (t<t1)]
        tn = np.log10(tn)
        slope,intercept = polyfit(tn,psd,1)
        psdfit = intercept + slope * tn
        dcl = np.sqrt(np.mean(abs(psdfit - psd)))
        return dcl

    @staticmethod
    def cal_spikes(data,wn,sigma):
        if len(data) < wn*2 :
            num_spike = 0
            return num_spike
        mad_array = lambda x: np.median(np.abs(x - np.median(x)),axis=1)
        diff_array = lambda x: np.median(x,axis=1)
        N = len(data)
        vert_idx_list = np.arange(0, N - wn, 1)
        hori_idx_list = np.arange(wn+1)
        A, B = np.meshgrid(hori_idx_list, vert_idx_list)
        idx_array = A + B 
        x_array = data[idx_array]
        mad = mad_array(x_array)
        x_mean = diff_array(x_array)
        data = data[vert_idx_list+int((wn+1)/2.)]
        difference = np.abs(data-x_mean)
        threshold = 1.4826 * sigma * mad
        outlier_idx = difference > threshold
        num_spike = len(data[outlier_idx])
        return num_spike
    
    @staticmethod
    def cal_rms(st):
        rms_values = []
        for tr in st:
            data = tr.data
            npts = tr.stats.npts
            rms_values.append(np.sqrt(np.sum(data**2) / npts))
        return sum(rms_values)/len(rms_values)

    @staticmethod
    def cal_percent_availability(st):
        if not st:
            return 0.0
        starttime = min(tr.stats.starttime for tr in st)
        endtime = max(tr.stats.endtime for tr in st)
        totaltime = endtime-starttime
        delta_gaps = 0
        for gap in st.get_gaps():
            # gap[6] is the delta (duration of the gap/overlap)
            if gap[6] > 0:
                delta_gaps += gap[6]
        if totaltime == 0:
            return 0.0
        percentage = 100 * ((totaltime - delta_gaps) / totaltime)
        return round(percentage,2)

    @staticmethod
    def cal_gaps_overlaps(st):
        result = st.get_gaps()
        gaps = 0
        overlaps = 0
        for r in result:
            if r[6] > 0:
                gaps += 1
            else:
                overlaps += 1
        return gaps, overlaps
    
    @staticmethod
    def prosess_matriks(files,data,time0,time1):
        st=data.copy()
        st.detrend()
        # mseedqc = MSEEDMetadata([files],starttime=time0,endtime=time1) ## deprecated
        rms = Calculation.cal_rms(st)
        if rms > 99999:
            rms = 99999
        
        ampmax = max([tr.data.max() for tr in st])
        ampmin = min([tr.data.min() for tr in st])
        psdata = Calculation.cal_percent_availability(st)
        ngap,nover = Calculation.cal_gaps_overlaps(st)
        # nd = mseedqc.meta['num_samples'] ## not used
        num_spikes = 0.0
        for tr in st:
            num_spike = Calculation.cal_spikes(tr.data,80,10)
            num_spikes += num_spike
        return rms,ampmax,ampmin,psdata,ngap,nover,num_spikes
    
    @staticmethod
    def prosess_psd(sig, inventory=None, output=''):
        NPZFNAME = '_{}.npz'
        data = sig.copy()
        if inventory is None:
            print('Please provide inventory file')
            return None
        if data.count() == 0:
            print('No data in mseed file')
            return None
        data.merge()

        if data[0].stats.npts<=3600*data[0].stats.sampling_rate:
            return {}
        ppsds_object = None
        id_ = '' 
        for tr in data:
            id_ = tr.id
            ppsds_object = PPSD(tr.stats, inventory)
            ppsds_object.add(tr)

        if output and ppsds_object:
            fname_out = output + NPZFNAME.format(id_)
            ppsds_object.save_npz(fname_out)
        elif output and not ppsds_object:
            print("Warning: No PPSD object generated to save.")
        return ppsds_object
    
    @staticmethod
    # calculation and process
    def calculate_ratioamp(ampmin,ampmax):
        if ampmax > ampmin:
            return ampmax/ampmin
        elif ampmax == 0 or ampmin == 0:
            return 1.0
        else:
            return ampmin/ampmax
    
class Analysis():
    @staticmethod
    def sql_execommit_analisqc(pool,db,kode,tanggal,percqc,kualitas,tipe,ket):
        if isinstance(ket, list):
            ket_str = ', '.join(ket)
        else:
            ket_str = str(ket)

        if db == 'mysql':
            # Use placeholders (%s) and pass values as a tuple for parameterized query
            sql = "INSERT INTO tb_qcres (kode_res, tanggal_res, percqc, kualitas, tipe, keterangan) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (kode, tanggal, percqc, kualitas, tipe, ket_str)
        elif db == 'postgresql':
            # Use placeholders (%s) for PostgreSQL (or %s for psycopg2)
            sql = "INSERT INTO stations_data_quality (code, date, quality_percentage, result, details) VALUES (%s, %s, %s, %s, %s)"
            values = (kode, tanggal, percqc, kualitas, ket_str)
        else:
            raise ValueError("Unsupported database type")
        pool.execute(sql,args=values,commit=True)

    @staticmethod
    def agregate(par,lim,m):
        grade=100.0-(15.0*(par-lim)/m)
        if grade > 100.0:
            grade=100.0
        elif grade < 0.0:
            grade=0.0
        return grade

    @staticmethod
    def check_qc(percqc):
        if percqc >= 90.0:
            kualitas = 'Baik'
        elif percqc >= 60.0:
            kualitas = 'Cukup Baik'
        elif percqc == 0.0:
            kualitas = 'Mati'
        else:
            kualitas = 'Buruk'
        return kualitas 
    
    @staticmethod
    def QC_Analysis(pool,db,tanggal,station):
        # flush data in related date
        if db == 'mysql':
            sql = f"SELECT * FROM tb_qcres WHERE tanggal_res = %s AND kode_res = %s"
        elif db == 'postgresql':
            sql = f"SELECT * FROM stations_data_quality WHERE date = %s AND code = %s"
        # print(sql)
        data=pool.execute(sql,args=(tanggal,station))
        print(f"number of qcdata available: {len(data)}", flush=True)

        if data:
            print(f"! Data {station} on {tanggal} available, flushing database!", flush=True)
            if db == 'mysql':
                sql = f"DELETE FROM tb_qcres WHERE tanggal_res = %s AND kode_res = %s"
            elif db == 'postgresql':
                sql = f"DELETE FROM stations_data_quality WHERE date = %s AND code = %s"
            pool.execute(sql, args=(tanggal, station), commit=True)
            print(f"! Data {station} on {tanggal} flush successful", flush=True)
        print(f"ready to fill database {station} on {tanggal}", flush=True)
    
    
        # Get station data
        if db == 'mysql':
            sql=f"SELECT kode_sensor,lokasi_sensor,sistem_sensor FROM tb_slmon WHERE kode_sensor = %s"
        elif db == 'postgresql':
            sql=f"SELECT code, location, network_group FROM stations WHERE code = %s"
        station=pool.execute(sql, args=(station,))

        for sta in station:
            kode = sta[0]
            tipe = sta[2]
            
            # check if there is duplicate data
            if db == 'mysql':
                sql_checker = f"SELECT * FROM tb_qcdetail WHERE tanggal = %s AND kode = %s"
            elif db == 'postgresql':
                sql_checker = f"SELECT * FROM stations_qc_details WHERE date = %s AND code = %s"
            dataqc = pool.execute(sql_checker, args=(tanggal, kode))
            
            # skip no data 
            if not dataqc:
                # print(f"!! <{tipe}> {kode} no data exist", flush=True)
                Analysis.sql_execommit_analisqc(pool,db,kode,tanggal,'0','Mati',tipe,'Tidak ada data')
                continue
            
            percqc=[]
            ket=[]
            for qc in dataqc:
                if db == 'mysql':
                    komp = qc[4]
                    rms = float(qc[5])
                    ratioamp = float(qc[6])
                    avail = float(qc[7])
                    ngap1 = int(qc[8])
                    nover = int(qc[9])
                    num_spikes = int(qc[10])
                    pct_above = float(qc[11])
                    pct_below = float(qc[12])
                    dcl = float(qc[13])
                    dcg = float(qc[14])
                elif db == 'postgresql':
                    komp = qc[3]
                    rms = float(qc[4])
                    ratioamp = float(qc[5])
                    avail = float(qc[6])
                    ngap1 = int(qc[7])
                    nover = int(qc[8])
                    num_spikes = int(qc[9])
                    pct_below = float(qc[10])
                    pct_above = float(qc[11])
                    dcl = float(qc[12])
                    dcg = float(qc[13])

                # rms calculation
                if rms > 1.0:
                    rms = Analysis.agregate(abs(rms),5000,10000)
                else:
                    rms = 0.0
                # ratio amp calculation
                ratioamp = Analysis.agregate(ratioamp,1.01,2.0)
                # availability and gap calculation
                if avail >= 100.0:
                    ngap1 = 0
                    avail = 100.0
                ngap = Analysis.agregate(ngap1,0,4)
                # overlap calculation
                nover = Analysis.agregate(nover,0,4)
                # spikes calculation
                num_spikes = Analysis.agregate(num_spikes,100,500)
                # pct calculation
                pct_noise = 100.0-pct_above-pct_below
                # pct_noise = Analysis.agregate(pct_noise,100,60)
                # dead channel calculation
                dcl = Analysis.agregate(dcl,2.0,-3.0)
                dcg = Analysis.agregate(dcg,1.0,1.0)

                # generate keterangan
                if rms < 1.0 and rms > 0.0:
                    ket.append(f"Komponen {komp} rusak")
                elif pct_below>50.0 and dcg > 5.0:
                    ket.append(f"Komponen {komp} tidak merespon getaran")
                elif pct_below>20.0:
                    ket.append(f"Cek metadata komponen {komp}")
                elif ngap1>500:
                    ket.append(f"Terlalu banyak gap pada komponen {komp}")
                elif pct_above>20 and avail>=10.0:
                    ket.append(f"Noise tinggi di komponen {komp}")
                    
                # generate weighted quality (botqc) per component
                if avail <= 0.0:
                    botqc = 0.0
                    ket.append(f'Komponen {komp} Mati')
                else:
                # botqc calculation,
                    #botqc = 0.2*avail+0.1*rms+0.1*ratioamp+0.05*ngap+0.05*nover+0.25*pct_below+0.25*pct_above
                    botqc = 0.15*avail+0.15*rms+0.1*ratioamp+0.025*ngap+0.025*nover+0.3*pct_noise+0.125*dcl+0.125*dcg
                percqc.append(botqc)
                
            # generate keterangan if keterangan is empty
            if len(ket) == 0:
                ket.append('')
            # generate general quality f station
            avg_percqc = np.sum(percqc)/len(percqc)
            kualitas = Analysis.check_qc(avg_percqc)
            Analysis.sql_execommit_analisqc(pool,db,kode,tanggal,str(round(avg_percqc,2)),kualitas,tipe,ket)
            print(f"<{tipe}> {kode} QC FINISH", flush=True)
            time.sleep(0.5) #make res to the process
            
        # make res to the process
        time.sleep(0.5)