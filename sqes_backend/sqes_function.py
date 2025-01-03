from obspy.signal import PPSD
import os, sys, math, mysql.connector, time
from mysql.connector.pooling import MySQLConnectionPool
# import psycopg2
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

# class PostgresSQLPool(object):
#     """
#     create a pool when connect postgresql using psycopg2, which will decrease the time spent in 
#     request connection, create connection and close connection.
#     """
#     def __init__(self, host="127.0.0.1", port="5432", user="postgres",
#                  password="password", database="test", pool_name="postgres_pool",
#                  pool_size=3, max_reconnect_attempts=10):
#         self._host = host
#         self._port = port
#         self._user = user
#         self._password = password
#         self._database = database
#         self._max_reconnect_attempts = max_reconnect_attempts
#         self._reconnect_attempts = 0
#         self._pool_name = pool_name
#         self._pool_size = pool_size
#         self.dbconfig = {
#             "host": self._host,
#             "port": self._port,
#             "user": self._user,
#             "password": self._password,
#             "database": self._database
#         }
#         self.pool = self.create_pool(pool_size=pool_size)
    
#     def create_pool(self, pool_size=3):
#         """
#         Create a connection pool. After creation, the request for connecting
#         to PostgreSQL can get a connection from this pool instead of creating
#         a new connection each time.
#         :param pool_size: The size of the pool, default is 3
#         :return: Connection pool
#         """
#         try:
#             pool = psycopg2.pool.SimpleConnectionPool( # type: ignore
#                 1, self._pool_size, **self.dbconfig
#             )
#             return pool
#         except Exception as e:
#             print(f"Error creating connection pool: {str(e)}")
#             return None
    
#     def get_connection(self):
#         """
#         Get a connection from the pool.
#         :return: A PostgreSQL connection
#         """
#         try:
#             return self.pool.getconn()
#         except Exception as e:
#             print(f"Error getting connection: {str(e)}")
#             return None
        
#     def release_connection(self, conn):
#         """
#         Release a connection back to the pool.
#         :param conn: The PostgreSQL connection to be released
#         """
#         try:
#             self.pool.putconn(conn)
#         except Exception as e:
#             print(f"Error releasing connection: {str(e)}")
            
#     def close_all(self):
#         """
#         Close all connections in the pool.
#         """
#         try:
#             self.pool.closeall()
#         except Exception as e:
#             print(f"Error closing all connections: {str(e)}")
    
#     def close(self, conn, cursor):
#         """
#         Close cursor and release connection.
#         :param conn: The PostgreSQL connection to be released
#         :param cursor: The cursor to be closed
#         """
#         cursor.close()
#         self.release_connection(conn)
        
#     def execute(self, sql, args=None, commit=False):
#         """
#         Execute a SQL command, optionally with arguments and commit.
#         :param sql: SQL statement to execute
#         :param args: Arguments for the SQL statement
#         :param commit: Whether to commit the transaction
#         :return: Result of the query if not committing, otherwise None
#         """
#         conn = self.get_connection()
#         if conn is None:
#             print("Failed to get connection")
#             return None
#         cursor = conn.cursor()
#         try:
#             if args:
#                 cursor.execute(sql, args)
#             else:
#                 cursor.execute(sql)
#             if commit:
#                 conn.commit()
#                 self.close(conn, cursor)
#                 self._reconnect_attempts = 0
#                 return None
#             else:
#                 res = cursor.fetchall()
#                 self.close(conn, cursor)
#                 self._reconnect_attempts = 0
#                 return res
#         except psycopg2.Error as e:
#             print(f"!! PostgresConnectionPool Error: {e}", flush=True)
#             return self.handle_error(conn, cursor, self.execute, sql, args=args, commit=commit)
#         except IndexError as e:
#             print(sql)
#             print(f"!! PostgresConnectionPool Error: {e}", flush=True)
#             return self.handle_error(conn, cursor, self.execute, sql, args=args, commit=commit)
    
#     def check_connections(self):
#         """
#         Check the connections in the pool to ensure they are still active.
#         Reconnect if necessary.
#         """
#         success = True
#         for i in range(self._pool_size):
#             conn = self.get_connection()
#             if conn:
#                 cursor = conn.cursor()
#                 try:
#                     cursor.execute("SELECT 1;")
#                     print(f"Connection {i+1} is active.")
#                 except psycopg2.Error:
#                     print(f"Connection {i+1} lost. Reconnecting...")
#                     success = False
#                     self.pool.putconn(conn, close=True)
#                     new_conn = self.pool.getconn()
#                     self.pool.putconn(new_conn)
#                     print(f"Connection {i+1} reestablished.")
#                 finally:
#                     self.close(conn,cursor)
#             else:
#                 print(f"Failed to get connection {i+1} for checking.")
#                 success = False

#         if success:
#             print("All connections are active and healthy.")
#         else:
#             print("Some connections were reestablished.")
                
#     def print_db(self):
#         print(self.__dict__, flush=True)
        
class MySQLPool(object):
    """
    based on: https://stackoverflow.com/questions/24374058/accessing-a-mysql-connection-pool-from-python-multiprocessing
    create a pool when connect mysql, which will decrease the time spent in 
    request connection, create connection and close connection.
    """
    def __init__(self, host="127.0.0.1", port="3306", user="root",
                 password="root", database="test", pool_name="sqes_pool",
                 pool_size=5, max_reconnect_attempts=3):
        res = {}
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_attempts = 0
        self._pool_name = pool_name
        self._pool_size = pool_size
        
        res["host"] = self._host
        res["port"] = self._port
        res["user"] = self._user
        res["password"] = self._password
        res["database"] = self._database
        self.dbconfig = res
        self.pool = self.create_pool(pool_name=pool_name, pool_size=pool_size)
        
    def create_pool(self, pool_name="sqes_pool", pool_size=3):
        """
        Create a connection pool, after created, the request of connecting 
        MySQL could get a connection from this pool instead of request to 
        create a connection.
        :param pool_name: the name of pool, default is "mypool"
        :param pool_size: the size of pool, default is 3
        :return: connection pool
        """
        pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            pool_reset_session=True,
            **self.dbconfig)
        return pool

    def close(self, conn, cursor):
        """
        A method used to close connection of mysql.
        :param conn: 
        :param cursor: 
        :return: 
        """
        cursor.close()
        conn.close()
        
    def execute(self, sql, args=None, commit=False):
        """
        Execute a sql, it could be with args and with out args. The usage is 
        similar with execute() function in module pymysql.
        :param sql: sql clause
        :param args: args need by sql clause
        :param commit: whether to commit
        :return: if commit, return None, else, return result
        """
        # get connection form connection pool instead of create one.
        conn = None
        cursor = None
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()  
            if args:
                cursor.execute(sql, args)
            else:
                cursor.execute(sql)
            if commit is True:
                conn.commit()
                self.close(conn, cursor)
                self._reconnect_attempts = 0
                return None
            else:
                res = cursor.fetchall()
                self.close(conn, cursor)
                self._reconnect_attempts = 0
                return res
        except mysql.connector.Error as e:
            print(f"!! MySQLPool Error: {e}", flush=True)
            return self.handle_error(conn, cursor, self.execute, sql, args=args, commit=commit)
        except IndexError as e:
            print(sql)
            print(f"!! IndexError: {e}", flush=True)
            return self.handle_error(conn, cursor, self.execute, sql, args=args, commit=commit)

    def executemany(self, sql, args, commit=False):
        """
        Execute with many args. Similar with executemany() function in pymysql.
        args should be a sequence.
        :param sql: sql clause
        :param args: args
        :param commit: commit or not.
        :return: if commit, return None, else, return result
        """
        # get connection form connection pool instead of create one.
        conn = None
        cursor = None
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()
            cursor.executemany(sql, args)
            if commit is True:
                conn.commit()
                self.close(conn, cursor)
                self._reconnect_attempts = 0
                return None
            else:
                res = cursor.fetchall()
                self.close(conn, cursor)
                self._reconnect_attempts = 0
                return res
        except mysql.connector.Error as e:
            print(f"!! MySQLPool Error: {e}", flush=True)
            return self.handle_error(conn, cursor, self.executemany, sql, args=args, commit=commit)
        except IndexError as e:
            print(sql)
            print(f"!! IndexError: {e}", flush=True)
            return self.handle_error(conn, cursor, self.executemany, sql, args=args, commit=commit)
                
    # connection checker
    def is_db_connected(self):
        try:
            conn = self.pool.get_connection()
            cursor = conn.cursor()
            # Example query to check connectivity
            cursor.execute("SELECT 1")
            cursor.fetchall() # Fetch results to ensure connection
            cursor.close()
            return True
        except mysql.connector.Error as e:
            print(f"!! MySQLPool Error: {e}", flush=True)
            return False
    
    def handle_error(self, conn, cursor, method, *args, **kwargs):
        """
        Handle connection errors by closing the current connection and cursor, 
        and attempting to reconnect. Limit the number of attempts.
        """
        if self._reconnect_attempts < self._max_reconnect_attempts:
            try:
                self._reconnect_attempts += 1
                self.close(conn, cursor)
                # Attempt to reconnect
                print(f"!! MySQLPool Attempting to reconnect... ({self._reconnect_attempts})", flush=True)
                time.sleep(60)
                self.pool = self.create_pool(pool_name=self._pool_name, pool_size=self._pool_size)
                # Re-run the original method
                return method(*args, **kwargs)
            except Exception as e:
                print(f"!! MySQLPool Error: Error while reconnecting: {e}", flush=True)
                # Wait for a while before attempting to reconnect again
                time.sleep(5)
                return self.handle_error(conn, cursor, method, *args, **kwargs)
        else:
            self.close(conn, cursor)
            print("!! MySQLPool Error: Exceeded maximum reconnect attempts.", flush=True)
            print("!! Warning, some data may be skipped !!", flush=True)
            
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
        psd = psd[(t>t0) & (t<t1)];
        model = model[(t>t0) & (t<t1)];
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
    def pct_model_period(psd,AHNM,t,t0,t1):
        percH=0
        psd = psd[(t>t0) & (t<t1)];
        AHNM = AHNM[(t>t0) & (t<t1)];
        for i in range(len(psd)):
            if psd[i] > AHNM[i]:
                percH += 1
        percH = round(float(percH*100/len(psd)),2)
        return percH

    @staticmethod
    def dead_channel_lin(psd,t,fs):
        #f dalam periode
        t0=0.1;t1=100.0;
        psd = psd[(t>t0) & (t<t1)];
        tn = t[(t>t0) & (t<t1)];
        tn = np.log10(tn)
        b,m = polyfit(tn,psd,1)
        psdfit = m + b * tn
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
    def prosess_matriks(files,data,time0,time1):
        tr=data.copy()
        tr.detrend()
        mseedqc = MSEEDMetadata([files],starttime=time0,endtime=time1)
        rms = mseedqc.meta['sample_rms']
        ampmax = mseedqc.meta['sample_max']
        ampmin = mseedqc.meta['sample_min']
        psdata = round(mseedqc.meta['percent_availability'],2)
        ngap = mseedqc.meta['num_gaps']
        nover = mseedqc.meta['num_overlaps']
        nd = mseedqc.meta['num_samples']
        num_spikes = 0.0
        for t in tr:
            num_spike = Calculation.cal_spikes(t.data,80,10)
            num_spikes += num_spike
        return rms,ampmax,ampmin,psdata,ngap,nover,num_spikes
    
    @staticmethod
    def prosess_psd(sig, inventory=None, output=''):
        NPZFNAME = '_{}.npz'
        data = sig.copy()
        if inventory is None:
            raise InventoryMissing('Please provide inventory file')
            return
        if data.count() == 0:
            raise DataMissing('No data in mseed file')
            return
        data.merge()
        ppsds = {}
        if data[0].stats.npts<=3600*data[0].stats.sampling_rate:
            return ppsds
        for tr in data:
            id_ = tr.id
            ppsds = PPSD(tr.stats, inventory)
            ppsds.add(tr)
        if output:
            fname_out = output + NPZFNAME.format(id_)
            ppsds.save_npz(fname_out) # type: ignore
        return ppsds
    
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
    def sql_execommit_analisqc(pool,kode,tanggal,percqc,kualitas,tipe,ket):
        if ket != 'Tidak ada data':
            ket=(', '.join(ket))
        sql =f"INSERT INTO tb_qcres (kode_res, tanggal_res, percqc, kualitas, tipe, keterangan) VALUES (\'{kode}\', \'{tanggal}\', \'{percqc}\', \'{kualitas}\', \'{tipe}\', \'{ket}\')"
        # print(sql)
        pool.execute(sql,commit=True)

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
    def QC_Analysis (pool,tanggal,station):
        # flush data in related date
        sql = f"SELECT * FROM tb_qcres WHERE tanggal_res = \'{tanggal}\' AND kode_res = \'{station}\'"
        # print(sql)
        data=pool.execute(sql)
        print(f"number of qcdata available: {len(data)}", flush=True)
        if data:
            print(f"! Data {station} on {tanggal} available, flushing database!", flush=True)
            sql = f"DELETE FROM tb_qcres WHERE tanggal_res = \'{tanggal}\' AND kode_res = \'{station}\'"
            pool.execute(sql,commit=True)
            print(f"! Data {station} on {tanggal} flush successful", flush=True)
        print(f"ready to fill database {station} on {tanggal}", flush=True)
    
    
        # Get station data
        # mycursor.execute("SELECT * FROM tb_slmon")
        sql=f"SELECT kode_sensor,lokasi_sensor,sistem_sensor FROM tb_slmon WHERE kode_sensor = \'{station}\'"
        station=pool.execute(sql)

        for sta in station:
            kode = sta[0]
            tipe = sta[2]
            
            # check if there is duplicate data
            sql_checker = f"SELECT * FROM tb_qcdetail WHERE tanggal = \'{tanggal}\' AND kode = \'{kode}\'"
            dataqc = pool.execute(sql_checker)
            
            # skip no data 
            if not dataqc:
                # print(f"!! <{tipe}> {kode} no data exist", flush=True)
                Analysis.sql_execommit_analisqc(pool,kode,tanggal,'0','Mati',tipe,'Tidak ada data')
                continue
            
            percqc=[]
            ket=[]
            for qc in dataqc:	
                komp = qc[4]
                # print(kode,komp,"processing")
                
                # rms calculation
                rms = float(qc[5])
                if rms > 1.0:
                    rms = Analysis.agregate(abs(rms),5000,10000)
                else:
                    rms = 0.0
                # ratio amp calculation
                ratioamp = float(qc[6])
                ratioamp = Analysis.agregate(ratioamp,1.01,2.0)
                # availability and gap calculation
                avail = float(qc[7])
                if avail >= 100.0:
                    ngap1 = 0
                    avail = 100.0
                else:
                    ngap1 = int(qc[8])
                ngap = Analysis.agregate(ngap1,0,4)
                # overlap calculation
                nover = int(qc[9])
                nover = Analysis.agregate(nover,0,4)
                # spikes calculation
                num_spikes = int(qc[10])
                num_spikes = Analysis.agregate(num_spikes,100,500)
                # pct calculation
                pct_above = float(qc[11])
                pct_below = float(qc[12])
                pct_noise = 100.0-pct_above-pct_below
                pct_noise = Analysis.agregate(pct_noise,100,60)
                # dead channel calculation
                dcl = float(qc[13])
                dcl = Analysis.agregate(dcl,2.0,-3.0)
                dcg = float(qc[14])
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
                    ket.append('Komponen '+komp+' Mati')
                else:
                # botqc calculation,
                    #botqc = 0.2*avail+0.1*rms+0.1*ratioamp+0.05*ngap+0.05*nover+0.25*pct_below+0.25*pct_above
                    botqc = 0.15*avail+0.15*rms+0.1*ratioamp+0.025*ngap+0.025*nover+0.3*pct_noise+0.125*dcl+0.125*dcg
                percqc.append(botqc)
                
            # generate keterangan if keterangan is empty
            if len(ket) == 0:
                ket.append('Tidak ada')
                    
            # generate general quality f station
            avg_percqc = np.sum(percqc)/3.0
            kualitas = Analysis.check_qc(avg_percqc)
            Analysis.sql_execommit_analisqc(pool,kode,tanggal,str(round(avg_percqc,2)),kualitas,tipe,ket)
            print(f"<{tipe}> {kode} QC FINISH", flush=True)
            time.sleep(0.5) #make res to the process
            
        # make res to the process
        time.sleep(0.5)