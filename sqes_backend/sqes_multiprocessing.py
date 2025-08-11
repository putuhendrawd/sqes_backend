import warnings
warnings.simplefilter("ignore", UserWarning) # obspy UserWarning ignore, use carefully
import matplotlib
matplotlib.use('Agg')
from pandas.plotting import register_matplotlib_converters; register_matplotlib_converters()
import os, sys, time, requests , signal
import numpy as np
import json
from obspy import UTCDateTime, read_inventory
from obspy.clients.fdsn import Client
from obspy.imaging.cm import pqlx
from sqes_function import Calculation, Analysis, DBPool, Config, Utils
from datetime import datetime
import multiprocessing
# from concurrent.futures import ThreadPoolExecutor


### function list ###
# verbose function
def vprint(*args):
    if verbose:
        print(*args, flush=True)

# timeout handling function
def handle_timeout(signum,frame):
    raise TimeoutError

# processes rounding function
def processes_round(x, base=2):
    min_value = 4
    max_value = multiprocessing.cpu_count() // 3
    rounded_value = base * round(x / base)
    if rounded_value < min_value:
        return min_value
    elif rounded_value > max_value:
        return max_value
    else:
        return rounded_value
    
# create directory function
def create_directory(dir_path):
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)
        vprint(f"{dir_path} created")
    else:
        vprint(f"{dir_path} exists")

def is_client_connected(client):
    try:
        # Try querying data to check connectivity
        client.get_events(starttime=UTCDateTime(2010,1,1,0,0,0), endtime=UTCDateTime(2010,1,2,0,1,0))
        return True
    except (requests.ConnectionError, requests.Timeout):
        return False

# default sql for bad data
def sql_default(db,id_kode,kode,tgl,cha,rms,ratioamp,psdata,ngap,nover,num_spikes):
    pct_above = 100
    pct_below = 0
    dead_channel_lin = 0
    dead_channel_gsn = 0
    diff20_100 = 0
    diff5_20 = 0
    diff5 = 0
    if db == 'mysql':
        query = f"INSERT INTO tb_qcdetail (id_kode, kode, tanggal, komp, rms, ratioamp, avail, ngap, nover, num_spikes, pct_above, pct_below, dead_channel_lin, dead_channel_gsn, diff20_100, diff5_20, diff5) VALUES (\'{id_kode}\', \'{kode}\', \'{tgl}\', \'{cha}\', \'{rms}\', \'{ratioamp}\', \'{psdata}\', \'{ngap}\', \'{nover}\', \'{num_spikes}\', \'{pct_above}\', \'{pct_below}\', \'{dead_channel_lin}\', \'{dead_channel_gsn}\', \'{diff20_100}\', \'{diff5_20}\', \'{diff5}\')"
    elif db == 'postgresql':
        query = f"INSERT INTO stations_qc_details (id, code, date, channel, rms, amplitude_ratio, availability, num_gap, num_overlap, num_spikes, perc_above_nhnm, perc_below_nlnm, linear_dead_channel, gsn_dead_channel, lp_percentage, bw_percentage, sp_percentage) VALUES (\'{id_kode}\', \'{kode}\', \'{tgl}\', \'{cha}\', {rms}, {ratioamp}, {psdata}, {ngap}, {nover}, {num_spikes}, {pct_above}, {pct_below}, {dead_channel_lin}, {dead_channel_gsn}, {diff20_100}, {diff5_20}, {diff5})"
    return query

# sql commit execute function
def sql_execommit(pool,db,id_kode,sistem_sensor,tgl,sql):
    if db == 'mysql':
        f = f"SELECT id_kode FROM tb_qcdetail WHERE id_kode= %s AND tanggal= %s;"
    elif db == 'postgresql':
        f = f"SELECT id FROM stations_qc_details WHERE id= %s AND date= %s;"
    data = pool.execute(f, args=(id_kode,tgl))
    # check if there is duplicate data
    if data:
        vprint(f"! <{sistem_sensor}> {id_kode} data exist:", data)
        vprint(f"! deleting previous data")
        if db == 'mysql':
            del_sql = f"DELETE FROM tb_qcdetail WHERE id_kode= %s AND tanggal= %s"
        elif db == 'postgresql':
            del_sql = f"DELETE FROM stations_qc_details WHERE id= %s AND date= %s"
        vprint("!",del_sql)
        pool.execute(del_sql, args=(id_kode,tgl), commit=True)
    pool.execute(sql, commit=True)

def process_data(sta):   
    # timeout function
    signal.signal(signal.SIGALRM, handle_timeout)
    db = basic_config['use_database']
    network = sta[0]
    kode = sta[1]
    location = sta[2]
    sistem_sensor = sta[3]
    channel_prefixes = sta[4].split(',')
    pool = DBPool(**db_credentials) # type: ignore
    channel = ['E','N','Z']
    print(f"<{sistem_sensor}> {kode} PROCESS START", flush=True)
    for ch in channel:
        # make labeler
        id_kode = f"{kode}_{ch}_{tgl}" # tgl from global var
        # vprint(id_kode)
        # download data
        sig, inv = Utils.download_data(client,network,kode,location,channel_prefixes,time0,time1,ch) # time0,time1 from global var
        # vprint(f"{id_kode} No Data" if sig=="No Data" else f"{id_kode} Downloaded")
        if sig=="No Data":
            sql=sql_default(db, id_kode,kode,tgl,ch,'0','0','0','1','0','0') # tgl from global var
            # vprint(sql)
            sql_execommit(pool,db,id_kode,sistem_sensor,tgl,sql) # tgl from global var
            print(f"!! {id_kode} No Data - Continuing", flush=True)
            time.sleep(0.5) #make res to the process
            continue
        else:
            vprint(f"{id_kode} Download complete")
        
        # saving exceptions
        try:
            signal.alarm(180)
            cha = sig[0].stats.channel
            fs = sig[0].stats.sampling_rate
            mseed_naming_code = f"{outputmseed}/{kode}_{cha[-1]}.mseed"
            sig.write(mseed_naming_code)
            sig.plot(outfile=f"{outputsignal}/{kode}_{cha[-1]}_signal.png")
            signal.alarm(0)
        except Exception as e:
            vprint(f"saving exception {kode} caught {type(e)}: {e}")
            sql=sql_default(db,id_kode,kode,tgl,ch,'0','0','0','1','0','0') # tgl from global var
            sql_execommit(pool,db,id_kode,sistem_sensor,tgl,sql) # tgl from global var
            print(f"!! {id_kode} Skip Processing with default parameter", flush=True)
            time.sleep(0.5) #make res to the process
            continue
        
        # process for basic info of the mseed
        try:
            vprint(f"{id_kode} Process basic info")
            rms,ampmax,ampmin,psdata,ngap,nover,num_spikes = Calculation.prosess_matriks(mseed_naming_code,sig,time0,time1) # time0,time1 from global var
        except Exception as e:
            sql=sql_default(db,id_kode,kode,tgl,ch,'0','0','0','1','0','0') # tgl from global var
            # vprint(sql)
            sql_execommit(pool,db,id_kode,sistem_sensor,tgl,sql) # tgl from global var
            vprint(f"basic info exception {kode} caught {type(e)}: {e}")
            print(f"!! {id_kode} miniseed basic process error - Skip Processing", flush=True)
            time.sleep(0.5) #make res to the process
            continue
        
        ampmax=abs(ampmax); ampmin=abs(ampmin); 
        ratioamp=Calculation.calculate_ratioamp(ampmin,ampmax)
        rms=str(round(rms,2)); ratioamp=str(round(ratioamp,2));psdata=str(round(psdata,2))
        ngap = str(int(ngap));nover = str(int(nover));num_spikes = str(int(num_spikes))
        
        # skip high gap data
        if int(ngap)>2000:
            sql=sql_default(db,id_kode,kode,tgl,cha,rms,ratioamp,psdata,ngap,nover,num_spikes) # tgl from global var
            # vprint("ngap except",sql)
            sql_execommit(pool,db,id_kode,sistem_sensor,tgl,sql)
            print(f"!! {id_kode} high gap - Continuing with default parameter", flush=True)
            time.sleep(.5) #make res to the process
            continue
        
        # processing ppsds
        if pdf_trigger:
            vprint(f"{id_kode} Process PPSDS")
            ppsds = Calculation.prosess_psd(sig,inv,output=outputPSD)
        else:
            vprint(f"{id_kode} Process PPSDS")
            ppsds = Calculation.prosess_psd(sig,inv,output='')
            
        # skip ppsds processing error
        if not ppsds or not ppsds._times_processed: # type: ignore
            sql=sql_default(db,id_kode,kode,tgl,cha,rms,ratioamp,psdata,ngap,nover,num_spikes) # tgl from global var
            # vprint(sql)
            sql_execommit(pool,db,id_kode,sistem_sensor,tgl,sql) # tgl from global var
            print(f"!! {id_kode} prosess_psd failed - Continuing without ppsd processing", flush=True)
            time.sleep(.5) #make res to the process
            continue
        
        # final parameter processing
        try:
            signal.alarm(1200) # add 20 min maximum processing for the final parameter processing
            vprint(f"{id_kode} Process final parameter")
            ppsds.plot(filename=f"{outputPDF}/{kode}_{cha[-1]}_PDF.png",cmap=pqlx,show=False) # type: ignore
            period, psd1 = ppsds.get_percentile() # type: ignore
            ind = period <= 100
            period = period[ind]
            psd1 = psd1[ind]
            powers = sorted(range(-190,-90+1), reverse=True)
            NHNM, NLNM, PInd = Calculation.get_models(period,powers)
            period = period[PInd]
            psd1 = psd1[PInd]
            dcg = Calculation.dead_channel_gsn(psd1,np.array(NLNM),period)
            pctH, pctL=Calculation.pct_model(psd1,NHNM,NLNM)
            pctH = str(pctH); pctL = str(pctL)
            diff20_100 = Calculation.pct_model_period(psd1,np.array(NLNM),np.array(NHNM),period,20,100)
            diff5_20 = Calculation.pct_model_period(psd1,np.array(NLNM),np.array(NHNM),period,5,20)
            diff5 = Calculation.pct_model_period(psd1,np.array(NLNM),np.array(NHNM),period,0.1,5)
            diff20_100 = str(diff20_100); diff5_20 = str(diff5_20); diff5 = str(diff5);
            period, psd1 = ppsds.get_mean() # type: ignore
            ind = period <= 100
            period = period[ind]
            psd1 = psd1[ind]
            dcl = Calculation.dead_channel_lin(psd1,period,fs)
            signal.alarm(0)
        except Exception as e:
            vprint(f"final parameter exception {kode} caught {type(e)}: {e}")
            print(f"!! {id_kode} processing final parameter error - Skip Processing with default parameter", flush=True)
            sql=sql_default(db,id_kode,kode,tgl,cha,rms,ratioamp,psdata,ngap,nover,num_spikes)
            sql_execommit(pool,db,id_kode,sistem_sensor,tgl,sql) # tgl from global var
            time.sleep(0.5) #make res to the process
            continue
        
        # commit result
        if db == 'mysql':
            sql = f"INSERT INTO tb_qcdetail (id_kode, kode, tanggal, komp, rms, ratioamp, avail, ngap, nover, num_spikes, pct_above, pct_below, dead_channel_lin, dead_channel_gsn, diff20_100, diff5_20, diff5) VALUES (\'{id_kode}\', \'{kode}\', \'{tgl}\', \'{cha}\', \'{rms}\', \'{ratioamp}\', \'{psdata}\', \'{ngap}\', \'{nover}\', \'{num_spikes}\', \'{pctH}\', \'{pctL}\', \'{str(round(dcl,2))}\', \'{str(round(dcg,2))}\', \'{diff20_100}\', \'{diff5_20}\', \'{diff5}\')"
        elif db == 'postgresql':
            sql = f"INSERT INTO stations_qc_details (id, code, date, channel, rms, amplitude_ratio, availability, num_gap, num_overlap, num_spikes, perc_above_nhnm, perc_below_nlnm, linear_dead_channel, gsn_dead_channel, lp_percentage, bw_percentage, sp_percentage) VALUES (\'{id_kode}\', \'{kode}\', \'{tgl}\', \'{cha}\', {rms}, {ratioamp}, {psdata}, {ngap}, {nover}, {num_spikes}, {pctH}, {pctL}, {str(round(dcl,2))}, {str(round(dcg,2))}, {diff20_100}, {diff5_20}, {diff5})"
        # vprint(sql)
        vprint(f"{id_kode} Saving to database")
        sql_execommit(pool,db,id_kode,sistem_sensor,tgl,sql) # tgl from global var
        vprint(f"{id_kode} Process finish")
        time.sleep(.5) #make res to the process
    # print process finish
    print(f"<{sistem_sensor}> {kode} PROCESS FINISH", flush=True)
    # run qc analysis
    Analysis.QC_Analysis(pool,db,tgl,kode) # tgl from global var
    time.sleep(.5) #make res to the process
### function end ###
    
if __name__ == "__main__":
    # multiprocessing set spawn
    # fix from: https://pythonspeed.com/articles/python-multiprocessing/
    # multiprocessing.set_start_method("spawn")
    
    # basic command prompt
    print(f"--- {sys.argv[0]} ---", flush=True)
    if len(sys.argv) < 2:
        print(
    f'''
    This script is used to re-download and re-analyze data from basic function olahqc_seismo.py 
    based on database availability by auto checking unavailable data

    How To Use:
    python3 sqes_multiprocessing.py <time> [verbose] [npz]
    time: (str) time format using %Y%m%d
    verbose: (str) will make verbose output (verbose = True) 
    npz: (str) will save matrix parameter as npz (numpy matrix)
    flush: (str) will flush entire data at the time selected and running it from zero

    Requirement:
    config.ini in "config" folder

    Running Directory: {os.getcwd()}
    ''', flush=True)
        exit()
    
    #verbose checker
    verbose = True if "verbose" in sys.argv else False
    print(f"Verbose: {verbose}", flush=True) 
    
    # save NPZ
    pdf_trigger = True if "npz" in sys.argv else False
    print(f"NPZ matrix saving: {pdf_trigger}", flush=True) 
    
    # flush data
    flush_data = True if "flush" in sys.argv else False
    print(f"Flush Data Mode: {flush_data}", flush=True)

    # datetime start 
    dt_start = datetime.now()
    print(f"running start at {dt_start}", flush=True)

    ## basic input
    try:
        wkt1 = time.strptime(sys.argv[1],"%Y%m%d")
        julday = wkt1.tm_yday
        tahun = wkt1.tm_year
        time0 = UTCDateTime(sys.argv[1])
        tgl = time0.strftime("%Y-%m-%d")
        time1 = time0 + 86400
    except:
        print(f"!! time input error : {sys.argv[1]}", flush=True)
        dt_end = datetime.now()
        print(f"running end at {dt_end}", flush=True)
        print(f"{sys.argv[0]} Running Complete ({dt_end-dt_start})", flush=True)
        exit()

    ## load credentials and config
    try:
        basic_config = Config.load_config(section='basic')
        db = basic_config['use_database']
        client_credentials = Config.load_config(section='client')
        db_credentials = Config.load_config(section=basic_config['use_database'])     
        
    except:
        print(f"!! client/db_credentials not found")
        dt_end = datetime.now()
        print(f"running end at {dt_end}", flush=True)
        print(f"{sys.argv[0]} Running Complete ({dt_end-dt_start})", flush=True)
        exit()

    # folder setup
    outputPSD = os.path.join(basic_config['outputpsd'],str(tahun),sys.argv[1])
    outputPDF = os.path.join(basic_config['outputpdf'],str(tgl))
    outputsignal = os.path.join(basic_config['outputsignal'],str(tgl))
    outputmseed = os.path.join(basic_config['outputmseed'],str(tgl))

    create_directory(outputsignal)
    create_directory(outputPDF)
    create_directory(outputmseed)
    create_directory(os.path.dirname(outputPSD))

    run_trigger = 1
    while run_trigger > 0 :
        ## data source
        client = Client(client_credentials['url'],user=client_credentials['user'],password=client_credentials['password'])
        print('client connected:',is_client_connected(client), flush=True)
        db_pool = DBPool(**db_credentials) # type: ignore
        db_pool.is_db_connected()
        
        if flush_data:
            vprint("flush qc_details data")
            if basic_config['use_database'] == 'mysql':
                flush_query = f"DELETE FROM tb_qcdetail WHERE tanggal = \'{tgl}\'"
            elif basic_config['use_database'] == 'postgresql':
                flush_query = f"DELETE FROM stations_qc_details WHERE date = \'{tgl}\'"
            db_pool.execute(flush_query, commit=True)
            
            if basic_config['use_database'] == 'mysql':
                flush_qcres_query = f"DELETE FROM tb_qcres WHERE tanggal_res = \'{tgl}\'"
            elif basic_config['use_database'] == 'postgresql':
                flush_qcres_query = f"DELETE FROM stations_data_quality WHERE date = \'{tgl}\'"
            db_pool.execute(flush_qcres_query, commit=True)
            vprint("flush success!")

        ## query for 'not downloaded' data
        if basic_config['use_database'] == 'mysql':
            db_query_a = f"WITH distinct_channels AS (SELECT DISTINCT code, location, SUBSTRING(channel, 1, 2) AS channel_prefix, CASE SUBSTRING(channel, 1, 2) WHEN 'SH' THEN 1 WHEN 'BH' THEN 2 WHEN 'HH' THEN 3 WHEN 'HN' THEN 4 ELSE 5 END AS sort_order FROM stations_sensor), aggregated_channels AS (SELECT code, location, GROUP_CONCAT(channel_prefix ORDER BY sort_order SEPARATOR ',') AS channel_prefixes FROM distinct_channels GROUP BY code, location), codes_to_exclude AS (SELECT code FROM stations_qc_details WHERE date = \'{tgl}\' GROUP BY code HAVING COUNT(code) = 3) SELECT s.network, s.code, ac.location, s.network_group, ac.channel_prefixes FROM stations AS s JOIN aggregated_channels AS ac ON s.code = ac.code LEFT JOIN codes_to_exclude AS cte ON s.code = cte.code WHERE cte.code IS NULL;"  
        elif basic_config['use_database'] == 'postgresql':
            db_query_a = f"WITH distinct_channels AS ( SELECT DISTINCT code, location, SUBSTRING(channel, 1, 2) AS channel_prefix, CASE SUBSTRING(channel, 1, 2) WHEN 'SH' THEN 1 WHEN 'BH' THEN 2 WHEN 'HH' THEN 3 WHEN 'HN' THEN 4 ELSE 5 END AS sort_order FROM stations_sensor ), aggregated_channels AS ( SELECT code, location, STRING_AGG(channel_prefix, ',' ORDER BY sort_order) AS channel_prefixes FROM distinct_channels GROUP BY code, location ), codes_to_exclude AS ( SELECT code FROM stations_qc_details WHERE date = \'{tgl}\' GROUP BY code HAVING COUNT(code) = 3 ) SELECT s.network, s.code, ac.location, s.network_group, ac.channel_prefixes FROM stations AS s JOIN aggregated_channels AS ac ON s.code = ac.code LEFT JOIN codes_to_exclude AS cte ON s.code = cte.code WHERE cte.code IS NULL;"
        vprint("query:",db_query_a)
        data = db_pool.execute(db_query_a)
        vprint(data)
        print(f"number of stations to be processed: {len(data)}", flush=True) # type: ignore

        ## number of processes determination
        if basic_config['cpu_number_used']:
            processes_req = basic_config['cpu_number_used']
        else:
            _ = len(data) // 35  # number maximum item per processes # type: ignore
            processes_req = processes_round(_)
        print(f"multiprocessing processes created: {processes_req}", flush=True)
        
        ## create mysql connection based on number of processes
        del(db_pool)
        # db_credentials['pool_size'] = 32 # process_req / # of pool
        db_pool = DBPool(**db_credentials) # type: ignore
        
        ########### multiprocessing block ###########
        if data:
            with multiprocessing.Pool(processes=processes_req) as pool: # type: ignore
                pool.map(process_data,data)
            # with ThreadPoolExecutor(max_workers=4) as executor:
            #     executor.map(process_data,data)
        else:
            print(f"Data {tgl} already complete", flush=True)
        ########### multiprocessing block ###########
        
        # update QC Analysis for data that are not auto downloaded by multiprocessing blocks
        print(f"Updating QC Data : {tgl}", flush=True)
        if basic_config['use_database'] == 'mysql':
            db_query_b = f"SELECT DISTINCT kode FROM tb_qcdetail WHERE tanggal=\'{tgl}\' AND kode NOT IN (SELECT DISTINCT kode_res FROM tb_qcres WHERE tanggal_res=\'{tgl}\')"
        elif basic_config['use_database'] == 'postgresql':
            db_query_b = f"SELECT DISTINCT code FROM stations_qc_details WHERE date=\'{tgl}\' AND code NOT IN (SELECT DISTINCT code FROM stations_data_quality WHERE date=\'{tgl}\')"
        vprint("query:",db_query_b)
        data = db_pool.execute(db_query_b)
        vprint(f"number of stations to be QC Analysis processed: {len(data)}") # type: ignore
        vprint(data)

        counter_qc=1
        if data:
            for sta in data:
                kode_qc = sta[0]
                vprint(f"{counter_qc}. {kode_qc}")
                Analysis.QC_Analysis(db_pool,db,tgl,kode_qc)
                counter_qc+=1   
        else:
            print(f"QC Data {tgl} already complete", flush=True)
            
        # check if all data already complete
        del(data)
        data_a = db_pool.execute(db_query_a)
        data_b = db_pool.execute(db_query_b)
        if (len(data_a) > 0) or (len(data_b) > 0): # type: ignore
            print(f"Some data may incompletely processed, running from begining! ({run_trigger})", flush=True)
            run_trigger+=1
            del(data_a,data_b)
        else:
            run_trigger=0
            del(data_a,data_b)

    # datetime end 
    dt_end = datetime.now()
    print(f"running end at {dt_end}", flush=True)
    print(f"{sys.argv[0]} Running Complete ({dt_end-dt_start})", flush=True)