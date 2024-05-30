import warnings
warnings.simplefilter("ignore", UserWarning) # obspy UserWarning ignore, use carefully
import matplotlib
matplotlib.use('Agg')
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

import os, mysql.connector, sys, time, glob, requests 
import numpy as np
import json
from obspy import UTCDateTime, read, read_inventory
from obspy.clients.fdsn import Client
from obspy.imaging.cm import pqlx
from sqes_v3_function import PSD_Calculation, Analysis
from datetime import datetime

print(f"--- {sys.argv[0]} ---")
### function list
# basic command prompt
if len(sys.argv) < 2:
    print(
f'''
This script is used to re-download and re-analyze data from basic function olahqc_seismo.py 
based on database availability by auto checking unavailable data

How To Use:
python3 olahqc_seismo_correction.py <time> [verbose] [npz]
time: (str) time format using %Y%m%d
verbose: (str) will make verbose = True 
npz: (str) will save matrix parameter as npz (numpy matrix)

Requirement:
credentials.json

Running Directory: {os.getcwd()}
'''
)
    exit()

# verbose function
verbose = True if "verbose" in sys.argv else False
def vprint(*args):
    if verbose:
        print(*args)
print(f"Verbose: {verbose}") 

# save NPZ
pdf_trigger = True if "npz" in sys.argv else False
print(f"NPZ matrix saving: {pdf_trigger}") 

# create directory function
def create_directory(dir_path):
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)
        vprint(f"{dir_path} created")
    else:
        vprint(f"{dir_path} exists")

# connection checker
def is_db_connected(connection):
    try:
        cursor = connection.cursor()
        # Example query to check connectivity
        cursor.execute("SELECT 1")
        cursor.fetchall() # Fetch results to ensure connection
        cursor.close()
        return True
    except mysql.connector.Error as e:
        print("Error:", e)
        return False

def is_client_connected(client):
    try:
        # Try querying data to check connectivity
        client.get_events(starttime=UTCDateTime(2010,1,1,0,0,0), endtime=UTCDateTime(2010,1,2,0,1,0))
        return True
    except (requests.ConnectionError, requests.Timeout):
        return False

# download data function
def DownloadData(client, sta, time0, time1, c):
    channel_codes = ["SH"+c, "BH"+c, "HH"+c]
    for channel_code in channel_codes:
        network = "*" if channel_code == f"BH{c}" else "IA"
        try:
            st = client.get_waveforms(network, sta, "*", channel_code, time0, time1)
            if st.count() > 0:
                try:
                    inv = read_inventory("https://geof.bmkg.go.id/fdsnws/station/1/query?station=" + sta + "&level=response&nodata=404")
                    return st, inv
                except:
                    return st, None
        except:
            continue
    return "No Data", None

# default sql for bad data
def sql_default(id_kode,kode,tgl,cha,rms,ratioamp,psdata,ngap,nover,num_spikes):
    pct_above = 100
    pct_below = 0
    dead_channel_lin = 0
    dead_channel_gsn = 0
    diff20_100 = 0
    diff5_20 = 0
    diff5 = 0
    prompt = f"INSERT INTO tb_qcdetail (id_kode, kode, tanggal, komp, rms, ratioamp, avail, ngap, nover, num_spikes, pct_above, pct_below, dead_channel_lin, dead_channel_gsn, diff20_100, diff5_20, diff5) VALUES (\'{id_kode}\', \'{kode}\', \'{tgl}\', \'{cha}\', \'{rms}\', \'{ratioamp}\', \'{psdata}\', \'{ngap}\', \'{nover}\', \'{num_spikes}\', \'{pct_above}\', \'{pct_below}\', \'{dead_channel_lin}\', \'{dead_channel_gsn}\', \'{diff20_100}\', \'{diff5_20}\', \'{diff5}\')"
    return prompt

def sql_execommit(cursor,db,id_kode,sql):
    f = f"SELECT id,id_kode FROM tb_qcdetail WHERE id_kode=\'{id_kode}';"
    cursor.execute(f)
    data = cursor.fetchall()
    # check if there is duplicate data
    if data:
        vprint(f"! <{sistem_sensor}> {id_kode} data exist:", data)
        vprint(f"! deleting previous data")
        del_sql = f"DELETE FROM tb_qcdetail WHERE id_kode=\'{id_kode}\'"
        vprint("!",del_sql)
        cursor.execute(del_sql);
        db.commit()
    cursor.execute(sql);
    db.commit()
    
def calculate_ratioamp(ampmin,ampmax):
    if ampmax > ampmin:
        return ampmax/ampmin
    elif ampmax == 0 or ampmin == 0:
        return 1.0
    else:
        return ampmin/ampmax
### function end

# datetime start 
dt_start = datetime.now()
vprint(f"running start at {dt_start}")

## basic input
try:
    wkt1 = time.strptime(sys.argv[1],"%Y%m%d")
    julday = wkt1.tm_yday
    tahun = wkt1.tm_year
    time0 = UTCDateTime(sys.argv[1])
    tgl = time0.strftime("%Y-%m-%d")
    time1 = time0 + 86400
except:
    print(f"time input error : {sys.argv[1]}")
    dt_end = datetime.now()
    vprint(f"running end at {dt_end}")
    print(f"{sys.argv[0]} Running Complete ({dt_end-dt_start})")
    exit()

## load credentials
try:
    with open("credentials.json") as f:
        credentials = json.load(f)
except:
    print(f"credentials.json not found")
    dt_end = datetime.now()
    vprint(f"running end at {dt_end}")
    print(f"{sys.argv[0]} Running Complete ({dt_end-dt_start})")
    exit()

## folder setup
outputPSD = '/home/idripsensor/QCDATA/PSD/'+str(tahun)+'/'+sys.argv[1]
outputPDF = '/var/www/html/dataqc/PDFimage/'+tgl
dirsignal = '/var/www/html/dataqc/signal/'+tgl
outputmseed = '/home/idripsensor/QCDATA/mseed/'+tgl

create_directory(dirsignal)
create_directory(outputPDF)
create_directory(outputmseed)
create_directory(os.path.dirname(outputPSD))

## data source
client = Client(credentials['client_url'],user=credentials['client_user'],password=credentials['client_password'])
vprint('client connected:',is_client_connected(client))
mydb = mysql.connector.connect(
  host=credentials['db_host'],
  user=credentials['db_user'],
  password=credentials['db_password'],
  database=credentials['db_database']
)
mycursor = mydb.cursor()
vprint('db connected:',is_db_connected(mydb))

## query for 'not downloaded' data
db_query = f"SELECT kode_sensor,sistem_sensor FROM tb_slmon WHERE kode_sensor NOT IN (SELECT kode FROM (SELECT DISTINCT kode, COUNT(kode) AS ccode FROM tb_qcdetail WHERE tanggal=\'{tgl}\' GROUP BY kode) AS o WHERE o.ccode = 3) LIMIT 0,3"
vprint("query:",db_query)
mycursor.execute(db_query)
data=mycursor.fetchall()
print("number of stations to be processed:", len(data))
vprint(data)

#iter counter
counter = 1
if data:
    for sta in data:
        kode = sta[0]
        sistem_sensor = sta[1]
        channel = ['E','N','Z']
        vprint(f"{counter}. <{sistem_sensor}> {kode}")
        for ch in channel:
            # make labeler
            id_kode = f"{kode}_{ch}_{tgl}"
            
            # download data
            sig, inv = DownloadData(client,kode,time0,time1,ch)
            vprint(f"{id_kode} No Data" if sig=="No Data" else f"{id_kode} Downloaded")
            if sig=="No Data":
                sql=sql_default(id_kode,kode,tgl,ch,'0','0','0','1','0','0')
                vprint(sql)
                sql_execommit(mycursor,mydb,id_kode,sql)
                print(f"{id_kode} No Data - Continuing")
                continue
            cha = sig[0].stats.channel
            fs = sig[0].stats.sampling_rate
            mseed_naming_code = f"{outputmseed}/{kode}_{cha[-1]}.mseed"
            sig.write(mseed_naming_code)
            sig.plot(outfile=f"{dirsignal}/{kode}_{cha[-1]}_signal.png")
            
            # process for basic info of the mseed
            try:
                rms,ampmax,ampmin,psdata,ngap,nover,num_spikes = PSD_Calculation.prosess_matriks(mseed_naming_code,sig,time0,time1)
            except:
                sql=sql_default(id_kode,kode,tgl,ch,'0','0','0','1','0','0')
                vprint(sql)
                sql_execommit(mycursor,mydb,id_kode,sql)
                print(f"{id_kode} miniseed basic process error - Skip Processing")
                continue
            
            ampmax=abs(ampmax); ampmin=abs(ampmin); 
            ratioamp=calculate_ratioamp(ampmin,ampmax)
            rms=str(round(rms,2)); ratioamp=str(round(ratioamp,2));psdata=str(round(psdata,2))
            ngap = str(int(ngap));nover = str(int(nover));num_spikes = str(int(num_spikes))
            
            # skip high gap data
            if int(ngap)>2000:
                sql=sql_default(id_kode,kode,tgl,cha,rms,ratioamp,psdata,ngap,nover,num_spikes)
                vprint(sql)
                sql_execommit(mycursor,mydb,id_kode,sql)
                print(f"{id_kode} high gap - Continuing with default parameter")
                continue
            
            # processing ppsds
            if pdf_trigger:
                ppsds = PSD_Calculation.prosess_psd(sig,inv,output=outputPSD)
            else:
                ppsds = PSD_Calculation.prosess_psd(sig,inv,output=None)
                
            # skip ppsds processing error
            if not ppsds or not ppsds._times_processed:
                sql=sql_default(id_kode,kode,tgl,cha,rms,ratioamp,psdata,ngap,nover,num_spikes)
                vprint(sql)
                sql_execommit(mycursor,mydb,id_kode,sql)
                print(f"{id_kode} prosess_psd failed - Continuing without ppsd processing")
                continue
            
            ppsds.plot(filename=f"{outputPDF}/{kode}_{cha[-1]}_PDF.png",cmap=pqlx,show=False)
            period, psd1 = ppsds.get_percentile()
            ind = period <= 100
            period = period[ind]
            psd1 = psd1[ind]
            powers = sorted(range(-190,-90+1), reverse=True)
            NHNM, NLNM, PInd = PSD_Calculation.get_models(period,powers)
            period = period[PInd]
            psd1 = psd1[PInd]
            dcg = PSD_Calculation.dead_channel_gsn(psd1,np.array(NLNM),period)
            pctH, pctL=PSD_Calculation.pct_model(psd1,NHNM,NLNM)
            pctH = str(pctH); pctL = str(pctL)
            diff20_100 = PSD_Calculation.pct_model_period(psd1,np.array(NHNM),period,20,100)
            diff5_20 = PSD_Calculation.pct_model_period(psd1,np.array(NHNM),period,5,20)
            diff5 = PSD_Calculation.pct_model_period(psd1,np.array(NHNM),period,0.1,5)
            diff20_100 = str(diff20_100); diff5_20 = str(diff5_20); diff5 = str(diff5);
            period, psd1 = ppsds.get_mean()
            ind = period <= 100
            period = period[ind]
            psd1 = psd1[ind]
            dcl = PSD_Calculation.dead_channel_lin(psd1,period,fs)
            
            # commit result
            sql = f"INSERT INTO tb_qcdetail (id_kode, kode, tanggal, komp, rms, ratioamp, avail, ngap, nover, num_spikes, pct_above, pct_below, dead_channel_lin, dead_channel_gsn, diff20_100, diff5_20, diff5) VALUES (\'{id_kode}\', \'{kode}\', \'{tgl}\', \'{cha}\', \'{rms}\', \'{ratioamp}\', \'{psdata}\', \'{ngap}\', \'{nover}\', \'{num_spikes}\', \'{pctH}\', \'{pctL}\', \'{str(round(dcl,2))}\', \'{str(round(dcg,2))}\', \'{diff20_100}\', \'{diff5_20}\', \'{diff5}\')"
            vprint(sql)
            sql_execommit(mycursor,mydb,id_kode,sql)
            print(f"{id_kode} Process finish")
            
        # run qc analysis
        Analysis.QC_Analysis(mycursor,mydb,tgl,kode)
        counter+=1
else:
    vprint(f"Data {tgl} already complete")
    
# update QC Analysis for not-downloading data above
vprint(f"Updating QC Data : {tgl}")
db_query = f"SELECT DISTINCT kode FROM tb_qcdetail WHERE tanggal=\'{tgl}\' AND kode NOT IN (SELECT DISTINCT kode_res FROM tb_qcres WHERE tanggal_res=\'{tgl}\')"
vprint("query:",db_query)
mycursor.execute(db_query)
data=mycursor.fetchall()
print("number of stations to be QC Analysis processed:", len(data))
vprint(data)

counter_qc=1
if data:
    for sta in data:
        kode_qc = sta[0]
        vprint(f"{counter_qc}. {kode_qc}")
        Analysis.QC_Analysis(mycursor,mydb,tgl,kode_qc)
        counter_qc+=1   
else:
    vprint(f"QC Data {tgl} already complete")

# datetime end 
dt_end = datetime.now()
vprint(f"running end at {dt_end}")
print(f"{sys.argv[0]} Running Complete ({dt_end-dt_start})")

# # auto running analisqc_seismo_correction after this
# os.system(f"python3 analisqc_seismo_correction.py {tgl} verbose")