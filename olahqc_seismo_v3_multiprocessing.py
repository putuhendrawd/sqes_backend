import os
import json
import mysql.connector
import time
import logging
import sys
from multiprocessing import Pool
from obspy import UTCDateTime
from obspy.clients.fdsn import Client
import requests

# Constants
CREDENTIALS_FILE = "credentials.json"
LOG_FILE = "qc_data.log"
VERBOSE = True

# Configure logging
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Function to load credentials from JSON file
def load_credentials(file_path):
    with open(file_path) as f:
        return json.load(f)

# Function to create directories if they do not exist
def create_directories(*dirs):
    for dir_path in dirs:
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        else:
            logging.info(f"{dir_path} exists")

# Function to check if the database connection is valid
def is_db_connected(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchall()
        cursor.close()
        return True
    except mysql.connector.Error as e:
        logging.error("Database connection error:", e)
        return False

# Function to check if the FDSN client is connected
def is_client_connected(client):
    try:
        client.get_events(starttime=UTCDateTime(2010,1,1,0,0,0), endtime=UTCDateTime(2010,1,2,0,1,0))
        return True
    except (requests.ConnectionError, requests.Timeout):
        return False

# Processing 
def prosess_psd(sig, inventory=None, output=''):
	NPZFNAME = '_{}.npz'
	data = sig.copy()
	#try:
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
	fname_out = output + NPZFNAME.format(id_)
	ppsds.save_npz(fname_out)
	#os.system('mv '+fname_out+' /home/sysop/PSD/'+station)
	return ppsds

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

def pct_model_period(psd,AHNM,t,t0,t1):
	percH=0
	psd = psd[(t>t0) & (t<t1)];
	AHNM = AHNM[(t>t0) & (t<t1)];
	for i in range(len(psd)):
		if psd[i] > AHNM[i]:
			percH += 1
	percH = round(float(percH*100/len(psd)),2)
	return percH

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
		num_spike = cal_spikes(t.data,80,10)
		num_spikes += num_spike
	return rms,ampmax,ampmin,psdata,ngap,nover,num_spikes

def dead_channel_gsn(psd,model,t,t0=4.0,t1=8.0):
	#f dalam periode
	psd = psd[(t>t0) & (t<t1)];
	model = model[(t>t0) & (t<t1)];
	dcg = np.mean(model-psd)
	return dcg

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

# Function to download data from the FDSN client
def download_data(client, sta, time0, time1, c):
    channel_codes = ["SH"+c, "BH"+c, "HH"+c]
    for channel_code in channel_codes:
        network = "*" if channel_code == f"BH{c}" else "IA"
        try:
            st = client.get_waveforms(network, sta, "*", channel_code, time0, time1)
            if st.count() > 0:
                try:
                    inv = read_inventory("https://geof.bmkg.go.id/fdsnws/station/1/query?station=" + sta + "&level=response&nodata=404")
                    return st, inv
                except Exception as e:
                    logging.error("Error reading inventory:", e)
                    return st, None
        except Exception as e:
            logging.error("Error downloading data:", e)
            continue
    return "No Data", None

# Function to insert data into the database
def insert_data(args):
    mycursor, mydb, data, time0, time1 = args
    sta, tgl, client, outputPDF, dirsignal, outputPSD, outputmseed = data
    kode, cha = sta
    id_kode = f"{kode}_{cha}_{tgl}"
    sig, inv = download_data(client, kode, time0, time1, cha)
    if sig == "No Data":
        insert_data_single(mycursor, mydb, id_kode, kode, tgl, cha, '0', '0', '0', '1', '0', '0', '100', '0', '0', '0', '0', '0', '0')
        return
    cha = sig[0].stats.channel
    fs = sig[0].stats.sampling_rate
    f = f"{outputmseed}/{kode}_{cha[-1]}.mseed"
    sig.write(f)
    rms, ampmax, ampmin, psdata, ngap, nover, num_spikes = process_matriks(f, sig, time0, time1)
    ampmax = abs(ampmax)
    ampmin = abs(ampmin)
    ratioamp = ampmax / ampmin if ampmax > ampmin else ampmin / ampmax if ampmax != 0 and ampmin != 0 else 1.0
    rms = round(rms, 2)
    ratioamp = round(ratioamp, 2)
    psdata = round(psdata, 2)
    ngap = int(ngap)
    nover = int(nover)
    num_spikes = int(num_spikes)
    if ngap > 2000:
        insert_data_single(mycursor, mydb, id_kode, kode, tgl, cha, rms, ratioamp, psdata, ngap, nover, num_spikes, '100', '0', '0', '0', '0', '0')
        return
    ppsds = prosess_psd(sig, inv, output=outputPSD)
    if not ppsds or not ppsds._times_processed:
        insert_data_single(mycursor, mydb, id_kode, kode, tgl, cha, rms, ratioamp, psdata, ngap, nover, num_spikes, '100', '0', '0', '0', '0', '0')
        return
    ppsds.plot(filename=f"{outputPDF}/{kode}_{cha[-1]}_PDF.png", cmap=pqlx, show=False)
    sig.plot(outfile=f"{dirsignal}/{kode}_{cha[-1]}_signal.png", show=False)
    period, psd1 = ppsds.get_percentile()
    ind = period <= 100
    period = period[ind]
    psd1 = psd1[ind]
    powers = sorted(range(-190, -90 + 1), reverse=True)
    NHNM, NLNM, PInd = get_models(period, powers)
    period = period[PInd]
    psd1 = psd1[PInd]
    dcg = dead_channel_gsn(psd1, np.array(NLNM), period)
    pctH, pctL = pct_model(psd1, NHNM, NLNM)
    pctH = str(pctH)
    pctL = str(pctL)
    diff20_100 = pct_model_period(psd1, np.array(NHNM), period, 20, 100)
    diff5_20 = pct_model_period(psd1, np.array(NHNM), period, 5, 20)
    diff5 = pct_model_period(psd1, np.array(NHNM), period, 0.1, 5)
    diff20_100 = str(diff20_100)
    diff5_20 = str(diff5_20)
    diff5 = str(diff5)
    period, psd1 = ppsds.get_mean()
    ind = period <= 100
    period = period[ind]
    psd1 = psd1[ind]
    dcl = dead_channel_lin(psd1, period, fs)
    insert_data_single(mycursor, mydb, id_kode, kode, tgl, cha, rms, ratioamp, psdata, ngap, nover, num_spikes, pctH, pctL, round(dcl, 2), round(dcg, 2), diff20_100, diff5_20, diff5)

# Function to insert data for a single entry into the database
def insert_data_single(mycursor, mydb, id_kode, kode, tgl, cha, rms, ratioamp, psdata, ngap, nover, num_spikes, pctH, pctL, dcl, dcg, diff20_100, diff5_20, diff5):
    sql = """INSERT INTO tb_qcdetail 
             (id_kode, kode, tanggal, komp, rms, ratioamp, avail, ngap, nover, num_spikes, pct_above, pct_below, dead_channel_lin, dead_channel_gsn, diff20_100, diff5_20, diff5) 
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    values = (id_kode, kode, tgl, cha, rms, ratioamp, psdata, ngap, nover, num_spikes, pctH, pctL, dcl, dcg, diff20_100, diff5_20, diff5)
    mycursor.execute(sql, values)
    mydb.commit()

# Function to process data
def process_data(data, mycursor, mydb, time0, time1):
    client = Client(credentials['client_url'], user=credentials['client_user'], password=credentials['client_password'])
    logging.info(f'Client connected: {is_client_connected(client)}')
    mydb = mysql.connector.connect(
        host=credentials['db_host'],
        user=credentials['db_user'],
        password=credentials['db_password'],
        database=credentials['db_database']
    )
    logging.info(f'Database connected: {is_db_connected(mydb)}')
    mycursor = mydb.cursor()

    pool_args = [(mycursor, mydb, data_entry, time0, time1) for data_entry in data]
    with Pool() as pool:
        pool.map(insert_data, pool_args)

    mycursor.close()
    mydb.close()

# Main function
if __name__ == '__main__':
    # Input Processing
    input_dt = time.strptime(sys.argv[1], "%Y%m%d")
    julday = input_dt.tm_yday
    tahun = input_dt.tm_year
    time0 = UTCDateTime(sys.argv[1])
    tgl = time0.strftime("%Y-%m-%d")
    time1 = time0 + 86400

    credentials = load_credentials(CREDENTIALS_FILE)

    # Generate config dynamically based on input
    outputPSD = f"/home/idripsensor/QCDATA/PSD/{tahun}/{sys.argv[1]}"
    outputPDF = f"/var/www/html/dataqc/PDFimage/{tgl}"
    dirsignal = f"/var/www/html/dataqc/signal/{tgl}"
    outputmseed = f"/home/idripsensor/QCDATA/mseed/{tgl}"

    create_directories(outputPSD, outputPDF, dirsignal, os.path.dirname(outputPSD))

    client = Client(credentials['client_url'], user=credentials['client_user'], password=credentials['client_password'])
    logging.info(f'Client connected: {is_client_connected(client)}')

    mydb = mysql.connector.connect(
        host=credentials['db_host'],
        user=credentials['db_user'],
        password=credentials['db_password'],
        database=credentials['db_database']
    )
    logging.info(f'Database connected: {is_db_connected(mydb)}')

    mycursor = mydb.cursor()
    mycursor.execute(f"SELECT kode_sensor, sistem_sensor FROM tb_slmon WHERE kode_sensor NOT IN (SELECT DISTINCT kode FROM tb_qcdetail WHERE tanggal='{tgl}') LIMIT 0,10")
    data = mycursor.fetchall()
    logging.info(f"Data: {data}")
    logging.info(f"Data type: {type(data[0])}")

    process_data(data, mycursor, mydb, time0, time1)
