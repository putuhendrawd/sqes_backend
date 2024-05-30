import os, mysql.connector, sys, time, glob
import numpy as np
from datetime import datetime
import json

print(f"--- {sys.argv[0]} ---")
### function list

# basic command prompt
if len(sys.argv) < 2:
    print(
f'''
This script is used to re-analyze data collected from olahqc_seismo_correction
output result to database connected via credentials.json

How To Use:
python3 analisqc_seismo_correction.py <time> [verbose]
time: (str) time format using %Y-%m-%d
verbose: (str) will make verbose = True 

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

# sql execute and commit function
def sql_execommit_analisqc(cursor,db,kode,tanggal,percqc,kualitas,tipe,ket):
    if ket != 'Tidak ada data':
        ket=(', '.join(ket))
    sql =f"INSERT INTO tb_qcres (kode_res, tanggal_res, percqc, kualitas, tipe, keterangan) VALUES (\'{kode}\', \'{tanggal}\', \'{percqc}\', \'{kualitas}\', \'{tipe}\', \'{ket}\')"
    vprint(sql)
    cursor.execute(sql)
    db.commit()

def agregate(par,lim,m):
	grade=100.0-(15.0*(par-lim)/m)
	if grade > 100.0:
		grade=100.0
	elif grade < 0.0:
		grade=0.0
	return grade

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
### function end

# datetime start 
dt_start = datetime.now()
vprint(f"running start at {dt_start}")

## basic input
try:
    tanggal = datetime.strptime(sys.argv[1],"%Y-%m-%d").strftime("%Y-%m-%d")
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

## sumber data
mydb = mysql.connector.connect(
  host=credentials['db_host'],
  user=credentials['db_user'],
  password=credentials['db_password'],
  database=credentials['db_database']
)
mycursor = mydb.cursor()
vprint('db connected:',is_db_connected(mydb))

# flush data in related date
sql = f"SELECT * FROM tb_qcres WHERE tanggal_res = \'{tanggal}\'"
vprint(sql)
mycursor.execute(sql)
data=mycursor.fetchall()
print("number of qcdata available", len(data))
if data:
    print(f"Data {tanggal} available, flushing database!")
    mycursor.execute(f"DELETE FROM tb_qcres WHERE tanggal_res = \'{tanggal}\'")
    mydb.commit()	
    print(f"Data {tanggal} flush successful")
print(f"ready to fill database {tanggal} ")
    
    
#Get data stasiun
# mycursor.execute("SELECT * FROM tb_slmon")
mycursor.execute("SELECT kode_sensor,lokasi_sensor,sistem_sensor FROM tb_slmon")
station=mycursor.fetchall()

for sta in station:
    kode = sta[0]
    nm = sta[1]
    tipe = sta[2]
    vprint(f"! <{tipe}> {kode}")
    
	# check if there is duplicate data
    sql_checker = f"SELECT * FROM tb_qcdetail WHERE tanggal = \'{tanggal}\' AND kode = \'{kode}\'"
    mycursor.execute(sql_checker)
    dataqc = mycursor.fetchall()
    
    # skip no data 
    if not dataqc:
        vprint(f"! <{tipe}> {kode} no data exist")
        sql_execommit_analisqc(mycursor,mydb,kode,tanggal,'0','Mati',tipe,'Tidak ada data')
        continue
    
    percqc=[]
    ket=[]
    for qc in dataqc:	
        komp = qc[4]
        vprint(kode,komp,"processing")
        
        # rms calculation
        rms = float(qc[5])
        if rms > 1.0:
            rms = agregate(abs(rms),5000,10000)
        else:
            rms = 0.0
        # ratio amp calculation
        ratioamp = float(qc[6])
        ratioamp = agregate(ratioamp,1.01,2.0)
        # availability and gap calculation
        avail = float(qc[7])
        if avail >= 100.0:
            ngap1 = 0
            avail = 100.0
        else:
            ngap1 = int(qc[8])
        ngap = agregate(ngap1,0,4)
        # overlap calculation
        nover = int(qc[9])
        nover = agregate(nover,0,4)
        # spikes calculation
        num_spikes = int(qc[10])
        num_spikes = agregate(num_spikes,100,500)
        # pct calculation
        pct_above = float(qc[11])
        pct_below = float(qc[12])
        pct_noise = 100.0-pct_above-pct_below
        pct_noise = agregate(pct_noise,100,60)
        # dead channel calculation
        dcl = float(qc[13])
        dcl = agregate(dcl,2.0,-3.0)
        dcg = float(qc[14])
        dcg = agregate(dcg,1.0,1.0)
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
        # generate keterangan 
        if not ket:
            ket.append('Tidak ada')
# generate general quality f station
    avg_percqc = np.sum(percqc)/3.0
    kualitas = check_qc(avg_percqc)
    sql_execommit_analisqc(mycursor,mydb,kode,tanggal,str(round(avg_percqc,2)),kualitas,tipe,ket)
    print(f"<{tipe}> {kode} Process finish")
# datetime end
dt_end = datetime.now()
vprint(f"running end at {dt_end}")
print(f"{sys.argv[0]} Running Complete ({dt_end-dt_start})")