# sqes/analysis/qc_analyzer.py
import time
import logging
import numpy as np
from sqes.services.repository import QCRepository

logger = logging.getLogger(__name__)

def _agregate(par, lim, m):
    """
    Private helper for grading logic.
    Calculates a score based on how far a parameter is from its limit.
    """
    grade = 100.0 - (15.0 * (par - lim) / m)
    if grade > 100.0:
        grade = 100.0
    elif grade < 0.0:
        grade = 0.0
    return grade

def _check_qc(percqc):
    """Private helper to assign a quality string based on the final score."""
    if percqc >= 90.0:
        kualitas = 'Baik'
    elif percqc >= 60.0:
        kualitas = 'Cukup Baik'
    elif percqc == 0.0:
        kualitas = 'Mati'
    else:
        kualitas = 'Buruk'
    return kualitas

def run_qc_analysis(repo: QCRepository, db_type: str, tanggal: str, station_code: str):
    """
    Main QC Analysis function.
    Fetches all component data for a station and calculates a final grade.
    """
    # 1. Flush any existing analysis data for this station/day
    try:
        repo.flush_analysis_result(tanggal, station_code)
        logger.debug(f"Ready to fill analysis for {station_code} on {tanggal}")
    except Exception as e:
        logger.error(f"Failed to flush analysis for {station_code}: {e}")

        return # Cannot proceed

    # 2. Get station info (e.g., 'tipe' or 'network_group')
    station_info = repo.get_station_info(station_code)
    
    if not station_info:
        logger.warning(f"<{station_code}> No station info found in database. Skipping analysis.")
        return

    # This loop will only run once, but it's an easy way to unpack the data
    for sta in station_info:
        network = sta[0]
        kode = sta[1]
        tipe = sta[3]

        # 3. Get all QC details for this station and day
        dataqc = repo.get_qc_details_for_station(tanggal, kode)
        
        if not dataqc:
            logger.warning(f"<{tipe}> {kode} no QC detail data exist, logging as 'Mati'")
            repo.insert_qc_analysis_result(kode, tanggal, '0', 'Mati', tipe, ['Tidak ada data'])
            continue
        
        percqc_list = []
        ket = [] 
        
        # 4. Loop through each component (E, N, Z)
        for qc_row in dataqc:
            # 5. Map columns based on DB type
            try:
                if db_type == 'mysql':
                    komp = qc_row[4]
                    rms = float(qc_row[5])
                    ratioamp = float(qc_row[6])
                    avail = float(qc_row[7])
                    ngap1 = int(qc_row[8])
                    nover = int(qc_row[9])
                    num_spikes = int(qc_row[10])
                    pct_above = float(qc_row[11])
                    pct_below = float(qc_row[12])
                    dcl = float(qc_row[13])
                    dcg = float(qc_row[14])
                elif db_type == 'postgresql':
                    komp = qc_row[3]
                    rms = float(qc_row[4])
                    ratioamp = float(qc_row[5])
                    avail = float(qc_row[6])
                    ngap1 = int(qc_row[7])
                    nover = int(qc_row[8])
                    num_spikes = int(qc_row[9])
                    pct_below = float(qc_row[10])
                    pct_above = float(qc_row[11])
                    dcl = float(qc_row[12])
                    dcg = float(qc_row[13]) # This is the binary 0 or 1
            except (ValueError, TypeError, IndexError) as e:
                logger.error(f"Error parsing QC row for {kode}: {e}. Row: {qc_row}")
                continue 
            
            # 6. Perform grading logic
            # rms
            if rms > 1.0:
                rms_grade = _agregate(abs(rms), 5000, 7500) # update 90th percentile threshold (Ringler 2015)
            else:
                rms_grade = 0.0 # Bad sensor
            # availability
            if avail >= 100.0: 
                ngap1 = 0
                avail = 100.0
            # ratioamp
            ratioamp_grade = _agregate(ratioamp, 1.01, 2.02) # update 90th percentile threshold (Ringler 2015)
            # ngap
            ngap_grade = _agregate(ngap1, 0.00274, 0.992) # QuARG
            # nover
            nover_grade = _agregate(nover, 0, 1.25) # update 90th percentile threshold
            # num_spikes
            num_spikes_grade = _agregate(num_spikes, 0, 25) # update 90th percentile threshold (Ringler 2015)
            # pct_noise
            pct_noise = 100.0 - pct_above - pct_below # % of PSD within NHNM/NLNM bounds
            # dcl
            dcl_grade = _agregate(dcl, 9.0, -1.0) # update 90th percentile threshold (Ringler 2015)
                
            # 7. Calculate final weighted score
            if avail <= 0.0:
                botqc = 0.0
                ket.append(f'Komponen {komp} Mati')
            elif dcg == 1 or dcl <= 2.25: # QuARG
                botqc = 1.0
                ket.append(f'Komponen {komp} tidak merespon getaran')
            elif rms < 1 and rms > 0:
                botqc = 1.0
                ket.append(f'Komponen {komp} Rusak')
            else:
                # 8. Weighted average for this component
                botqc = (0.35 * pct_noise + 0.15 * avail + 0.1 * rms_grade + 0.1 * ratioamp_grade + 
                         0.1 * ngap_grade + 0.1 * nover_grade + 0.1 * num_spikes_grade)

                # Generate 'keterangan' (details)
                if pct_below > 20.0:
                    ket.append(f"Cek metadata komponen {komp}")
                elif ngap1 > 5:
                    ket.append(f"Terlalu banyak gap pada komponen {komp}")
                elif nover > 5:
                    ket.append(f"Terlalu banyak overlap pada komponen {komp}")
                elif pct_above > 20 and avail >= 10.0:
                    ket.append(f"Noise tinggi di komponen {komp}")
                elif num_spikes > 25:
                    ket.append(f"Spike berlebihan pada komponen {komp}")
                elif (avail < 97) and (avail >= 50):
                    ket.append(f"Availability rendah pada komponen {komp}")
                    botqc = max(botqc, 89.0)
                elif (avail < 50) and (avail > 0):
                    ket.append(f"Availability sangat rendah pada komponen {komp}")
                    botqc = max(botqc, 59.0)              
            
            # Append botqc to list
            percqc_list.append(botqc)
            
        # 9. Average score and save
        if not percqc_list:
             score = 0.0
        else:
            if 1.0 in percqc_list: # limit to 59 (Poor/Buruk) if any component is not responding
                score = min(np.percentile(percqc_list, 25), 59.0)
            else:
                score = np.percentile(percqc_list, 25)
            
        kualitas = _check_qc(score)
        
        repo.insert_qc_analysis_result(
            kode, 
            tanggal, 
            str(round(float(score), 2)), 
            kualitas, 
            tipe, 
            ket
        )
        logger.info(f"{network}.{kode} ({tipe}) QC ANALYSIS FINISH (Score: {score:.2f})")
        time.sleep(0.5)

    time.sleep(0.5)