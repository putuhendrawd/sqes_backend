import signal
import os
import time
import numpy as np
from obspy import UTCDateTime, Trace
from typing import Optional, cast, Dict, Any
from obspy.clients.fdsn import Client as FDSNClient
from obspy.clients.filesystem.sds import Client as SDSClient
from ..services.db_pool import DBPool
from ..services.logging_config import initialize_worker_logger, get_station_logger
from ..services.repository import QCRepository
from ..services import source_mapper
from ..analysis import qc_analyzer
from ..core import basic_metrics, ppsd_metrics, models, utils
from ..clients import fdsn, sds, local

# Global Worker Resources
GW_DB_POOL: Optional[DBPool] = None
GW_CONTEXT: Dict[str, Any] = {}

def init_worker(db_credentials, basic_config, log_level, log_file_path,
                tgl, time0, time1, client_credentials, output_paths,
                pdf_trigger, mseed_trigger, qc_thresholds):
    """
    Initializer for worker processes.
    Sets up DBPool, Logging, and Context once per process.
    """
    global GW_DB_POOL, GW_CONTEXT
    
    # 1. Setup Logging
    initialize_worker_logger(log_level, log_file_path)
    # Logger for the init phase itself
    logger = get_station_logger("Worker Init")
    logger.debug(f"Worker process started (PID: {os.getpid()})")

    # 2. Setup DB Pool
    try:
        GW_DB_POOL = DBPool(**db_credentials)
        logger.debug("Worker DB Pool initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize Worker DB Pool: {e}")
        raise e
        
    # 3. Handle Signals
    signal.signal(signal.SIGALRM, _handle_timeout)
    
    # 4. Populate Context
    GW_CONTEXT.update({
        'tgl': tgl,
        'time0': time0,
        'time1': time1,
        'client_credentials': client_credentials,
        'basic_config': basic_config,
        'output_paths': output_paths,
        'pdf_trigger': pdf_trigger,
        'mseed_trigger': mseed_trigger,
        'qc_thresholds': qc_thresholds
    })
    
    # 5. Load source mapping (cached after first load)
    try:
        source_mapper.load_source_mapping()
        logger.debug("Source mapping loaded successfully")
    except Exception as e:
        logger.warning(f"Failed to load source mapping: {e}. Will use defaults from global.cfg")


def _handle_timeout(signum, frame):
    """Timeout handler for worker processes."""
    print(f"!! Process TIMEOUT after signal {signum}", flush=True)
    raise TimeoutError("Process took too long")

def process_station_data(sta_tuple):
    """
    This is the main worker function that runs in a separate process.
    It processes all components (e.g., E,N,Z or 1,2,Z) for a single station.
    """
    global GW_DB_POOL, GW_CONTEXT
    
    # Unpack Context
    tgl = GW_CONTEXT['tgl']
    time0 = GW_CONTEXT['time0']
    time1 = GW_CONTEXT['time1']
    client_credentials = GW_CONTEXT['client_credentials']
    basic_config = GW_CONTEXT['basic_config']
    output_paths = GW_CONTEXT['output_paths']
    pdf_trigger = GW_CONTEXT['pdf_trigger']
    mseed_trigger = GW_CONTEXT['mseed_trigger']
    qc_thresholds = GW_CONTEXT['qc_thresholds']
    
    try:
        # --- UPDATED: Unpack 6 items ---
        (network, kode, location, 
         sistem_sensor, channel_prefixes_str, channel_components_str) = sta_tuple
         
        location = location or ''
        channel_prefixes = (channel_prefixes_str or '').split(',')
        channel_components = (channel_components_str or '').split(',')

        
    except Exception as e:
        print(f"!! FATAL: Error unpacking station tuple {sta_tuple}: {e}", flush=True)
        return

    # Use LoggerAdapter
    logger = get_station_logger(kode)
    logger.info(f"PROCESS START {network}.{kode} ({sistem_sensor}). Channel: {channel_prefixes}, Components: {channel_components}")
    
    try:
        # --- Use Global DB Pool ---
        if GW_DB_POOL is None:
             logger.error("Global DB Pool is not initialized!")
             return
             
        repo = QCRepository(GW_DB_POOL, basic_config['use_database'])
        
        # --- Resolve Station-Specific Sources ---
        # Check if this station has custom source configuration
        station_sources = source_mapper.get_station_sources(network, kode)
        
        # Resolve waveform source (station-specific or default)
        waveform_type = basic_config.get('waveform_source', 'fdsn').lower()
        waveform_tag = 'client' if waveform_type == 'fdsn' else 'archive'
        waveform_source_label = f"{waveform_type} ({waveform_tag}) [default]"
        
        if station_sources and station_sources.waveform:
            # Use station-specific waveform source
            waveform_type = station_sources.waveform.type
            waveform_tag = station_sources.waveform.tag
            waveform_source_label = f"{waveform_type} ({waveform_tag})"
        
        # Resolve inventory source (station-specific or default)
        inventory_type = basic_config.get('inventory_source', 'fdsn').lower()
        inventory_tag = 'inventory_client' if inventory_type == 'fdsn' else 'inventory'
        inventory_source_label = f"{inventory_type} ({inventory_tag}) [default]"
        
        if station_sources and station_sources.inventory:
            # Use station-specific inventory source
            inventory_type = station_sources.inventory.type
            inventory_tag = station_sources.inventory.tag
            inventory_source_label = f"{inventory_type} ({inventory_tag})"
        
        # Log the resolved sources
        logger.info(f"{network}.{kode} - Waveform: {waveform_source_label}, Inventory: {inventory_source_label}")
        
        # --- Get config settings ---
        waveform_source = waveform_type  # Use resolved type
        inventory_source = inventory_type  # Use resolved type
        
        # Load station-specific or default configuration
        from ..services.config_loader import load_client_config, load_archive_config, load_inventory_client_config, load_inventory_path_config
        
        # Get waveform-related config
        if waveform_source == 'fdsn':
            waveform_client_config = load_client_config(waveform_tag)
        elif waveform_source == 'sds':
            # Always load from archive section
            archive_path = load_archive_config(waveform_tag)
        
        # Get inventory-related config
        if inventory_source == 'fdsn':
            if inventory_tag == 'inventory_client':
                # Check if there's an [inventory_client] section, otherwise use waveform client
                try:
                    inventory_client_config = load_inventory_client_config(inventory_tag)
                except:
                    # Fall back to waveform client if inventory_client section doesn't exist
                    inventory_client_config = waveform_client_config if waveform_source == 'fdsn' else load_client_config('client')
            else:
                inventory_client_config = load_inventory_client_config(inventory_tag)
        elif inventory_source == 'local':
            # Always load from inventory section
            inventory_path = load_inventory_path_config(inventory_tag)

        # --- Conditionally create FDSN client (only if needed) ---
        fdsn_client: Optional[FDSNClient] = None
        inventory_fdsn_client: Optional[FDSNClient] = None
        
        if waveform_source == 'fdsn':
            logger.debug(f"Creating FDSN client for waveforms: {waveform_tag}")
            fdsn_client = FDSNClient(
                waveform_client_config['url'],
                user=waveform_client_config['user'],
                password=waveform_client_config['password']
            )
        
        if inventory_source == 'fdsn':
            logger.debug(f"Creating FDSN client for inventory: {inventory_tag}")
            # Check if same as waveform client to avoid duplicate
            if waveform_source == 'fdsn' and inventory_tag == waveform_tag:
                inventory_fdsn_client = fdsn_client
            else:
                inventory_fdsn_client = FDSNClient(
                    inventory_client_config['url'],
                    user=inventory_client_config['user'],
                    password=inventory_client_config['password']
                )
        
        # --- Create Waveform Data Client ---
        data_client: FDSNClient | SDSClient
        if waveform_source == 'sds':
            if not archive_path:
                logger.error(f"'waveform_source' is 'sds' but archive path is not set for tag '{waveform_tag}'. Worker exiting.")
                return
            data_client = SDSClient(sds_root=archive_path)
        else:
            if not fdsn_client:
                raise ConnectionError("FDSN client for waveforms was not initialized.")
            data_client = fdsn_client  # Use FDSN client

        # --- Validate Inventory Config ---
        if inventory_source == 'local' and not inventory_path:
            logger.error(f"'inventory_source' is 'local' but inventory path is not set for tag '{inventory_tag}'. Worker exiting.")
            return

    except Exception as e:
        logger.error(f"Failed to initialize worker resources: {e}")
        return

    # Unpack output paths
    outputmseed = output_paths['outputmseed']
    outputsignal = output_paths['outputsignal']
    outputPSD = output_paths['outputPSD']
    outputPDF = output_paths['outputPDF']

    # --- UPDATED: Main Loop ---
    for ch in channel_components:
        id_kode = f"{kode}_{ch}_{tgl}"
        
        def log_default_and_continue(base_metrics=None, cha=ch, reason=""):
            if base_metrics:
                metrics = base_metrics
            else:
                metrics = {'rms': '0', 'ratioamp': '0', 'psdata': '0', 'ngap': '1', 'nover': '0', 'num_spikes': '0'}
            
            try:
                repo.check_and_delete_qc_detail(id_kode, tgl)
                repo.insert_default_qc_detail(id_kode, kode, tgl, cha, metrics)
                logger.warning(f"{id_kode} - Skipped with default parameters. Reason: {reason}")
            except Exception as e:
                logger.error(f"{id_kode} - FAILED to log default parameters: {e}")
            time.sleep(0.5)

        # --- 2. Load/Download Waveforms ---
        logger.debug(f"{id_kode} Acquiring waveforms (method: {waveform_source})...")
        sig = None
        
        try:
            if waveform_source == 'sds':
                sig = sds.get_waveforms(
                    cast(SDSClient, data_client), # Cast for Pylance
                    network, kode, location, 
                    channel_prefixes, time0, time1, ch
                )
            else: # 'fdsn' or default
                if not fdsn_client:
                     raise ConnectionError("FDSN client was not initialized (check config).")
                sig = fdsn.get_waveforms(
                    fdsn_client, network, kode, location, 
                    channel_prefixes, time0, time1, ch
                )
        
        except TimeoutError:
            logger.error(f"!! {id_kode} FDSN download timeout!")
            log_default_and_continue(reason="Download Timeout")
            continue
        except Exception as e:
            logger.error(f"!! {id_kode} data acquisition error: {e}")
            log_default_and_continue(reason="Data Acquisition Error")
            continue

        if sig is None or sig.count() == 0:
            logger.info(f"!! {id_kode} No Data found (source: {waveform_source})")
            log_default_and_continue(reason="No Data")
            continue

        logger.debug(f"{id_kode} Waveform acquisition complete")

        # --- 2b: Load/Download Inventory ---
        logger.debug(f"{id_kode} Acquiring inventory (method: {inventory_source})...")
        tr = cast(Trace, sig[0]) 
        inv = None
        
        if inventory_source == 'local':
            inv = local.get_inventory(
                cast(str, inventory_path), tr.stats.network, tr.stats.station,
                tr.stats.location, tr.stats.channel, time0
            )
        else: # 'fdsn' or default
            if not inventory_fdsn_client:
                 raise ConnectionError("FDSN client for inventory was not initialized (check config).")
            inv = fdsn.get_inventory(
                inventory_fdsn_client, tr.stats.network, tr.stats.station, 
                tr.stats.location, tr.stats.channel, time0
            )
        
        if not inv:
            logger.warning(f"!! {id_kode} Got data but NO INVENTORY (source: {inventory_source}). Skipping.")
            log_default_and_continue(reason="No Inventory")
            continue
        
        # --- 3. Save Waveform & Plot ---
        try:
            signal.alarm(180) # 3 min timeout
            cha = tr.stats.channel
            mseed_naming_code = f"{outputmseed}/{kode}_{cha[-1]}.mseed"
            if mseed_trigger:
                sig.write(mseed_naming_code)
            sig.plot(outfile=f"{outputsignal}/{kode}_{cha[-1]}_signal.png")
            signal.alarm(0)
        except Exception as e:
            signal.alarm(0)
            logger.error(f"{id_kode} saving exception: {e}")
            log_default_and_continue(cha=ch, reason="Save waveform/plot failed")
            continue
        
        # --- 4. Process Basic Metrics ---
        try:
            logger.debug(f"{id_kode} Process basic info")
            spike_method = basic_config.get('spike_method', 'fast').lower()

            metrics = basic_metrics.process_basic_metrics(
                sig, 
                time0, 
                time1,
                spike_method=spike_method
            )
            
            basic_metrics_dict = {
                'rms': str(round(float(metrics['rms']), 2)),
                'ratioamp': str(round(float(metrics['ratioamp']), 2)),
                'psdata': str(round(float(metrics['psdata']), 2)),
                'ngap': str(int(metrics['ngap'])),
                'nover': str(int(metrics['nover'])),
                'num_spikes': str(int(metrics['num_spikes']))
            }

        except Exception as e:
            logger.error(f"{id_kode} basic info exception: {e}")
            log_default_and_continue(cha=cha, reason="Basic metrics failed")
            continue
        
        # # --- 5. High Gap Check ---
        # if int(basic_metrics_dict['ngap']) > 2000:
        #     logger.warning(f"{id_kode} high gap ({basic_metrics_dict['ngap']}) - Continuing with default")
        #     log_default_and_continue(basic_metrics_dict, cha, reason="High gap count")
        #     continue
            
        # --- 6. Process PPSD Metrics ---
        logger.debug(f"{id_kode} Process PPSD metrics")
        plot_filename = f"{outputPDF}/{kode}_{cha[-1]}_PDF.png"
        npz_path = outputPSD if pdf_trigger else ''
        
        final_metrics = None
        try:
            signal.alarm(1200) # 20 min timeout
            final_metrics = ppsd_metrics.process_ppsd_metrics(
                sig, 
                inv, 
                plot_filename=plot_filename, 
                npz_output_path=npz_path
            )
            signal.alarm(0)
        except TimeoutError:
            signal.alarm(0)
            logger.error(f"{id_kode} PPSD metric processing timed out")
            log_default_and_continue(basic_metrics_dict, cha, reason="PPSD processing timeout")
            continue
        except Exception as e:
            signal.alarm(0)
            logger.error(f"{id_kode} PPSD metric processing failed: {e}")
            log_default_and_continue(basic_metrics_dict, cha, reason="PPSD processing error")
            continue

        # --- 7. Check PPSD Result ---
        if not final_metrics:
            logger.warning(f"{id_kode} PPSD metrics returned None. Skipping with defaults.")
            log_default_and_continue(basic_metrics_dict, cha, reason="PPSD calculation failed")
            continue
            
        # --- 8. Commit Full Result ---
        all_metrics = {
            'id_kode': id_kode, 'kode': kode, 'tgl': tgl, 'cha': cha,
            **basic_metrics_dict,
            **final_metrics
        }
        
        try:
            logger.debug(f"{id_kode} Saving to database")
            repo.check_and_delete_qc_detail(id_kode, tgl)
            repo.insert_qc_detail(all_metrics)
            logger.info(f"{id_kode} Process finish")
            time.sleep(0.5)
        except Exception as e:
            logger.error(f"{id_kode} Database commit error: {e}")
            
    # --- End of channel loop ---

    # --- 9. Run QC Analysis for this station ---
    logger.info(f"PROCESS FINISH. Running final analysis...")
    try:
        if qc_thresholds is not None:
            qc_analyzer.run_qc_analysis(repo, basic_config['use_database'], tgl, kode, qc_thresholds)
        else:
            # Fallback to defaults if not provided
            qc_analyzer.run_qc_analysis(repo, basic_config['use_database'], tgl, kode)
    except Exception as e:
        logger.error(f"QC Analysis failed for {kode}: {e}")
        
    time.sleep(0.5)
    
    # --- 10. Cleanup ---
    del(repo)
    logger.debug("Worker complete.")