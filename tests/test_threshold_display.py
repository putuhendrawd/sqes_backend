#!/usr/bin/env python3
"""
Test script to demonstrate QC threshold checking output.
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

def test_threshold_display():
    """Simulate what --check-config will show for QC thresholds."""
    print("=" * 60)
    print("QC Threshold Display Test")
    print("=" * 60)
    print()
    
    try:
        from sqes.services.config_loader import load_qc_thresholds
        from sqes.analysis.models import DEFAULT_THRESHOLDS
        from configparser import ConfigParser
        
        # Load thresholds
        thresholds = load_qc_thresholds()
        
        # Check if config has custom section
        config_path = os.path.join(
            os.path.dirname(__file__), 
            '..', 'config', 'config.ini'
        )
        
        has_custom = False
        custom_params = []
        
        if os.path.exists(config_path):
            parser = ConfigParser()
            parser.read(config_path)
            if parser.has_section('qc_thresholds'):
                has_custom = True
                custom_params = parser.options('qc_thresholds')
        
        print("--- QC Analysis Thresholds ---")
        
        if has_custom:
            print(f"✅ Using QC thresholds from config ({len(custom_params)} custom parameters)")
            print("   Custom parameters:")
            
            for param in sorted(custom_params):
                custom_val = getattr(thresholds, param, None)
                default_val = getattr(DEFAULT_THRESHOLDS, param, None)
                
                if custom_val != default_val:
                    print(f"   • {param} = {custom_val} (default: {default_val})")
                else:
                    print(f"   • {param} = {custom_val}")
            
            all_params = set(vars(DEFAULT_THRESHOLDS).keys())
            default_params = all_params - set(custom_params)
            
            if default_params:
                print(f"   Using defaults for {len(default_params)} other parameters")
        else:
            print("ℹ️  Using default QC thresholds (no [qc_thresholds] section in config)")
            print("   To customize, add [qc_thresholds] section to config.ini")
            print("   See config/sample_config.ini for all available parameters")
        
        print("\n   Key thresholds:")
        print(f"   • rms_limit: {thresholds.rms_limit}")
        print(f"   • gap_limit: {thresholds.gap_limit}")
        print(f"   • avail_good: {thresholds.avail_good}%")
        print(f"   • weight_noise: {thresholds.weight_noise}")
        
        print("\n✅ QC threshold display test passed!")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    test_threshold_display()
