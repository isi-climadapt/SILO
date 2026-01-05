"""
SILO Met File Creation

This script fetches climate data from SILO API and exports it
to both APSIM .met format and CSV format.
"""

from silo_api import SILOWeatherAPI
import os

# Configuration
# TODO: Replace with your SILO API credentials
USERNAME = "ibianchival@gmail.com"  # Register at longpaddock.qld.gov.au
PASSWORD = "password"

# Coordinates and date range
LATITUDE = -31.75
LONGITUDE = 117.5999984741211  # Will be rounded to 2 decimals in filename
START_YEAR = 1990
END_YEAR = 2024

# Output directory - Change this to your desired output location
OUTPUT_DIR = r"C:\Users\ibian\Desktop\ClimAdapt\Anameka\Anameka_South_16_226042"


def main():
    """Main function to fetch and export SILO data"""
    
    print("Initializing SILO Weather API client...")
    api = SILOWeatherAPI(username=USERNAME, password=PASSWORD)
    
    print(f"\nFetching data for coordinates:")
    print(f"  Latitude: {LATITUDE}")
    print(f"  Longitude: {LONGITUDE}")
    print(f"  Year range: {START_YEAR} to {END_YEAR}")
    
    try:
        # Fetch data from SILO API
        print("\nFetching data from SILO API...")
        df = api.get_silo_data(
            lat=LATITUDE,
            lon=LONGITUDE,
            start_year=START_YEAR,
            end_year=END_YEAR,
            format="fao56"  # Use FAO56 format to get et_short_crop
        )
        
        print(f"Successfully fetched {len(df)} days of data")
        print(f"\nDataFrame columns: {list(df.columns)}")
        print(f"\nFirst few rows:")
        print(df.head())
        
        # Export to APSIM .met format
        print("\nExporting to APSIM .met format...")
        met_file = api.export_to_met(
            df=df,
            lat=LATITUDE,
            lon=LONGITUDE,
            start_year=START_YEAR,
            end_year=END_YEAR,
            output_dir=OUTPUT_DIR
        )
        print(f"Created: {met_file}")
        
        # Export to CSV format
        print("\nExporting to CSV format...")
        csv_file = api.export_to_csv(
            df=df,
            lat=LATITUDE,
            lon=LONGITUDE,
            start_year=START_YEAR,
            end_year=END_YEAR,
            output_dir=OUTPUT_DIR
        )
        print(f"Created: {csv_file}")
        
        # Display statistics
        print("\n" + "="*60)
        print("Climate Statistics:")
        print("="*60)
        print(f"Daily Rainfall - Mean: {df['daily_rain'].mean():.2f} mm")
        print(f"Max Temperature - Mean: {df['max_temp'].mean():.2f} °C")
        print(f"Min Temperature - Mean: {df['min_temp'].mean():.2f} °C")
        print(f"Vapour Pressure - Mean: {df['vp'].mean():.2f} hPa")
        print(f"Radiation - Mean: {df['radiation'].mean():.2f} MJ/m²")
        print(f"ET (FAO56) - Mean: {df['et_short_crop'].mean():.2f} mm")
        print(f"Pan Evaporation - Mean: {df['evap_pan'].mean():.2f} mm")
        
        # Calculate TAV and AMP
        tav = api.calculate_tav(df)
        amp = api.calculate_amp(df)
        print(f"\nTAV (Annual Average Temperature): {tav:.2f} °C")
        print(f"AMP (Annual Amplitude): {amp:.2f} °C")
        
        print("\n" + "="*60)
        print("Export completed successfully!")
        print("="*60)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

