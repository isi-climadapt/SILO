"""
SILO Weather API Client

A Python client for fetching historical climate data from Queensland Government's
SILO (Scientific Information for Land Owners) Patched Dataset API.
"""

import urllib.parse
import urllib.request
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict, Tuple
import os


class SILOAPIError(Exception):
    """Base exception for SILO API errors"""
    pass


class InvalidCoordinatesError(SILOAPIError):
    """Raised when coordinates are outside Australian bounds"""
    pass


class AuthenticationError(SILOAPIError):
    """Raised when authentication fails"""
    pass


class MissingVariableError(SILOAPIError):
    """Raised when required variables are missing from SILO response"""
    pass


class SILOWeatherAPI:
    """
    Client for fetching climate data from SILO Patched Dataset API.
    
    Provides methods to fetch, parse, and export climate data in standardized formats.
    """
    
    # Variable mapping for FAO56 format
    # Note: SILO uses "Rain" (not "Rainfall"), "Radn" (not "Radiation"), "Evap" (not "Evaporation")
    FAO56_MAPPING = {
        'Date': 'date',
        'Rain': 'daily_rain',        # Actual SILO column name (primary)
        'Rainfall': 'daily_rain',    # Alternative name (fallback)
        'T.Max': 'max_temp',
        'T.Min': 'min_temp',
        'VP': 'vp',
        'Radn': 'radiation',         # Actual SILO column name (primary)
        'Radiation': 'radiation',     # Alternative name (fallback)
        'FAO56': 'et_short_crop',
        'Evap': 'evap_pan',          # Actual SILO column name (primary)
        'Evaporation': 'evap_pan'    # Alternative name (fallback)
    }
    
    # Variable mapping for daily format
    DAILY_MAPPING = {
        'Date': 'date',
        'Rain': 'daily_rain',
        'Tmax': 'max_temp',
        'Tmin': 'min_temp',
        'VP': 'vp',
        'Radiation': 'radiation',
        'FAO56': 'et_short_crop',
        'Evap': 'evap_pan'
    }
    
    # Required output variables (in order)
    REQUIRED_VARIABLES = [
        'daily_rain',
        'max_temp',
        'min_temp',
        'vp',
        'radiation',
        'et_short_crop',
        'evap_pan'
    ]
    
    # Australian coordinate bounds
    LAT_MIN = -44.0
    LAT_MAX = -10.0
    LON_MIN = 112.0
    LON_MAX = 154.0
    
    # Missing data codes used by SILO
    MISSING_CODES = [-9999, -99.9, -999, -99]
    
    def __init__(self, username: str, password: str):
        """
        Initialize SILO Weather API client.
        
        Parameters:
        - username: SILO API username (register at longpaddock.qld.gov.au)
        - password: SILO API password
        """
        self.username = username
        self.password = password
        self.api_base_url = "https://www.longpaddock.qld.gov.au/cgi-bin/silo"
    
    def validate_coordinates(self, lat: float, lon: float) -> bool:
        """
        Validate coordinates are within Australian bounds.
        
        Parameters:
        - lat: Latitude (decimal degrees)
        - lon: Longitude (decimal degrees)
        
        Returns:
        - True if coordinates are valid
        
        Raises:
        - InvalidCoordinatesError if coordinates are outside bounds
        """
        if not (self.LAT_MIN <= lat <= self.LAT_MAX):
            raise InvalidCoordinatesError(
                f"Latitude {lat} is outside Australian bounds ({self.LAT_MIN} to {self.LAT_MAX})"
            )
        if not (self.LON_MIN <= lon <= self.LON_MAX):
            raise InvalidCoordinatesError(
                f"Longitude {lon} is outside Australian bounds ({self.LON_MIN} to {self.LON_MAX})"
            )
        return True
    
    def get_silo_data(self, lat: float, lon: float, start_year: int, end_year: int, 
                     format: str = "fao56") -> pd.DataFrame:
        """
        Fetch SILO patched climate data for specified coordinates and date range.
        
        Parameters:
        - lat: Latitude (decimal degrees, -44 to -10 for Australia)
        - lon: Longitude (decimal degrees, 112 to 154 for Australia)
        - start_year: Start year (1889-present)
        - end_year: End year (1889-present)
        - format: 'fao56' (FAO56 reference evapotranspiration) or 'daily' (raw daily data)
        
        Returns:
        - Parsed climate data as pandas DataFrame with exactly 7 standardized variables
        """
        # Validate coordinates
        self.validate_coordinates(lat, lon)
        
        # Validate date range
        current_year = datetime.now().year
        if start_year < 1889 or end_year > current_year:
            raise ValueError(f"Date range must be between 1889 and {current_year}")
        if start_year > end_year:
            raise ValueError("start_year must be <= end_year")
        
        # Build URL
        params = {
            'format': format,
            'lat': str(round(lat, 4)),
            'lon': str(round(lon, 4)),
            'start': f"{start_year}0101",
            'finish': f"{end_year}1231",
            'username': self.username,
            'password': self.password
        }
        
        url = f"{self.api_base_url}/DataDrillDataset.php?" + urllib.parse.urlencode(params)
        
        # Make HTTP request
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise SILOAPIError("Request timeout - SILO API did not respond in time")
        except requests.exceptions.HTTPError as e:
            if 'response' in locals() and response.status_code == 401:
                raise AuthenticationError("Authentication failed - check username and password")
            raise SILOAPIError(f"HTTP error {response.status_code if 'response' in locals() else 'unknown'}: {e}")
        except requests.exceptions.RequestException as e:
            raise SILOAPIError(f"Network error: {e}")
        
        # Check for authentication errors in response
        response_lower = response.text.lower()
        if "username" in response_lower and "password" in response_lower:
            if "invalid" in response_lower or "error" in response_lower:
                raise AuthenticationError("Authentication failed - check username and password")
        
        # Parse response
        df = self.parse_silo_response(response.text, format)
        
        # Extract and standardize variables
        df = self.extract_variables(df, format)
        
        return df
    
    def parse_silo_response(self, response_text: str, format: str) -> pd.DataFrame:
        """
        Parse SILO CSV response into DataFrame.
        
        Parameters:
        - response_text: Raw CSV response from SILO API
        - format: Format type ('fao56' or 'daily')
        
        Returns:
        - DataFrame with raw SILO column names
        """
        lines = response_text.strip().split('\n')
        
        # Find where data starts (skip metadata header)
        data_start_idx = 0
        for i, line in enumerate(lines):
            # Look for header row (usually contains 'Date' or column names)
            if 'Date' in line or 'date' in line.lower():
                # Check if this looks like a data header
                if any(col in line for col in ['Rainfall', 'Rain', 'T.Max', 'Tmax', 'T.Min', 'Tmin']):
                    data_start_idx = i
                    break
        
        if data_start_idx == 0:
            # Try to find first line with comma-separated values that looks like data
            for i, line in enumerate(lines):
                if ',' in line and len(line.split(',')) > 5:
                    # Check if first value looks like a date
                    first_val = line.split(',')[0].strip()
                    if len(first_val) == 8 and first_val.isdigit():  # YYYYMMDD format
                        data_start_idx = i - 1  # Header is line before
                        break
        
        if data_start_idx == 0:
            raise SILOAPIError("Could not find data start in SILO response")
        
        # Read CSV starting from data header
        # SILO returns SPACE-DELIMITED data, not comma-delimited
        try:
            csv_content = '\n'.join(lines[data_start_idx:])
            # Use sep=r'\s+' for space-delimited data (delim_whitespace is deprecated)
            df = pd.read_csv(
                pd.io.common.StringIO(csv_content),
                sep=r'\s+',
                skipinitialspace=True
            )
        except Exception as e:
            raise SILOAPIError(f"Failed to parse SILO CSV response: {e}")
        
        # Clean column names (remove extra whitespace, special characters)
        df.columns = df.columns.str.strip()
        
        # Convert numeric columns to numeric (handle string values)
        # SILO data may have string values that need conversion
        for col in df.columns:
            if col.lower() not in ['date', 'day', 'date2']:
                # Try to convert to numeric, coerce errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert date column to datetime
        date_col = None
        for col in df.columns:
            if 'date' in col.lower():
                date_col = col
                break
        
        if date_col is None:
            raise SILOAPIError("Could not find date column in SILO response")
        
        # Handle different date formats
        try:
            df[date_col] = pd.to_datetime(df[date_col], format='%Y%m%d', errors='coerce')
        except:
            try:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            except:
                raise SILOAPIError("Could not parse date column")
        
        # Set date as index
        df = df.set_index(date_col)
        df.index.name = 'date'
        
        # Remove rows with invalid dates
        df = df[df.index.notna()]
        
        # Sort by date
        df = df.sort_index()
        
        return df
    
    def extract_variables(self, df: pd.DataFrame, format: str) -> pd.DataFrame:
        """
        Extract and rename required variables from SILO DataFrame.
        
        Parameters:
        - df: DataFrame with raw SILO column names
        - format: Format type ('fao56' or 'daily')
        
        Returns:
        - DataFrame with exactly 7 standardized variables + quality codes
        """
        # Select appropriate mapping
        mapping = self.FAO56_MAPPING if format == 'fao56' else self.DAILY_MAPPING
        
        # Create output DataFrame
        output_df = pd.DataFrame(index=df.index)
        
        # Map variables - check each required output variable
        # Build a reverse mapping: output_col -> possible SILO column names
        output_to_silo = {}
        for silo_col, output_col in mapping.items():
            if silo_col == 'Date':
                continue
            if output_col not in output_to_silo:
                output_to_silo[output_col] = []
            output_to_silo[output_col].append(silo_col)
        
        # Map variables
        for output_col in self.REQUIRED_VARIABLES:
            if output_col not in output_to_silo:
                continue
            
            # Try to find the column using any of the possible SILO names
            found_col = None
            possible_names = output_to_silo[output_col]
            
            for col in df.columns:
                col_clean = col.strip()
                col_lower = col_clean.lower()
                for silo_col in possible_names:
                    silo_lower = silo_col.lower().strip()
                    # Exact match or substring match (handle space-delimited column names)
                    if col_lower == silo_lower or silo_lower in col_lower or col_lower in silo_lower:
                        found_col = col
                        break
                if found_col:
                    break
            
            if found_col is None:
                raise MissingVariableError(
                    f"Required variable '{output_col}' not found in SILO response. "
                    f"Tried: {possible_names}. Available columns: {list(df.columns)}"
                )
            
            # Copy data and handle missing values
            data = df[found_col].copy()
            
            # Convert missing data codes to NaN
            for code in self.MISSING_CODES:
                data = data.replace(code, np.nan)
            
            output_df[output_col] = data
        
        # Try to extract quality codes if available
        # SILO quality codes are typically in a column named 'code' or similar
        code_col = None
        for col in df.columns:
            if 'code' in col.lower():
                code_col = col
                break
        
        if code_col:
            output_df['code'] = df[code_col].astype(str).str.zfill(6)
        else:
            # If no quality codes found, use default (all interpolated)
            output_df['code'] = '222222'
        
        # Verify all required variables are present
        missing_vars = [var for var in self.REQUIRED_VARIABLES if var not in output_df.columns]
        if missing_vars:
            raise MissingVariableError(f"Missing required variables: {missing_vars}")
        
        # Reorder columns to match required order
        output_df = output_df[self.REQUIRED_VARIABLES + ['code']]
        
        # Ensure continuous date index (fill missing dates with NaN)
        if len(output_df) > 0:
            full_date_range = pd.date_range(
                start=output_df.index.min(),
                end=output_df.index.max(),
                freq='D'
            )
            output_df = output_df.reindex(full_date_range)
        
        return output_df
    
    def calculate_tav(self, df: pd.DataFrame) -> float:
        """
        Calculate annual average ambient temperature.
        
        Formula: mean((max_temp + min_temp) / 2) for all days
        
        Parameters:
        - df: DataFrame with max_temp and min_temp columns
        
        Returns:
        - TAV value (float)
        """
        if 'max_temp' not in df.columns or 'min_temp' not in df.columns:
            raise ValueError("DataFrame must contain max_temp and min_temp columns")
        
        daily_mean_temp = (df['max_temp'] + df['min_temp']) / 2.0
        tav = daily_mean_temp.mean()
        
        return float(tav)
    
    def calculate_amp(self, df: pd.DataFrame) -> float:
        """
        Calculate annual amplitude in mean monthly temperature.
        
        Formula: mean(monthly_max) - mean(monthly_min)
        
        Parameters:
        - df: DataFrame with max_temp and min_temp columns
        
        Returns:
        - AMP value (float)
        """
        if 'max_temp' not in df.columns or 'min_temp' not in df.columns:
            raise ValueError("DataFrame must contain max_temp and min_temp columns")
        
        # Calculate monthly mean temperatures
        df_copy = df.copy()
        df_copy['year_month'] = df_copy.index.to_period('M')
        df_copy['daily_mean'] = (df_copy['max_temp'] + df_copy['min_temp']) / 2.0
        
        monthly_means = df_copy.groupby('year_month')['daily_mean'].mean()
        
        # Calculate annual amplitude
        monthly_max = monthly_means.max()
        monthly_min = monthly_means.min()
        amp = monthly_max - monthly_min
        
        return float(amp)
    
    def calculate_day_of_year(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add day of year column (1-366, handling leap years).
        
        Parameters:
        - df: DataFrame with DatetimeIndex
        
        Returns:
        - DataFrame with added 'day' column
        """
        df = df.copy()
        df['day'] = df.index.dayofyear
        return df
    
    def shift_evaporation(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Shift evaporation values by 1 day (APSIM convention).
        Evaporation measured on day N is recorded on day N-1.
        
        Parameters:
        - df: DataFrame with evap_pan column
        
        Returns:
        - DataFrame with shifted evaporation values
        """
        if 'evap_pan' not in df.columns:
            raise ValueError("DataFrame must contain evap_pan column")
        
        df = df.copy()
        # Shift evaporation forward by 1 day
        # For the last day of the year (day 365 or 366), use the previous day's value
        # since there's no next day to shift from
        df['evap_pan'] = df['evap_pan'].shift(-1)
        
        # Fill the last day (which becomes NaN after shift) with the previous day's value
        # This handles both regular years (day 365) and leap years (day 366)
        if df['evap_pan'].isna().any():
            # Fill NaN values (which should only be the last day) with forward fill
            # This uses the previous day's value for the last day
            df['evap_pan'] = df['evap_pan'].ffill()
        
        return df
    
    def generate_filename(self, lat: float, lon: float, start_year: int, 
                         end_year: int, extension: str) -> str:
        """
        Generate filename following convention: SILO_{START_YEAR}-{END_YEAR}_{LAT}_{LON}.{extension}
        
        Parameters:
        - lat: Latitude
        - lon: Longitude
        - start_year: Start year
        - end_year: End year
        - extension: File extension (without dot)
        
        Returns:
        - Filename string
        """
        # Round coordinates to 2 decimal places for filename
        lat_str = f"{lat:.2f}"
        lon_str = f"{lon:.2f}"
        
        filename = f"SILO_{start_year}-{end_year}_{lat_str}_{lon_str}.{extension}"
        return filename
    
    def export_to_met(self, df: pd.DataFrame, lat: float, lon: float,
                     start_year: int, end_year: int, output_dir: str = ".") -> str:
        """
        Export DataFrame to APSIM .met format.
        
        Parameters:
        - df: DataFrame with climate data
        - lat: Latitude
        - lon: Longitude
        - start_year: Start year
        - end_year: End year
        - output_dir: Output directory path
        
        Returns:
        - Full path to created file
        """
        # Calculate TAV and AMP
        tav = self.calculate_tav(df)
        amp = self.calculate_amp(df)
        
        # Add day of year
        df = self.calculate_day_of_year(df)
        
        # Shift evaporation
        df = self.shift_evaporation(df)
        
        # Generate filename
        filename = self.generate_filename(lat, lon, start_year, end_year, 'met')
        filepath = os.path.join(output_dir, filename)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Format header
        extraction_date = datetime.now().strftime("%d/%m/%Y")
        
        header = f"""[weather.met.weather]
!Your Ref:  "
latitude = {lat:.2f}  (DECIMAL DEGREES)
longitude =  {lon:.2f}  (DECIMAL DEGREES)
tav = {tav:.2f} (oC) ! Annual average ambient temperature. Based on 1 Jan {start_year} to current.
amp = {amp:.2f} (oC) ! Annual amplitude in mean monthly temperature. Based on 1 Jan {start_year} to current.
!Data Extracted from SILO 'BoM Only' dataset on {extraction_date} " for APSIM
!As evaporation is read at 9am, it has been shifted to day before
!ie The evaporation measured on 20 April is in row for 19 April
!The 6 digit code indicates the source of the 6 data columns
!0 actual observation, 1 actual observation composite station
!2 interpolated from daily observations
!3 interpolated from daily observations using anomaly interpolation method for CLIMARC data
!6 synthetic pan
!7 interpolated long term averages
!more detailed two digit codes are available in SILO's 'Standard' format files
!
!For further information see the documentation on the datadrill
!  http://www.longpaddock.qld.gov.au/silo
!
year  day radn  maxt   mint  rain  evap    vp   code
 ()   () (MJ/m^2) (oC)  (oC)  (mm)  (mm) (hPa)     ()
"""
        
        # Format data rows
        with open(filepath, 'w') as f:
            f.write(header)
            
            for date, row in df.iterrows():
                year = date.year
                day = int(row['day']) if pd.notna(row['day']) else 1
                
                # Format values with fixed width (6 chars, 1 decimal place, right-aligned)
                # Handle NaN values
                if pd.notna(row['radiation']):
                    radn = f"{row['radiation']:6.1f}"
                else:
                    radn = "   NaN"
                
                if pd.notna(row['max_temp']):
                    maxt = f"{row['max_temp']:6.1f}"
                else:
                    maxt = "   NaN"
                
                if pd.notna(row['min_temp']):
                    mint = f"{row['min_temp']:6.1f}"
                else:
                    mint = "   NaN"
                
                if pd.notna(row['daily_rain']):
                    rain = f"{row['daily_rain']:6.1f}"
                else:
                    rain = "   NaN"
                
                if pd.notna(row['evap_pan']):
                    evap = f"{row['evap_pan']:6.1f}"
                else:
                    evap = "   NaN"
                
                if pd.notna(row['vp']):
                    vp = f"{row['vp']:6.1f}"
                else:
                    vp = "   NaN"
                
                code = str(row['code']).zfill(6) if pd.notna(row['code']) else "222222"
                
                # Write row with fixed-width formatting
                # Format: year(4) day(4) radn(6) maxt(6) mint(6) rain(6) evap(6) vp(6) code(7)
                # Match example: "2000   1   26.8  33.7  16.4   0.0   9.8  12.7 222222"
                f.write(f"{year:4d} {day:4d} {radn:>6s} {maxt:>6s} {mint:>6s} {rain:>6s} {evap:>6s} {vp:>6s} {code:>7s}\n")
        
        return filepath
    
    def export_to_csv(self, df: pd.DataFrame, lat: float, lon: float,
                     start_year: int, end_year: int, output_dir: str = ".") -> str:
        """
        Export DataFrame to CSV format.
        
        Parameters:
        - df: DataFrame with climate data
        - lat: Latitude
        - lon: Longitude
        - start_year: Start year
        - end_year: End year
        - output_dir: Output directory path
        
        Returns:
        - Full path to created file
        """
        # Add day of year
        df = self.calculate_day_of_year(df.copy())
        
        # Shift evaporation
        df = self.shift_evaporation(df)
        
        # Prepare export DataFrame
        export_df = pd.DataFrame(index=df.index)
        export_df['year'] = df.index.year
        export_df['day'] = df['day']
        export_df['radiation'] = df['radiation']
        export_df['max_temp'] = df['max_temp']
        export_df['min_temp'] = df['min_temp']
        export_df['daily_rain'] = df['daily_rain']
        export_df['evap_pan'] = df['evap_pan']
        export_df['vp'] = df['vp']
        export_df['et_short_crop'] = df['et_short_crop']
        export_df['code'] = df['code']
        
        # Generate filename
        filename = self.generate_filename(lat, lon, start_year, end_year, 'csv')
        filepath = os.path.join(output_dir, filename)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Export to CSV
        export_df.to_csv(filepath, index=False)
        
        return filepath

