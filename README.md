# SILO Weather API Client

A Python client for fetching historical climate data from Queensland Government's SILO (Scientific Information for Land Owners) Patched Dataset API. This client extracts standardized climate variables and exports them in both APSIM .met format and CSV format.

## Features

- Fetch daily climate data from SILO API (1889-present)
- Extract exactly 7 standardized climate variables
- Export to APSIM .met format with fixed-width formatting
- Export to CSV format
- Auto-generate filenames following convention: `SILO_{START}-{END}_{LAT}_{LON}.{ext}`
- Handle missing data and quality codes
- Calculate TAV (Annual Average Temperature) and AMP (Annual Amplitude)
- Support for both FAO56 and daily formats

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

2. Register for SILO API credentials at [longpaddock.qld.gov.au](https://www.longpaddock.qld.gov.au)

## Usage

### Basic Example

```python
from silo_api import SILOWeatherAPI

# Initialize API client
api = SILOWeatherAPI(username="your_username", password="your_password")

# Fetch data
df = api.get_silo_data(
    lat=-31.75,
    lon=117.60,
    start_year=2000,
    end_year=2024,
    format="fao56"
)

# Export to APSIM .met format
met_file = api.export_to_met(
    df=df,
    lat=-31.75,
    lon=117.60,
    start_year=2000,
    end_year=2024,
    output_dir="./output"
)

# Export to CSV format
csv_file = api.export_to_csv(
    df=df,
    lat=-31.75,
    lon=117.60,
    start_year=2000,
    end_year=2024,
    output_dir="./output"
)
```

### Running the Example Script

1. Edit `example_usage.py` and update:
   - `USERNAME` and `PASSWORD` with your SILO API credentials
   - `START_YEAR` and `END_YEAR` with your desired date range
   - `OUTPUT_DIR` with your output directory path

2. Run the script:
```bash
python example_usage.py
```

## Output Variables

The client extracts exactly 7 standardized climate variables:

| Variable | Unit | Description |
|----------|------|-------------|
| `daily_rain` | mm | Daily rainfall |
| `max_temp` | °C | Maximum temperature |
| `min_temp` | °C | Minimum temperature |
| `vp` | hPa | Vapour pressure |
| `radiation` | MJ/m² | Solar radiation |
| `et_short_crop` | mm | FAO56 reference evapotranspiration |
| `evap_pan` | mm | Pan evaporation |

## File Naming Convention

Output files follow the naming convention:
```
SILO_{START_YEAR}-{END_YEAR}_{LAT}_{LON}.{extension}
```

Examples:
- `SILO_2000-2025_-31.75_117.60.met`
- `SILO_2000-2025_-31.75_117.60.csv`

## Output Formats

### APSIM .met Format

The .met file includes:
- Header with latitude, longitude, TAV, and AMP values
- Metadata comments explaining data sources
- Fixed-width formatted data columns:
  - `year`: 4 characters
  - `day`: 4 characters (day of year, 1-366)
  - `radn`: 6 characters (radiation, MJ/m²)
  - `maxt`: 6 characters (max temperature, °C)
  - `mint`: 6 characters (min temperature, °C)
  - `rain`: 6 characters (rainfall, mm)
  - `evap`: 6 characters (evaporation, mm)
  - `vp`: 6 characters (vapour pressure, hPa)
  - `code`: 7 characters (6-digit quality code)

**Note:** Evaporation values are shifted by 1 day (APSIM convention - evaporation measured on day N is recorded on day N-1).

### CSV Format

Standard CSV with columns:
- `year`, `day`, `radiation`, `max_temp`, `min_temp`, `daily_rain`, `evap_pan`, `vp`, `et_short_crop`, `code`

## API Methods

### `get_silo_data(lat, lon, start_year, end_year, format='fao56')`

Fetch climate data from SILO API.

**Parameters:**
- `lat` (float): Latitude (-44 to -10 for Australia)
- `lon` (float): Longitude (112 to 154 for Australia)
- `start_year` (int): Start year (1889-present)
- `end_year` (int): End year (1889-present)
- `format` (str): 'fao56' or 'daily' (default: 'fao56')

**Returns:** pandas DataFrame with 7 standardized variables + quality codes

### `export_to_met(df, lat, lon, start_year, end_year, output_dir='.')`

Export DataFrame to APSIM .met format.

**Returns:** Full path to created .met file

### `export_to_csv(df, lat, lon, start_year, end_year, output_dir='.')`

Export DataFrame to CSV format.

**Returns:** Full path to created CSV file

### `calculate_tav(df)`

Calculate annual average ambient temperature.

**Returns:** TAV value (float)

### `calculate_amp(df)`

Calculate annual amplitude in mean monthly temperature.

**Returns:** AMP value (float)

## Quality Codes

SILO provides 6-digit quality codes indicating the data source for each variable:

- `0`: Actual observation
- `1`: Actual observation from composite station
- `2`: Interpolated from daily observations
- `3`: Interpolated using anomaly method
- `6`: Synthetic pan
- `7`: Interpolated long-term averages

Quality codes are preserved in both output formats.

## Error Handling

The client includes comprehensive error handling:

- `InvalidCoordinatesError`: Coordinates outside Australian bounds
- `AuthenticationError`: Invalid username/password
- `MissingVariableError`: Required variables missing from API response
- `SILOAPIError`: General API errors (network, timeout, etc.)

## Coordinate Validation

Coordinates are validated against Australian bounds:
- Latitude: -44.0 to -10.0
- Longitude: 112.0 to 154.0

## Requirements

- Python 3.8+
- pandas >= 1.3.0
- requests >= 2.25.0

## License

This project is provided as-is for use with the SILO API.

## References

- [SILO API Documentation](https://www.longpaddock.qld.gov.au/silo)
- [SILO DataDrill Documentation](http://www.longpaddock.qld.gov.au/silo/datadrill/)
