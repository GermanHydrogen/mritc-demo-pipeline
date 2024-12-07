# Marimba MRITC Demo Pipeline

## Overview

The Marimba MRITC (Marine Resources and Industry Towed Camera) Demo Pipeline is a demonstration implementation of a
[Marimba](https://github.com/csiro-fair/marimba) Pipeline designed to process underwater imagery and sensor data
collected during the RV _Investigator_ voyage IN2018_V06. This pipeline showcases how to create a custom Marimba
Pipeline that handles multi-instrument data processing, including still images, videos, and associated sensor data.


## Features

- Imports and processes underwater imagery data from multiple deployments
- Handles both still images (JPG) and videos (MP4)
- Processes associated sensor data from CSV files
- Implements automated file renaming based on temporal metadata
- Generates image thumbnails and overview grids
- Creates FAIR-compliant Marimba Datasets with embedded iFDO metadata


## Prerequisites

- Python 3.10 or higher
- Git
- jq (optional, for pretty-printing JSON metadata)


## Installation

1. Create and activate a Python virtual environment:
   ```bash
   python3 -m venv .env
   source .env/bin/activate
   ```

2. Update pip to the latest version:
   ```bash
   pip install --upgrade pip
   ```

3. Install Marimba:
   ```bash
   pip install marimba
   ```


## Usage

1. Clone the example data repository:
   ```bash
   git clone https://github.com/csiro-fair/mritc-demo-data
   ```

2. Create a new Marimba project:
   ```bash
   marimba new project IN2018_V06
   cd IN2018_V06
   ```

3. Add this pipeline to your project:
   ```bash
   marimba new pipeline MRITC https://github.com/csiro-fair/mritc-demo-pipeline.git
   ```
   When prompted for pipeline metadata, simply press Enter to accept the default values:
   ```
   voyage_id (IN2018_V06): 
   voyage_pi (Alan Williams): 
   start_date (2018-11-23): 
   end_date (2018-12-19): 
   platform_id (MRITC): 
   ```

4. Import data from each deployment directory. For detailed debugging output, use the `--level DEBUG` flag:
   ```bash
   # Import first deployment with debug logging
   marimba --level DEBUG import IN2018_V06_025 ../mritc-demo-data/025

   # Import remaining deployments
   marimba import IN2018_V06_026 ../mritc-demo-data/026
   marimba import IN2018_V06_045 ../mritc-demo-data/045
   marimba import IN2018_V06_181 ../mritc-demo-data/057
   marimba import IN2018_V06_060 ../mritc-demo-data/060
   marimba import IN2018_V06_064 ../mritc-demo-data/064
   marimba import IN2018_V06_114 ../mritc-demo-data/114
   marimba import IN2018_V06_119 ../mritc-demo-data/119
   marimba import IN2018_V06_128 ../mritc-demo-data/128
   marimba import IN2018_V06_168 ../mritc-demo-data/168
   ```

5. Process the imported data:
   ```bash
   marimba process
   ```

6. Package the processed data into a FAIR-compliant dataset:
   ```bash
   marimba package IN2018_V06 \
       --version 1.0 \
       --contact-name "Keiko Abe" \
       --contact-email "keiko.abe@email.com" \
       --zoom 9
   ```

7. Verify the embedded iFDO metadata in any processed image:
   ```bash
   exiftool -j datasets/IN2018_V06/data/MRITC/IN2018_V06_025/images/MRITC_SCP_IN2018_V06_025_20181126T100011Z_0001.JPG | jq '.[0].UserComment | fromjson'
   ```

*Note: [Keiko Abe](https://en.wikipedia.org/wiki/Keiko_Abe) is a renowned Japanese marimba player and composer, widely 
recognised for her role in establishing the marimba as a respected concert instrument.*


## Output Structure

The Pipeline creates an organized Marimba Dataset with the following structured:

```
datasets/IN2018_V06/
├── data/
│   └── MRITC/
│       ├── IN2018_V06_025/
│       │   ├── data/
│       │   │   └── MRITC_IN2018_V06_025.CSV
│       │   ├── images/
│       │   │   └── MRITC_SCP_IN2018_V06_025_20181126T100011Z_0001.JPG
│       │   │   └── ...
│       │   ├── thumbnails/
│       │   │   └── MRITC_SCP_IN2018_V06_025_20181126T100011Z_0001_THUMB.JPG
│       │   │   └── ...
│       │   ├── video/
│       │   │   └── MRITC_IN2018_V06_025_20181126T100011Z.MP4
│       │   └── overview.jpg
│       └── ... (additional deployment directories)
├── logs/
│   ├── pipelines/
│   │   └── MRITC.log
│   ├── dataset.log
│   └── project.log
├── pipelines/
│   └── MRITC/
│       ├── repo/
│       │   ├── LICENSE.txt
│       │   ├── mritc.pipeline.py
│       │   └── README.md
│       └── pipeline.yml
├── ifdo.yml
├── manifest.txt
├── map.png
└── summary.md
```


## Pipeline Components

The MRITC Demo Pipeline implements three main processing stages:

1. **Import (`_import`)**: Copies raw data files from source directories into the Marimba project structure
2. **Process (`_process`)**: 
   - Renames files according to standardized naming conventions
   - Generates thumbnails for all images
   - Creates an overview image for each deployment
3. **Package (`_package`)**: 
   - Maps files to their final Dataset locations
   - Extracts and formats metadata from sensor data
   - Creates iFDO metadata records for all imagery


## License

The MRITC Demo Pipeline is distributed under the [CSIRO BSD/MIT license](LICENSE.txt).


## Contact

For inquiries related to this repository, please contact:

- **Chris Jackett**  
  *Software Engineer, CSIRO*  
  Email: [chris.jackett@csiro.au](mailto:chris.jackett@csiro.au)


## Acknowledgments

This pipeline processes data collected on the Marine National Facility (MNF) RV _Investigator_ voyage IN2018_V06. We
acknowledge all research participants and organizations involved in the original data collection.
