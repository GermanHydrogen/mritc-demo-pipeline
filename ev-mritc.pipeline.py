from datetime import datetime
from shutil import copy2
from time import sleep
from uuid import uuid4
from pathlib import Path
from typing import Any, Dict, List, Tuple

from ifdo.models import ImageData

from marimba.core.pipeline import BasePipeline


class TestPipeline(BasePipeline):
    """
    Test pipeline. No-op.
    """
    
    @staticmethod
    def get_pipeline_config_schema() -> dict:
        return {
            "my_str_key": "my_str_value",
            "my_int_key": 1,
            "my_float_key": 1.0,
            "my_bool_key": True,
        }
    
    @staticmethod
    def get_collection_config_schema() -> dict:
        return {
            "year": 2020,
            "month": 1,
            "day": 1,
        }
    
    def _import(self, data_dir: Path, source_paths: List[Path], config: Dict[str, Any], **kwargs: dict):
        self.logger.info(f"Importing data from {source_paths=} to {data_dir}")
        for source_path in source_paths:
            if not source_path.is_dir():
                continue
            
            for source_file in source_path.glob("**/*"):
                if source_file.is_file() and source_file.suffix.lower() in [".png", ".jpg", ".jpeg"]:
                    if not self.dry_run:
                        copy2(source_file, data_dir)
                    self.logger.debug(f"Copied {source_file.resolve().absolute()} -> {data_dir}")

    def _process(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        n = kwargs.get("n", 3)
        try:
            n = int(n)
        except ValueError:
            n = 3
        
        for idx in range(n):
            self.logger.info(f"Processing {idx+1}/{n}...")
            if not self.dry_run:
                sleep(1)  # Pretend to do something
    
    def _compose(self, data_dirs: List[Path], configs: List[Dict[str, Any]], **kwargs: dict) -> Dict[Path, Tuple[Path, List[ImageData]]]:
        # Find all .png, .jpg, .jpeg files in data_dirs and create a mapping from input file path to output file path
        data_mapping = {}
        for data_dir, config in zip(data_dirs, configs):
            year = str(config.get("year"))
            month = str(config.get("month"))
            day = str(config.get("day"))
            output_dir = Path(f"{year:0>4}") / f"{month:0>2}" / f"{day:0>2}"
            
            image_file_paths = []
            image_file_paths.extend(data_dir.glob("**/*.png"))
            image_file_paths.extend(data_dir.glob("**/*.jpg"))
            image_file_paths.extend(data_dir.glob("**/*.jpeg"))
            
            for image_file_path in image_file_paths:
                output_name = f"{image_file_path.stem}-{uuid4()}{image_file_path.suffix}"
                output_file_path = output_dir / output_name
                
                file_created_datetime = datetime.fromtimestamp(image_file_path.stat().st_ctime)
                image_data_list = [  # in iFDO, the image data list for an image is a list containing single ImageData
                    ImageData(
                        image_datetime=file_created_datetime
                    )
                ]
                
                data_mapping[image_file_path] = output_file_path, image_data_list
        
        return data_mapping
