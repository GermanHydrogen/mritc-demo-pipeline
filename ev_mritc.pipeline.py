from datetime import datetime
from pathlib import Path
from shutil import copy2
from typing import Any, Dict, List, Tuple
from uuid import uuid4
from PIL import Image
from PIL.ExifTags import TAGS

from ifdo.models import ImageData

from marimba.core.pipeline import BasePipeline


class MRITCPipeline(BasePipeline):
    """
    Test pipeline. No-op.
    """

    @staticmethod
    def get_pipeline_config_schema() -> dict:
        return {
            "voyage_id": "IN2018_V06",
            "voyage_pi": "Alan Williams",
            "start_date": "2018-11-23",
            "end_date": "2018-12-19",
            "platform_id": "MRITC",
        }

    @staticmethod
    def get_collection_config_schema() -> dict:
        return {
            "deployment_id": "IN2018_V06_001",
        }

    def _import(
        self,
        data_dir: Path,
        source_paths: List[Path],
        config: Dict[str, Any],
        **kwargs: dict,
    ):
        self.logger.info(f"Importing data from {source_paths=} to {data_dir}")
        for source_path in source_paths:
            if not source_path.is_dir():
                continue

            for source_file in source_path.glob("**/*"):
                if source_file.is_file() and source_file.suffix.lower() in [
                    ".csv",
                    ".jpg",
                    ".mp4",
                ]:
                    if not self.dry_run:
                        copy2(source_file, data_dir)
                    self.logger.debug(
                        f"Copied {source_file.resolve().absolute()} -> {data_dir}"
                    )

    def get_image_output_file_name(
        self, deployment_config: dict, file_path: Path, index: int
    ) -> str:
        try:
            image = Image.open(file_path)

            # Check if image has EXIF data
            if hasattr(image, "_getexif"):
                exif_data = image._getexif()
                if exif_data is not None:
                    # Loop through EXIF tags
                    for tag, value in exif_data.items():
                        tag_name = TAGS.get(tag, tag)
                        if tag_name == "DateTime":
                            date = datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
                            # Convert to ISO 8601 format
                            iso_timestamp = date.strftime("%Y%m%dT%H%M%SZ")

                            # Construct and return new filename
                            return (
                                f'{self.config.get("platform_id")}_'
                                f"SCP_"
                                f'{self.config.get("voyage_id").split("_")[0]}_'
                                f'{self.config.get("voyage_id").split("_")[1]}_'
                                f'{deployment_config.get("deployment_id").split("_")[2]}_'
                                f"{iso_timestamp}_"
                                f"{index:04d}"
                                f".JPG"
                            )
            else:
                self.logger.error(f"No EXIF DateTime tag found in image {file_path}")

        except IOError:
            self.logger.error(
                f"Error: Unable to open {file_path}. Are you sure it's an image?"
            )

    def _process(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        for file in data_dir.glob("**/*"):
            if file.is_file() and file.suffix.lower() in [".jpg"]:
                stills_path = data_dir / "stills"
                stills_path.mkdir(exist_ok=True)

                # Rename images
                output_file_name = self.get_image_output_file_name(
                    config, str(file), int(str(file).split("_")[-1].split(".")[0])
                )
                output_file_path = stills_path / output_file_name
                file.rename(output_file_path)

            if file.is_file() and file.suffix.lower() in [".mp4"]:
                video_path = data_dir / "video"
                video_path.mkdir(exist_ok=True)

                # Move videos
                file_path = video_path / file.name
                file.rename(file_path)

            if file.is_file() and file.suffix.lower() in [".csv"]:
                data_path = data_dir / "data"
                data_path.mkdir(exist_ok=True)

                # Move data
                file_path = data_path / file.name
                file.rename(file_path)

    def _compose(
        self, data_dirs: List[Path], configs: List[Dict[str, Any]], **kwargs: dict
    ) -> Dict[Path, Tuple[Path, List[ImageData]]]:
        # Find all .png, .jpg, .jpeg files in data_dirs and create a mapping from input file path to output file path
        data_mapping = {}
        for data_dir, config in zip(data_dirs, configs):
            file_paths = []
            file_paths.extend(data_dir.glob("**/*"))
            base_output_path = Path(config.get("deployment_id"))

            for file_path in file_paths:
                if file_path.is_file() and file_path.suffix.lower() in [".jpg"]:
                    output_file_path = base_output_path / "stills" / file_path.name

                    file_created_datetime = datetime.fromtimestamp(
                        file_path.stat().st_ctime
                    )
                    image_data_list = [  # in iFDO, the image data list for an image is a list containing single ImageData
                        ImageData(
                            image_datetime=file_created_datetime,
                            # image_latitude=sdf,
                            # image_longitude=sdfd,
                        )
                    ]

                    data_mapping[file_path] = output_file_path, image_data_list

                if file_path.is_file() and file_path.suffix.lower() in [".mp4"]:
                    output_file_path = base_output_path / "video" / file_path.name
                    data_mapping[file_path] = output_file_path, None

                if file_path.is_file() and file_path.suffix.lower() in [".csv"]:
                    output_file_path = base_output_path / "data" / file_path.name
                    data_mapping[file_path] = output_file_path, None

        return data_mapping
