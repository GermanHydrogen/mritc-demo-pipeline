from datetime import datetime
from pathlib import Path
from shutil import copy2
from typing import Any, Dict, List, Tuple
from uuid import uuid4

import pandas as pd
from PIL import Image
from PIL.ExifTags import TAGS
from ifdo.models import ImageData

from marimba.core.pipeline import BasePipeline
from marimba.lib import image


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
        jpg_list = []

        for file in data_dir.glob("**/*"):
            if (
                file.is_file()
                and file.suffix.lower() in [".jpg"]
                and "_THUMB" not in file.name
                and "overview" not in file.name
            ):
                stills_path = data_dir / "stills"
                stills_path.mkdir(exist_ok=True)

                # Rename images
                output_file_name = self.get_image_output_file_name(
                    config, str(file), int(str(file).split("_")[-1].split(".")[0])
                )
                output_file_path = stills_path / output_file_name
                file.rename(output_file_path)
                self.logger.info(f"Renamed file {file.name} -> {output_file_path}")

                jpg_list.append(output_file_path)

            if file.is_file() and file.suffix.lower() in [".mp4"]:
                video_path = data_dir / "video"
                video_path.mkdir(exist_ok=True)

                # Move videos
                output_file_path = video_path / file.name
                file.rename(output_file_path)
                self.logger.info(f"Renamed file {file.name} -> {output_file_path}")

            if file.is_file() and file.suffix.lower() in [".csv"]:
                data_path = data_dir / "data"
                data_path.mkdir(exist_ok=True)

                # Move data
                output_file_path = data_path / file.name
                file.rename(output_file_path)
                self.logger.info(f"Renamed file {file.name} -> {output_file_path}")

        thumb_list = []
        thumbs_path = data_dir / "thumb"
        thumbs_path.mkdir(exist_ok=True)

        for jpg in jpg_list:
            output_filename = jpg.stem + "_THUMB" + jpg.suffix
            output_path = thumbs_path / output_filename
            self.logger.info(f"Generating thumbnail image: {output_path}")
            image.resize_fit(jpg, 300, 300, output_path)
            thumb_list.append(output_path)

        thumbnail_overview_path = data_dir / "overview.jpg"
        self.logger.info(
            f"Creating thumbnail overview image: {str(thumbnail_overview_path)}"
        )
        image.create_grid_image(thumb_list, data_dir / "overview.jpg")

    def _compose(
        self, data_dirs: List[Path], configs: List[Dict[str, Any]], **kwargs: dict
    ) -> Dict[Path, Tuple[Path, List[ImageData]]]:
        data_mapping = {}
        for data_dir, config in zip(data_dirs, configs):
            file_paths = []
            file_paths.extend(data_dir.glob("**/*"))
            base_output_path = Path(config.get("deployment_id"))

            sensor_data_df = pd.read_csv(next((data_dir / "data").glob("*.CSV")))
            sensor_data_df["FinalTime"] = pd.to_datetime(
                sensor_data_df["FinalTime"], format="%Y-%m-%d %H:%M:%S.%f"
            ).dt.floor("S")

            for file_path in file_paths:
                output_file_path = base_output_path / file_path.relative_to(data_dir)

                if (
                    file_path.is_file()
                    and file_path.suffix.lower() in [".jpg"]
                    and "_THUMB" not in file_path.name
                    and "overview" not in file_path.name
                ):
                    iso_timestamp = file_path.name.split("_")[5]
                    target_datetime = pd.to_datetime(
                        iso_timestamp, format="%Y%m%dT%H%M%SZ"
                    )
                    matching_rows = sensor_data_df[
                        sensor_data_df["FinalTime"] == target_datetime
                    ]

                    if not matching_rows.empty:
                        # in iFDO, the image data list for an image is a list containing single ImageData
                        image_data_list = [
                            ImageData(
                                image_datetime=datetime.strptime(
                                    iso_timestamp, "%Y%m%dT%H%M%SZ"
                                ),
                                image_latitude=matching_rows["UsblLatitude"].values[0],
                                image_longitude=float(
                                    matching_rows["UsblLongitude"].values[0]
                                ),
                                image_depth=float(matching_rows["Altitude"].values[0]),
                                image_altitude=float(
                                    matching_rows["Altitude"].values[0]
                                ),
                                image_event=str(matching_rows["Operation"].values[0]),
                                image_platform=self.config.get("platform_id"),
                                image_sensor=str(matching_rows["Camera"].values[0]),
                                image_camera_pitch_degrees=float(
                                    matching_rows["Pitch"].values[0]
                                ),
                                image_camera_roll_degrees=float(
                                    matching_rows["Roll"].values[0]
                                ),
                                image_uuid=str(uuid4()),
                                # image_pi=self.config.get("voyage_pi"),
                                image_creators=[],
                                image_license="MIT",
                                image_copyright="",
                                image_abstract=self.config.get("abstract"),
                            )
                        ]

                        data_mapping[file_path] = output_file_path, image_data_list

                elif file_path.is_file():
                    data_mapping[file_path] = output_file_path, None

        return data_mapping
